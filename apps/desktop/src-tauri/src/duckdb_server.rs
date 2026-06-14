//! Loopback DuckDB HTTP bridge — persistent-DuckDB Phases 1+2
//! (docs/persistent-duckdb-agent-design.md in the platform repo).
//!
//! WHY a loopback HTTP server (not a Tauri command): the Node SEA sidecar CANNOT call into
//! Rust — `invoke()` is a webview-only API and the stdio `sidecar_rpc` channel is Rust→Node
//! only. So Rust hosts a tiny loopback endpoint the sidecar POSTs SQL to. The bound port is
//! handed to the sidecar via `AGENT_DUCKDB_RPC_PORT`; if this engine fails to start the sidecar
//! falls back to the DuckDB CLI / raw path (zero regression).
//!
//! Phase 1: one shared in-memory DuckDB; each worker holds a `try_clone`d connection (MVCC →
//! concurrent reads); each request runs the inbound SQL AS-IS (`read_csv` inline).
//!
//! Phase 2 (this file): a **StoreRegistry** materializes each file ONCE into a content-hash-keyed
//! warm table (`src_<hash>`) in the shared DB; requests that carry a `filePath` get the read-call
//! over that file rewritten to the warm table, so fan-out + repeat queries skip re-scanning the
//! CSV. Correctness guards:
//!   - **content-hash keyed, NEVER mtime:size** (design risk #1). A stat fast-path (size+mtime
//!     unchanged → reuse the cached hash, no re-read); any size/mtime change → re-hash, and a
//!     changed file lands on a NEW table → never serves stale content.
//!   - **same `all_varchar=true` read options** as the inline path → the warm table is column-
//!     and type-identical, so the query's TRY_CAST aggregations produce identical results.
//!   - any rewrite/materialize miss → run the SQL AS-IS (Phase 1 behavior) → CLI → raw. Never wrong.
//!   - `memory_limit` + `temp_directory` → DuckDB spills instead of OOM-killing the GUI process.
//!
//! Rows are serialized by DuckDB via `to_json(list(t))` → byte-identical to the CLI `-json`
//! output (order preserved), so the platform shadow/serve gate is unchanged.

use std::collections::HashMap;
use std::hash::Hasher;
use std::io::Read as _;
use std::sync::{Arc, Mutex};

use duckdb::Connection;
use tiny_http::{Header, Method, Request, Response, Server};

const N_WORKERS: usize = 4;
/// Materialized-table byte budget. Beyond this, LRU tables are dropped. Generous so eviction is
/// rare for typical cube counts; DuckDB's `memory_limit` independently spills working memory.
const TABLE_BYTE_BUDGET: u64 = 1_500_000_000; // ~1.5 GB
/// Phase 3: files larger than this are NOT materialized into RAM — the query direct-scans them
/// from disk (DuckDB streams), so a GB file can't OOM the warm store. Override via env.
const DEFAULT_INMEM_MAX_BYTES: u64 = 256 * 1024 * 1024; // 256 MB
/// Phase 3: refuse to ship a result whose JSON exceeds this over the loopback (decline → CLI/raw)
/// rather than streaming GBs. Aggregation group sets are tiny; this bounds pathological passthrough.
const DEFAULT_RESPONSE_MAX_BYTES: usize = 256 * 1024 * 1024; // 256 MB

// ── StoreRegistry ──────────────────────────────────────────────────────────────
struct StoreMeta {
    table: String,
    bytes: u64,
    last_used: u64,
}
struct Registry {
    /// path → (content_hash, size, mtime_nanos) for the stat fast-path.
    path_cache: HashMap<String, (u64, u64, u128)>,
    /// content_hash → warm table.
    stores: HashMap<u64, StoreMeta>,
    total_bytes: u64,
    clock: u64,
}
impl Registry {
    fn new() -> Self {
        Registry { path_cache: HashMap::new(), stores: HashMap::new(), total_bytes: 0, clock: 0 }
    }
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }
}

struct Shared {
    registry: Mutex<Registry>,
    /// The materialize connection IS the single-flight lock: only one thread CREATE/DROPs at a
    /// time, and it shares the in-mem DB with all worker connections.
    mat: Mutex<Connection>,
    /// Above this file size, direct-scan from disk instead of materializing into RAM (Phase 3 §2c).
    inmem_max_bytes: u64,
    /// Refuse to ship a result whose JSON exceeds this over the loopback (Phase 3 IPC contract).
    response_max_bytes: usize,
}

/// Start the loopback DuckDB engine. Returns the bound 127.0.0.1 port.
pub fn start() -> Result<u16, String> {
    let server = Server::http("127.0.0.1:0").map_err(|e| format!("bind: {e}"))?;
    let port = server
        .server_addr()
        .to_ip()
        .map(|a| a.port())
        .ok_or_else(|| "duckdb-server: non-IP listen addr".to_string())?;
    let server = Arc::new(server);

    let base = Connection::open_in_memory().map_err(|e| format!("open_in_memory: {e}"))?;
    let temp = std::env::temp_dir().join("ubi-duckdb-spill");
    let _ = std::fs::create_dir_all(&temp);
    let temp_lit = temp.to_string_lossy().replace('\'', "''");
    // memory_limit + temp_directory → spill instead of OOM (design §2a/G0d). threads → multi-core.
    base.execute_batch(&format!(
        "SET threads TO 4; SET memory_limit='1GB'; SET temp_directory='{temp_lit}'; SET max_temp_directory_size='16GB';"
    ))
    .ok();

    let inmem_max_bytes = std::env::var("AGENT_DUCKDB_INMEM_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_INMEM_MAX_BYTES);
    let response_max_bytes = std::env::var("AGENT_DUCKDB_RESPONSE_MAX_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_RESPONSE_MAX_BYTES);

    let shared = Arc::new(Shared {
        registry: Mutex::new(Registry::new()),
        mat: Mutex::new(base.try_clone().map_err(|e| format!("try_clone mat: {e}"))?),
        inmem_max_bytes,
        response_max_bytes,
    });

    for i in 0..N_WORKERS {
        let server = Arc::clone(&server);
        let shared = Arc::clone(&shared);
        let conn = base.try_clone().map_err(|e| format!("try_clone worker {i}: {e}"))?;
        std::thread::Builder::new()
            .name(format!("duckdb-worker-{i}"))
            .spawn(move || {
                for req in server.incoming_requests() {
                    handle(req, &conn, &shared);
                }
            })
            .map_err(|e| format!("spawn worker {i}: {e}"))?;
    }

    std::mem::forget(base); // keep the in-mem DB alive for the whole process
    eprintln!("[duckdb-server] in-process DuckDB engine on 127.0.0.1:{port} ({N_WORKERS} workers, warm StoreRegistry)");
    Ok(port)
}

// ── HTTP handling ──────────────────────────────────────────────────────────────
fn json_header() -> Header {
    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap()
}
fn respond_json(req: Request, status: u16, body: String) {
    let _ = req.respond(
        Response::from_string(body)
            .with_status_code(status)
            .with_header(json_header()),
    );
}

fn handle(mut req: Request, conn: &Connection, shared: &Shared) {
    if *req.method() != Method::Post || req.url() != "/duckdb" {
        respond_json(req, 404, r#"{"error":"not found"}"#.to_string());
        return;
    }
    let mut body = String::new();
    if let Err(e) = req.as_reader().read_to_string(&mut body) {
        respond_json(req, 400, format!(r#"{{"error":"read body: {}"}}"#, esc(&e.to_string())));
        return;
    }
    let v: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(_) => {
            respond_json(req, 400, r#"{"error":"bad json"}"#.to_string());
            return;
        }
    };
    let sql = match v.get("sql").and_then(|s| s.as_str()) {
        Some(s) if !s.trim().is_empty() => s.to_string(),
        _ => {
            respond_json(req, 400, r#"{"error":"missing sql"}"#.to_string());
            return;
        }
    };
    let file_path = v.get("filePath").and_then(|s| s.as_str()).map(str::to_string);

    // Phase 2: if a filePath is supplied, try to serve from a warm table (rewrite the read call).
    // Any failure to resolve/rewrite → run the SQL AS-IS (Phase 1 inline read) → still correct.
    let effective_sql = match &file_path {
        Some(fp) if !fp.is_empty() => match resolve_warm_table(fp, shared) {
            Some(table) => rewrite_read_to_table(&sql, fp, &table).unwrap_or(sql.clone()),
            None => sql.clone(),
        },
        _ => sql.clone(),
    };

    match run_sql_as_json_array(conn, &effective_sql) {
        // Phase 3 IPC contract: refuse to ship an oversized result over the loopback; decline with
        // 200 + {error} → Node falls back to the CLI (which has its own cap) → raw. Never stream GBs.
        Ok(rows_json) if rows_json.len() > shared.response_max_bytes => respond_json(
            req,
            200,
            format!(
                r#"{{"error":"result too large: {} bytes > cap {}"}}"#,
                rows_json.len(),
                shared.response_max_bytes
            ),
        ),
        Ok(rows_json) => respond_json(req, 200, format!(r#"{{"rows":{rows_json}}}"#)),
        // 200 + {error} (not 5xx) so the Node side parses it and falls back to the CLI cleanly.
        Err(e) => respond_json(req, 200, format!(r#"{{"error":"{}"}}"#, esc(&e))),
    }
}

/// Run `sql`, return the result rows as a JSON array STRING (DuckDB renders it). Empty → "[]".
fn run_sql_as_json_array(conn: &Connection, sql: &str) -> Result<String, String> {
    let wrapped = format!("SELECT CAST(to_json(list(t)) AS VARCHAR) FROM ({sql}) AS t");
    let out: Option<String> = conn
        .query_row(&wrapped, [], |row| row.get::<_, Option<String>>(0))
        .map_err(|e| e.to_string())?;
    Ok(out.unwrap_or_else(|| "[]".to_string()))
}

// ── StoreRegistry resolution ─────────────────────────────────────────────────────
/// Resolve the warm table for `file_path`, materializing it once if needed. Returns None on any
/// problem (missing file, unsupported ext, materialize error) → caller runs the SQL inline instead.
fn resolve_warm_table(file_path: &str, shared: &Shared) -> Option<String> {
    let (size, mtime) = stat(file_path).ok()?;

    // Phase 3 §2c: large files are NOT materialized into RAM. Returning None makes handle() run
    // the inline read_csv, which DuckDB STREAMS from disk (respecting memory_limit → spills) — a
    // GB file aggregates without OOM. (Trade-off: no warm reuse for large files yet; an on-disk
    // .db cache for warm large-file reuse is deferred hardening.)
    if size > shared.inmem_max_bytes {
        return None;
    }

    // 1. Stat fast-path: size+mtime unchanged since we last hashed → reuse the cached hash, no read.
    let cached_hash = {
        let reg = shared.registry.lock().ok()?;
        match reg.path_cache.get(file_path) {
            Some((h, s, m)) if *s == size && *m == mtime => Some(*h),
            _ => None,
        }
    };
    let hash = match cached_hash {
        Some(h) => h,
        None => {
            // Size/mtime changed (or first sight) → content-hash the file. A changed file yields a
            // different hash → a NEW table → never serves stale (design risk #1).
            let h = content_hash(file_path).ok()?;
            let mut reg = shared.registry.lock().ok()?;
            reg.path_cache.insert(file_path.to_string(), (h, size, mtime));
            h
        }
    };

    // 2. Warm hit?
    {
        let mut reg = shared.registry.lock().ok()?;
        let now = reg.tick();
        if let Some(meta) = reg.stores.get_mut(&hash) {
            meta.last_used = now;
            return Some(meta.table.clone());
        }
    }

    // 3. Cold → materialize under the materialize lock (single-flight + serialized).
    let read = read_clause(file_path)?; // None for unsupported ext → caller runs inline
    let mat = shared.mat.lock().ok()?;
    // Double-check: another thread may have materialized while we waited for the lock.
    {
        let mut reg = shared.registry.lock().ok()?;
        let now = reg.tick();
        if let Some(meta) = reg.stores.get_mut(&hash) {
            meta.last_used = now;
            return Some(meta.table.clone());
        }
    }
    let table = format!("src_{hash:016x}");
    if let Err(e) = mat.execute_batch(&format!("CREATE TABLE {table} AS SELECT * FROM {read};")) {
        eprintln!("[duckdb-server] materialize failed for {file_path}: {e}");
        return None; // run inline
    }
    {
        let mut reg = shared.registry.lock().ok()?;
        let now = reg.tick();
        reg.stores.insert(hash, StoreMeta { table: table.clone(), bytes: size, last_used: now });
        reg.total_bytes = reg.total_bytes.saturating_add(size);
        evict_if_needed(&mut reg, &mat, &hash);
    }
    Some(table)
}

/// Byte-budget LRU eviction. Drops least-recently-used tables (never the just-created `keep` hash)
/// until under budget. A concurrent query on an evicted table errors → that query falls back to
/// the CLI (safe); refcount-pinning to avoid even that race is Phase 2 hardening.
fn evict_if_needed(reg: &mut Registry, mat: &Connection, keep: &u64) {
    while reg.total_bytes > TABLE_BYTE_BUDGET {
        let victim = reg
            .stores
            .iter()
            .filter(|(h, _)| *h != keep)
            .min_by_key(|(_, m)| m.last_used)
            .map(|(h, m)| (*h, m.table.clone(), m.bytes));
        let Some((vh, vtable, vbytes)) = victim else { break };
        let _ = mat.execute_batch(&format!("DROP TABLE IF EXISTS {vtable};"));
        reg.stores.remove(&vh);
        reg.total_bytes = reg.total_bytes.saturating_sub(vbytes);
        // Drop matching path_cache entries so the next query re-materializes.
        reg.path_cache.retain(|_, (h, _, _)| *h != vh);
    }
}

// ── file helpers ─────────────────────────────────────────────────────────────────
fn stat(path: &str) -> std::io::Result<(u64, u128)> {
    let m = std::fs::metadata(path)?;
    let mtime = m
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Ok((m.len(), mtime))
}

/// SipHash (std DefaultHasher) over the file bytes — a cache key only (not the platform fileSig),
/// so a non-cryptographic 64-bit hash is fine; collisions are astronomically unlikely.
fn content_hash(path: &str) -> std::io::Result<u64> {
    let mut f = std::fs::File::open(path)?;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.write(&buf[..n]);
    }
    Ok(h.finish())
}

/// Read clause for materialization — MUST mirror the Node side's buildFromClause (same
/// `all_varchar=true` options) so the warm table is column/type-identical to the inline read.
fn read_clause(path: &str) -> Option<String> {
    let lit = format!("'{}'", path.replace('\'', "''"));
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("csv") => Some(format!("read_csv_auto({lit}, all_varchar=true, header=true)")),
        Some("tsv") => Some(format!("read_csv_auto({lit}, all_varchar=true, header=true, delim='\t')")),
        Some("json") => Some(format!("read_json_auto({lit})")),
        Some("parquet") => Some(format!("read_parquet({lit})")),
        _ => None, // xlsx etc. — pre-parsed elsewhere; run inline (which will also reject)
    }
}

/// Replace the `read_xxx('<file_path>', …)` call in `sql` with `table`. Structural parse of OUR
/// OWN generated SQL (platform-internal contract): locate the quoted path literal, expand to the
/// enclosing read-function call (balanced parens, skipping string literals), swap in the table.
/// Returns None if it can't locate the call → caller runs the SQL as-is.
fn rewrite_read_to_table(sql: &str, file_path: &str, table: &str) -> Option<String> {
    let lit = format!("'{}'", file_path.replace('\'', "''"));
    let lit_pos = sql.find(&lit)?;
    let prefix = &sql[..lit_pos];
    let markers = ["read_csv_auto(", "read_json_auto(", "read_parquet(", "read_csv("];
    let fn_start = markers
        .iter()
        .filter_map(|m| prefix.rfind(m))
        .max()?;
    let open = sql[fn_start..].find('(')? + fn_start;

    let bytes = sql.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    let mut close = None;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    close = Some(i);
                    break;
                }
            }
            b'\'' => {
                // skip a single-quoted string literal ('' is an escaped quote)
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
            }
            _ => {}
        }
        i += 1;
    }
    let close = close?;
    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..fn_start]);
    out.push_str(table);
    out.push_str(&sql[close + 1..]);
    Some(out)
}

/// Minimal escaper for error strings embedded in our hand-built JSON error envelopes.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
        .replace('\r', " ")
}
