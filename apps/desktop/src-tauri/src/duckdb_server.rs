//! Loopback DuckDB HTTP bridge — Phase 1 of the persistent-DuckDB design
//! (docs/persistent-duckdb-agent-design.md in the platform repo).
//!
//! WHY a loopback HTTP server (not a Tauri command): the Node SEA sidecar CANNOT call into
//! Rust — `invoke()` is a webview-only API and the stdio `sidecar_rpc` channel is Rust→Node
//! only. So Rust hosts a tiny loopback endpoint the sidecar POSTs SQL to (symmetric with the
//! sidecar's own `localhost:9322` query server). The bound port is handed to the sidecar via
//! the `AGENT_DUCKDB_RPC_PORT` env var; if this engine fails to start, the sidecar never gets
//! the port and transparently falls back to the DuckDB CLI / raw path (zero regression).
//!
//! Phase 1 scope: one shared in-memory DuckDB; each worker holds a `try_clone`d connection
//! (DuckDB MVCC → concurrent reads). Each request runs the inbound SQL AS-IS (the SQL compiled
//! by the Node side already embeds `read_csv_auto(..., all_varchar=true)` + per-aggregation
//! casts, so semantics are unchanged — only the executor moves off `spawn(duckdb.exe)`).
//! Phase 2 adds the content-hash StoreRegistry (materialize-once warm tables in this same DB).
//!
//! Rows are serialized by DuckDB itself via `to_json(list(t))` → byte-identical to the CLI's
//! `-json` output, so the platform's shadow/serve gate needs no changes. (Verified offline.)

use std::sync::Arc;

use duckdb::Connection;
use tiny_http::{Header, Method, Request, Response, Server};

const N_WORKERS: usize = 4;

/// Start the loopback DuckDB engine. Returns the bound port (127.0.0.1:<port>).
/// Errors are returned to the caller (lib.rs), which logs + proceeds without the engine.
pub fn start() -> Result<u16, String> {
    let server = Server::http("127.0.0.1:0").map_err(|e| format!("bind: {e}"))?;
    let port = server
        .server_addr()
        .to_ip()
        .map(|a| a.port())
        .ok_or_else(|| "duckdb-server: non-IP listen addr".to_string())?;
    let server = Arc::new(server);

    // One shared in-memory database; workers each hold a cloned connection. DuckDB's MVCC lets
    // these run read queries concurrently. `SET threads` lets a single query use multiple cores.
    let base = Connection::open_in_memory().map_err(|e| format!("open_in_memory: {e}"))?;
    base.execute_batch("SET threads TO 4;").ok();

    for i in 0..N_WORKERS {
        let server = Arc::clone(&server);
        let conn = base
            .try_clone()
            .map_err(|e| format!("try_clone worker {i}: {e}"))?;
        std::thread::Builder::new()
            .name(format!("duckdb-worker-{i}"))
            .spawn(move || {
                // Multiple workers iterating a shared Server share its request queue — the
                // standard tiny_http multithreading pattern (recv() is internally synchronized).
                for req in server.incoming_requests() {
                    handle(req, &conn);
                }
            })
            .map_err(|e| format!("spawn worker {i}: {e}"))?;
    }

    // Keep the in-memory DB alive for the whole process even if every worker clone were dropped.
    std::mem::forget(base);
    eprintln!("[duckdb-server] in-process DuckDB engine on 127.0.0.1:{port} ({N_WORKERS} workers)");
    Ok(port)
}

fn json_header() -> Header {
    // Static, valid bytes — unwrap cannot fail here.
    Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap()
}

fn respond_json(req: Request, status: u16, body: String) {
    let _ = req.respond(
        Response::from_string(body)
            .with_status_code(status)
            .with_header(json_header()),
    );
}

fn handle(mut req: Request, conn: &Connection) {
    if *req.method() != Method::Post || req.url() != "/duckdb" {
        respond_json(req, 404, r#"{"error":"not found"}"#.to_string());
        return;
    }

    let mut body = String::new();
    if let Err(e) = req.as_reader().read_to_string(&mut body) {
        respond_json(req, 400, format!(r#"{{"error":"read body: {}"}}"#, esc(&e.to_string())));
        return;
    }

    let sql = match serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| v.get("sql").and_then(|s| s.as_str()).map(str::to_string))
    {
        Some(s) if !s.trim().is_empty() => s,
        _ => {
            respond_json(req, 400, r#"{"error":"missing sql"}"#.to_string());
            return;
        }
    };

    match run_sql_as_json_array(conn, &sql) {
        // `rows_json` is a JSON array string DuckDB rendered → embed verbatim under "rows".
        Ok(rows_json) => respond_json(req, 200, format!(r#"{{"rows":{rows_json}}}"#)),
        // Return 200 + {error} (NOT a 5xx) so the Node side parses it and falls back to the CLI
        // cleanly. The whole point is: any engine failure → CLI/raw path, never a wrong number.
        Err(e) => respond_json(req, 200, format!(r#"{{"error":"{}"}}"#, esc(&e))),
    }
}

/// Run `sql`, return the result rows as a JSON array STRING (DuckDB renders it → byte-identical
/// to the CLI `-json` path). Empty result → "[]" (to_json(list()) over empty input is SQL NULL).
fn run_sql_as_json_array(conn: &Connection, sql: &str) -> Result<String, String> {
    let wrapped = format!("SELECT CAST(to_json(list(t)) AS VARCHAR) FROM ({sql}) AS t");
    let out: Option<String> = conn
        .query_row(&wrapped, [], |row| row.get::<_, Option<String>>(0))
        .map_err(|e| e.to_string())?;
    Ok(out.unwrap_or_else(|| "[]".to_string()))
}

/// Minimal escaper for error strings embedded in our hand-built JSON error envelopes.
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', " ")
        .replace('\r', " ")
}
