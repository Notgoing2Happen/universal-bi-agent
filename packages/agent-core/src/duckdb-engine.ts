/**
 * DuckDB engine — Phase 3 of the DuckDB local-passthrough plan.
 *
 * Compiles a neutral QuerySpec (contractVersion 2) to DuckDB SQL over the local
 * file and runs it by SHELLING OUT to the DuckDB CLI binary (no native Node addon,
 * so the pure-JS SEA agent is unaffected — the binary is an external, lazily-
 * acquired asset). Returns small final result rows.
 *
 * CORRECTNESS: the SQL is engineered to MATCH the JS reference path
 * (`applyAggregations` in query-server.ts, mirrored by the platform's
 * `applyAggregation`) so DuckDB output is byte-comparable under the existing
 * shadow gate before any live cutover. Key reconciliations (verified against
 * DuckDB v1.5 + the JS oracle):
 *   - SUM  → SUM(COALESCE(TRY_CAST(col AS DOUBLE), 0))   matches `parseFloat(x)||0`
 *            (blank/non-numeric → 0). Residual edge: JS parseFloat is lenient on a
 *            leading number ('12abc'→12) where TRY_CAST→NULL→0; shadow-caught.
 *   - MIN/MAX → MIN/MAX(TRY_CAST(col AS DOUBLE))         ignores blank/non-numeric,
 *            all-blank group → NULL (matches JS `null`).
 *   - COUNT(*) → COUNT(*). (COUNT(namedCol): DuckDB excludes blanks-read-as-NULL
 *            where JS counts '' as present — rare; shadow-caught.)
 *   - GROUP KEY → COALESCE(col, '') for CSV, because DuckDB reads an empty cell as
 *            NULL while the JS CSV parse yields '' (and JS groups '' under '', not
 *            'Unknown'). Genuine nulls (short rows / JSON null → JS 'Unknown') are
 *            the rare residual; shadow-caught.
 * Whatever still diverges simply keeps the cube on the raw path — never wrong.
 */
import * as fs from 'fs';
import * as path from 'path';
import * as http from 'http';
import { spawn } from 'child_process';
import { getConfigDir } from './config';
import type { QuerySpec, SpecAggregation, SpecFilter, SpecDateTrunc } from './query-spec';

type Rows = Record<string, unknown>[];

// ── Engine discovery ─────────────────────────────────────────────────────────

/** The directory the agent looks in for a DuckDB binary that is EITHER bundled by
 * the installer OR lazily downloaded. `AGENT_ENGINES_DIR` lets the Tauri sidecar
 * point at a bundle-resource dir (option A: ship the binary with the installer);
 * otherwise it's <configDir>/engines (option B: lazy-download target). */
export function enginesDir(): string {
  return process.env.AGENT_ENGINES_DIR || path.join(getConfigDir(), 'engines');
}

/** Locate a DuckDB CLI binary: explicit env override → PATH → engines dir
 * (bundled-resource or lazy-download). Returns null when none is found (caller
 * falls back to the JS path). */
export function findDuckdbBinary(): string | null {
  const exe = process.platform === 'win32' ? 'duckdb.exe' : 'duckdb';

  const envPath = process.env.AGENT_DUCKDB_PATH;
  if (envPath && safeExists(envPath)) return envPath;

  for (const dir of (process.env.PATH || '').split(path.delimiter)) {
    if (!dir) continue;
    const p = path.join(dir, exe);
    if (safeExists(p)) return p;
  }

  // Bundled-resource OR lazy-download target.
  const p = path.join(enginesDir(), exe);
  if (safeExists(p)) return p;

  return null;
}

export function isDuckdbAvailable(): boolean {
  // The in-process Rust engine (AGENT_DUCKDB_RPC_PORT, set by the Tauri shell) provides DuckDB
  // even when no CLI binary is present, so advertise the capability when either is available.
  return !!process.env.AGENT_DUCKDB_RPC_PORT || findDuckdbBinary() !== null;
}

function safeExists(p: string): boolean {
  try {
    return fs.existsSync(p);
  } catch {
    return false;
  }
}

// ── SQL compilation ──────────────────────────────────────────────────────────

function quoteIdent(name: string): string {
  return `"${String(name).replace(/"/g, '""')}"`;
}
function quoteLit(value: unknown): string {
  return `'${String(value).replace(/'/g, "''")}'`;
}
function numLit(value: unknown): string {
  const n = Number(value);
  return Number.isFinite(n) ? String(n) : 'NULL';
}

/** The read-source clause for the file. CSV/TSV read as all_varchar so group keys
 * are string-identical to the JS parse; measures are TRY_CAST per aggregation. */
export function buildFromClause(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  const lit = quoteLit(filePath);
  if (ext === '.csv') return `read_csv_auto(${lit}, all_varchar=true, header=true)`;
  if (ext === '.tsv') return `read_csv_auto(${lit}, all_varchar=true, header=true, delim='\t')`;
  if (ext === '.json') return `read_json_auto(${lit})`;
  if (ext === '.parquet') return `read_parquet(${lit})`;
  throw new Error(`duckdb-engine: unsupported file type "${ext}" (xlsx must be pre-parsed into a temp table)`);
}

function aggExpr(a: SpecAggregation): string {
  const col = quoteIdent(a.column);
  switch (a.type) {
    case 'count':
      if (a.column === '*' || a.column === '1') return 'COUNT(*)';
      // DISTINCT: the platform counts distinct NON-NULL raw values, where a blank
      // CSV cell is '' (a counted value). all_varchar reads blank as NULL, so
      // COALESCE(col,'') restores '' as one distinct value — matching the platform.
      // Non-distinct COUNT(col) counts non-NULL (blank excluded) — a documented
      // edge vs the platform's blank-as-present; shadow-caught, rare for named-col counts.
      return a.distinct ? `COUNT(DISTINCT COALESCE(${col}, ''))` : `COUNT(${col})`;
    case 'sum':
      return `SUM(COALESCE(TRY_CAST(${col} AS DOUBLE), 0))`;
    case 'avg': {
      // Match the platform EXACTLY (computeAggregate 'avg'): over rows where the
      // value is non-null AND non-blank, sum `parseFloat(v)||0` (so NON-NUMERIC
      // text contributes 0 to the numerator) and divide by the COUNT of those
      // rows (non-numeric text IS in the denominator). DuckDB's bare
      // `AVG(TRY_CAST … DOUBLE)` is WRONG here — it drops non-numeric from BOTH
      // sum and count, inflating the mean (e.g. [10,'abc',20] → 15 not 10).
      // Empty group → COUNT 0 → NULLIF → NULL (matches the platform's `null`).
      const keep = `${col} IS NOT NULL AND ${col} <> ''`;
      return `SUM(CASE WHEN ${keep} THEN COALESCE(TRY_CAST(${col} AS DOUBLE), 0) END) ` +
        `/ NULLIF(COUNT(CASE WHEN ${keep} THEN 1 END), 0)`;
    }
    case 'min':
      return `MIN(TRY_CAST(${col} AS DOUBLE))`;
    case 'max':
      return `MAX(TRY_CAST(${col} AS DOUBLE))`;
    default:
      throw new Error(`duckdb-engine: unsupported aggregation "${(a as SpecAggregation).type}"`);
  }
}

function groupExpr(col: string, dateTruncs?: SpecDateTrunc[]): string {
  const dt = dateTruncs?.find((d) => d.column === col);
  if (dt) {
    // Match the platform's truncDate format BYTE-FOR-BYTE so the group key aligns:
    // day/week/month/quarter/year all render as 'YYYY-MM-DD' (UTC; ISO Monday-start
    // week; month→YYYY-MM-01, quarter→first month, year→YYYY-01-01 — date_trunc
    // already lands on those days, so '%Y-%m-%d' reproduces them). We use strftime
    // over date_trunc — NOT DuckDB's CAST AS VARCHAR, which yields '2024-01-01
    // 00:00:00'. An unparseable date → TRY_CAST NULL → date_trunc NULL → strftime
    // NULL, exactly like the platform (truncDate→null → keyed as 'Unknown' on both
    // sides during compare). Sub-day grains (hour/minute/second) are excluded by
    // the platform gate, so '%Y-%m-%d' covers every granularity reaching here.
    return `strftime(date_trunc(${quoteLit(dt.granularity)}, TRY_CAST(${quoteIdent(col)} AS TIMESTAMP)), '%Y-%m-%d')`;
  }
  // Empty CSV cell reads as NULL in DuckDB but '' in the JS parse → COALESCE to ''
  // so empty-key rows group under '' (matching JS), not under 'Unknown'.
  return `COALESCE(${quoteIdent(col)}, '')`;
}

/** ISO 8601 date prefix — mirrors the platform's orderedCompare date detector. */
const ISO_DATE_PREFIX = /^\d{4}-\d{2}-\d{2}/;

function filterExpr(f: SpecFilter): string {
  const col = quoteIdent(f.column);
  // Normalize to a value list (multi-value IN / any-of). Single-value filters carry
  // `value`; multi-value carry `values`. Ordered ops use vals[0] only.
  const vals: Array<string | number> =
    f.values && f.values.length ? f.values : f.value !== undefined ? [f.value] : [];
  const v0 = vals[0];
  const lowerLit = (v: unknown) => quoteLit(String(v).toLowerCase());
  const orList = (clauses: string[]) => (clauses.length === 1 ? clauses[0] : `(${clauses.join(' OR ')})`);
  // Ordered: mirror the platform's orderedCompare — ISO-date value → TIMESTAMP cmp, else numeric.
  const ordered = (sym: string) =>
    ISO_DATE_PREFIX.test(String(v0))
      ? `TRY_CAST(${col} AS TIMESTAMP) ${sym} TRY_CAST(${quoteLit(v0)} AS TIMESTAMP)`
      : `TRY_CAST(${col} AS DOUBLE) ${sym} ${numLit(v0)}`;
  switch (f.operator) {
    case 'equals':
      return `${col} IN (${vals.map(quoteLit).join(', ')})`;
    case 'notEquals':
      // any-of NONE-match; null col → included (matches the platform's String(null)!==v).
      return `(${col} IS NULL OR ${col} NOT IN (${vals.map(quoteLit).join(', ')}))`;
    case 'contains':
      return orList(vals.map((v) => `contains(lower(${col}), ${lowerLit(v)})`));
    case 'notContains':
      // Platform: null col → included; else NOT (any substring).
      return `(${col} IS NULL OR NOT ${orList(vals.map((v) => `contains(lower(${col}), ${lowerLit(v)})`))})`;
    case 'startsWith':
      return orList(vals.map((v) => `starts_with(lower(${col}), ${lowerLit(v)})`));
    case 'endsWith':
      return orList(vals.map((v) => `ends_with(lower(${col}), ${lowerLit(v)})`));
    case 'gt':
      return ordered('>');
    case 'lt':
      return ordered('<');
    case 'gte':
      return ordered('>=');
    case 'lte':
      return ordered('<=');
    case 'set':
      // Platform: non-null AND non-blank. all_varchar reads blank as NULL → IS NOT NULL covers both.
      return `${col} IS NOT NULL`;
    case 'notSet':
      return `${col} IS NULL`;
    default:
      throw new Error(`duckdb-engine: unsupported filter operator "${(f as SpecFilter).operator}"`);
  }
}

/** Compile a QuerySpec to a single DuckDB SQL statement over `filePath`. */
export function compileSpecToSql(spec: QuerySpec, filePath: string): string {
  const from = buildFromClause(filePath);
  const groupBy = spec.groupBy || [];
  const aggs = spec.aggregations || [];

  const select = [
    ...groupBy.map((g) => `${groupExpr(g, spec.dateTruncs)} AS ${quoteIdent(g)}`),
    ...aggs.map((a) => `${aggExpr(a)} AS ${quoteIdent(a.alias)}`),
  ];
  if (select.length === 0) throw new Error('duckdb-engine: empty spec (no groupBy, no aggregations)');

  let sql = `SELECT ${select.join(', ')} FROM ${from}`;
  if (spec.filters && spec.filters.length) {
    sql += ` WHERE ${spec.filters.map(filterExpr).join(' AND ')}`;
  }
  if (groupBy.length) sql += ` GROUP BY ALL`;
  if (spec.orderBy && spec.orderBy.length) {
    sql += ` ORDER BY ${spec.orderBy
      .map((o) => `${quoteIdent(o.column)} ${o.dir === 'desc' ? 'DESC' : 'ASC'}`)
      .join(', ')}`;
  }
  if (spec.limit && Number.isFinite(spec.limit)) sql += ` LIMIT ${Math.floor(spec.limit)}`;
  return sql;
}

/** Honesty/diagnostic counters computed in DuckDB (no parallel JS parse). */
export interface SpecCounters {
  totalSourceRows: number;
  filteredRows: number;
  nullMeasureRows: number;
}

/**
 * Compile the honesty counters for a QuerySpec into ONE DuckDB SELECT over the file:
 *  __t  = totalSourceRows — COUNT(*) over the UNFILTERED file
 *  __f  = filteredRows    — COUNT(*) under the spec's filters
 *  __nm = nullMeasureRows — MAX over measures of the count of FILTERED rows whose measure is NULL/''
 * Pinned to byte-match the JS oracle `countNullMeasureRows` (v == null || v === '', MAX over
 * measures, over the FILTERED set) so the platform's high_null_rate / silent-drop disclosure
 * thresholds fire IDENTICALLY to the shadow-proven JS path. Runs ONLY on the warm in-process
 * engine path (the StoreRegistry table is already materialized → ~free, no re-scan) — never as a
 * 2nd CLI spawn (that 2nd spawn on the Windows-SEA fan-out path is the documented hang risk).
 */
export function compileCountersSql(spec: QuerySpec, filePath: string): string {
  const from = buildFromClause(filePath);
  const filters = spec.filters && spec.filters.length ? spec.filters.map(filterExpr).join(' AND ') : '';
  const filterPred = filters ? `(${filters})` : 'TRUE';
  // Measure columns the JS oracle inspects: skip COUNT(*)/COUNT(1) (no source column).
  const measureCols = Array.from(new Set(
    (spec.aggregations || [])
      .filter((a) => !(a.type === 'count' && (a.column === '*' || a.column === '1')))
      .map((a) => a.column),
  ));
  const nullExprs = measureCols.map((c) => {
    const col = quoteIdent(c);
    // FILTERED rows whose measure is NULL or blank. all_varchar reads a blank cell as NULL, so
    // `IS NULL OR = ''` reproduces the JS `v == null || v === ''` for both blank and missing.
    return `COUNT(*) FILTER (WHERE ${filterPred} AND (${col} IS NULL OR ${col} = ''))`;
  });
  const nmExpr = nullExprs.length === 0 ? '0'
    : nullExprs.length === 1 ? nullExprs[0]
      : `GREATEST(${nullExprs.join(', ')})`;
  return `SELECT COUNT(*) AS __t, COUNT(*) FILTER (WHERE ${filterPred}) AS __f, ${nmExpr} AS __nm FROM ${from}`;
}

// ── Execution ────────────────────────────────────────────────────────────────

/**
 * Bound pushdown DuckDB calls well under the platform's 25s per-cube deadline so a
 * hung spawn fails fast → the caller falls back to RAW rows WITHIN budget (the user
 * gets the answer, never a 25s timeout). DuckDB over a local file is normally <1-3s
 * even for tens-of-MB CSVs, so 10s is a safety net, not a tight bound. Defense-in-depth
 * for the v0.1.41 regression where the SEA-embedded Node hung on the DuckDB
 * child_process spawn (the raw fallback served correctly the whole time, but at the
 * 30s default the cube's 25s deadline fired first → a user-visible 25s timeout).
 */
export const PUSHDOWN_DUCKDB_TIMEOUT_MS = 10_000;

/**
 * Run a raw SQL statement via the DuckDB CLI and parse its JSON output.
 *
 * Uses `spawn` (NOT execFile) with stdin explicitly IGNORED + an INDEPENDENT timer —
 * the two changes that matter for the Windows Tauri-SEA-sidecar (GUI subsystem, no console)
 * spawn hang:
 *  - `stdio: ['ignore','pipe','pipe']` — the default piped-but-never-closed stdin from a
 *    no-console parent can block the child / spawn-init indefinitely on Windows
 *    (nodejs#52364). Ignoring stdin removes that handle entirely.
 *  - An INDEPENDENT `setTimeout` (not execFile's `timeout` option) — execFile's timeout
 *    only arms AFTER the child spawns, so it can't bound a hang IN spawn-init (the exact
 *    v0.1.41/0.1.42 failure mode: the 10s timeout never fired). This timer fires regardless;
 *    on fire we SIGKILL any child + reject, so the caller falls back to RAW rows in budget.
 */
export function runDuckdbJson(binary: string, sql: string, timeoutMs = 30000): Promise<Rows> {
  return new Promise((resolve, reject) => {
    const t0 = Date.now();
    let settled = false;
    let child: ReturnType<typeof spawn> | undefined;
    let stdout = '';
    let stderr = '';
    let overflow = false;

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      try { if (child && child.pid) child.kill('SIGKILL'); } catch { /* best effort */ }
      reject(new Error(`duckdb TIMED OUT after ${Date.now() - t0}ms (limit ${timeoutMs}ms) — spawn/exec did not complete (independent timer; likely a SEA child_process spawn-init hang)`));
    }, timeoutMs);

    const finish = (fn: () => void) => { if (settled) return; settled = true; clearTimeout(timer); fn(); };

    try {
      child = spawn(binary, ['-json', '-c', sql], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    } catch (e) {
      finish(() => reject(new Error(`duckdb spawn threw after ${Date.now() - t0}ms: ${e instanceof Error ? e.message : String(e)}`)));
      return;
    }
    child.on('error', (err) => finish(() => reject(new Error(`duckdb spawn error after ${Date.now() - t0}ms: ${err.message}`))));
    child.stdout?.on('data', (d) => {
      stdout += d;
      if (stdout.length > 256 * 1024 * 1024 && !overflow) { overflow = true; try { child!.kill('SIGKILL'); } catch { /* best effort */ } }
    });
    child.stderr?.on('data', (d) => { stderr += d; });
    child.on('close', (code) => finish(() => {
      if (overflow) { reject(new Error(`duckdb output exceeded 256MB after ${Date.now() - t0}ms`)); return; }
      if (code !== 0) { reject(new Error(`duckdb exited ${code} after ${Date.now() - t0}ms: ${(stderr || '').trim().slice(0, 200) || 'no stderr'}`)); return; }
      const out = stdout.trim();
      if (!out) { resolve([]); return; }
      try {
        const parsed = JSON.parse(out);
        resolve(Array.isArray(parsed) ? parsed : [parsed]);
      } catch (e) {
        reject(new Error(`duckdb JSON parse failed after ${Date.now() - t0}ms: ${e instanceof Error ? e.message : String(e)}`));
      }
    }));
  });
}

/**
 * Append an engine MISS (error / parse-fail / POST-error / timeout) + the SQL to a local log file.
 * The in-process engine's errors were previously swallowed silently (return null → CLI/raw), which
 * made the shadow-bake fly blind. The agent's stderr is lost in the GUI (no console), so a file in
 * the config dir is the diagnosable channel. Best-effort; never throws.
 */
function logEngineMiss(reason: string, detail: string, sql: string): void {
  try {
    const line = `[${new Date().toISOString()}] ${reason}: ${detail.slice(0, 600)} | SQL: ${sql.slice(0, 4000)}\n`;
    fs.appendFileSync(path.join(getConfigDir(), 'duckdb-engine.log'), line);
  } catch {
    /* best effort — diagnostics must never break the query path */
  }
}

/**
 * Phase 1 in-process engine bridge: POST the compiled SQL to the Tauri shell's loopback DuckDB
 * server (AGENT_DUCKDB_RPC_PORT) and return the result rows. Resolves to the rows on success, or
 * `null` on ANY miss — engine not running (port unset), connection error, a non-rows response
 * ({error}), malformed body, or timeout — so the caller falls back to the CLI / raw path. NEVER
 * throws and NEVER returns a partial: a wrong number must never come from a transport hiccup.
 *
 * Output parity: the engine renders rows via DuckDB's own `to_json` → byte-identical to the CLI
 * `-json` path, so the platform shadow gate compares like-for-like.
 */
export function runViaRustEngine(sql: string, filePath: string, timeoutMs: number): Promise<Rows | null> {
  const port = process.env.AGENT_DUCKDB_RPC_PORT;
  if (!port) return Promise.resolve(null);
  return new Promise((resolve) => {
    let settled = false;
    const done = (v: Rows | null) => {
      if (!settled) {
        settled = true;
        resolve(v);
      }
    };
    // filePath lets the engine serve from a warm content-hash-keyed table (Phase 2 StoreRegistry);
    // it rewrites the read_csv call → the warm table. Empty/unknown → engine runs the SQL inline.
    const payload = JSON.stringify({ sql, filePath });
    const req = http.request(
      {
        host: '127.0.0.1',
        port: Number(port),
        path: '/duckdb',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(payload),
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (d) => chunks.push(d as Buffer));
        res.on('end', () => {
          const raw = Buffer.concat(chunks).toString('utf8');
          try {
            const parsed = JSON.parse(raw);
            if (parsed && Array.isArray(parsed.rows)) {
              done(parsed.rows as Rows);
            } else {
              // Engine returned {error} (or an unexpected shape) → LOG the SQL + error so the cause
              // is diagnosable, then fall back (null → CLI/raw). Closes the silent-swallow gap.
              logEngineMiss(
                'engine-error',
                typeof parsed?.error === 'string' ? parsed.error : raw.slice(0, 600),
                sql,
              );
              done(null);
            }
          } catch (e) {
            logEngineMiss('parse-fail', `${e instanceof Error ? e.message : String(e)} | body=${raw.slice(0, 300)}`, sql);
            done(null);
          }
        });
      },
    );
    req.on('error', (err) => {
      logEngineMiss('post-error', `port=${port} ${err.message}`, sql);
      done(null);
    });
    req.setTimeout(timeoutMs, () => {
      try {
        req.destroy();
      } catch {
        /* noop */
      }
      logEngineMiss('timeout', `${timeoutMs}ms port=${port}`, sql);
      done(null);
    });
    req.write(payload);
    req.end();
  });
}

/**
 * High-level: compile + run a QuerySpec against a local file via DuckDB.
 * Phase 1: prefer the in-process Rust engine; fall back to the CLI; return null only when neither
 * is available (caller then falls back to JS/raw).
 */
export async function runSpec(
  spec: QuerySpec,
  filePath: string,
  opts: { binary?: string; timeoutMs?: number } = {},
): Promise<Rows | null> {
  const sql = compileSpecToSql(spec, filePath);
  const timeoutMs = opts.timeoutMs ?? PUSHDOWN_DUCKDB_TIMEOUT_MS;
  const viaRust = await runViaRustEngine(sql, filePath, timeoutMs);
  if (viaRust !== null) return viaRust;
  const binary = opts.binary || findDuckdbBinary();
  if (!binary) return null;
  return runDuckdbJson(binary, sql, timeoutMs);
}

/**
 * Like {@link runSpec}, but ALSO returns the honesty counters
 * (totalSourceRows / filteredRows / nullMeasureRows) WITHOUT a parallel JS parse — the whole
 * point of the engine fast path is to avoid loading the file into Node, so the counters that
 * fed the platform's silent-drop disclosures (formerly from the JS parse) are recomputed in
 * DuckDB instead. Counters are produced ONLY when the IN-PROCESS engine serves the aggregate (a
 * 2nd query over the already-warm StoreRegistry table is ~free). On the CLI-fallback path counters
 * are `null` (caller marks `countersDeferred` — NEVER zeroed) so we never add a 2nd CLI spawn (the
 * documented Windows-SEA fan-out hang). `rows` is null only when neither engine nor CLI is
 * available (caller falls back to the raw Node path). A CLI error propagates (caller catches).
 */
export async function runSpecWithCounters(
  spec: QuerySpec,
  filePath: string,
  opts: { binary?: string; timeoutMs?: number } = {},
): Promise<{ rows: Rows | null; counters: SpecCounters | null }> {
  const sql = compileSpecToSql(spec, filePath);
  const timeoutMs = opts.timeoutMs ?? PUSHDOWN_DUCKDB_TIMEOUT_MS;
  const viaRust = await runViaRustEngine(sql, filePath, timeoutMs);
  if (viaRust !== null) {
    let counters: SpecCounters | null = null;
    try {
      const cRows = await runViaRustEngine(compileCountersSql(spec, filePath), filePath, timeoutMs);
      if (cRows && cRows.length) {
        const r = cRows[0] as Record<string, unknown>;
        counters = {
          totalSourceRows: Number(r.__t) || 0,
          filteredRows: Number(r.__f) || 0,
          nullMeasureRows: Number(r.__nm) || 0,
        };
      }
    } catch {
      /* counters are best-effort; on any miss the caller marks countersDeferred (never zeroed) */
    }
    return { rows: viaRust, counters };
  }
  // CLI fallback: a SINGLE spawn, no counters companion (a 2nd spawn on the fan-out hot path is
  // net-negative + the documented SEA hang). Counters deferred honestly by the caller.
  const binary = opts.binary || findDuckdbBinary();
  if (!binary) return { rows: null, counters: null };
  const rows = await runDuckdbJson(binary, sql, timeoutMs);
  return { rows, counters: null };
}

/**
 * MUST match `AGENT_VIEW` in the platform's apps/cube/drivers/pg-to-duckdb.js — the
 * translated SQL references the source under this name. A reserved-word-safe internal
 * name so a cube logically named `order`/`table` can't collide with the relation.
 */
export const PASSTHROUGH_VIEW = '__agent_src__';

/**
 * Phase 1 SQL-passthrough: run the platform-translated DuckDB SQL over the local file —
 * DuckDB as a local drop-in for the platform's pg-temp executor. The translated SQL
 * references the source under `__agent_src__` (the platform's pg-to-duckdb.js rewrote the
 * cube's table token to it). We INLINE-substitute that placeholder with the file-read
 * clause, producing a SINGLE SELECT statement — this deliberately avoids a multi-statement
 * (`CREATE VIEW …; SELECT …`) batch, whose DuckDB-CLI `-json` output can emit more than one
 * JSON blob and break the parser. Phase 1 SQL has no date functions, so no `SET TimeZone`
 * is needed yet (Phase 2 adds date_trunc + the UTC pin, at which point a verified
 * statement-batching or init-file approach replaces the inline substitution).
 *
 * Returns null when no DuckDB binary (caller falls back to JS/raw); THROWS on a DuckDB
 * error or unsupported file (buildFromClause throws for xlsx) so the caller catches it and
 * falls back. The result is byte-validated against the pg-temp baseline by the platform
 * shadow before it can ever serve — a bad translation diverges and is declined, never wrong.
 */
export async function runPassthroughSql(
  translatedSql: string,
  filePath: string,
  opts: { binary?: string; timeoutMs?: number } = {},
): Promise<Rows | null> {
  const from = buildFromClause(filePath); // throws for xlsx → caller falls back
  // Replace EVERY occurrence of the source placeholder with the file-read clause —
  // BARE (no wrapping parens): a table function takes its alias directly, so
  // `FROM __agent_src__ "t"` → `FROM read_csv_auto('…', all_varchar=true) "t"`.
  // (Wrapping in parens — `FROM (read_csv_auto(...)) "t"` — is a DuckDB syntax error;
  // verified against the real binary before release.)
  const sql = translatedSql.split(PASSTHROUGH_VIEW).join(from);
  const timeoutMs = opts.timeoutMs ?? PUSHDOWN_DUCKDB_TIMEOUT_MS;
  // Phase 1: prefer the in-process Rust engine (no CLI spawn); fall back to the CLI on any miss.
  const viaRust = await runViaRustEngine(sql, filePath, timeoutMs);
  if (viaRust !== null) return viaRust;
  const binary = opts.binary || findDuckdbBinary();
  if (!binary) return null;
  return runDuckdbJson(binary, sql, timeoutMs);
}
