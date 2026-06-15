/**
 * Agent Local Query Server
 *
 * Serves data from local files when the platform's Cube.js needs to query
 * agent-passthrough connections. Runs on a local HTTP port that the platform
 * can reach via a tunnel or direct connection.
 *
 * Flow:
 * 1. Platform Cube.js receives a query for an agent-passthrough connection
 * 2. Cube.js NangoDriver detects syncMode='agent-passthrough' and calls
 *    the agent's query server instead of Nango Proxy
 * 3. Agent reads the local file, applies filters/aggregations, returns data
 * 4. Cube.js processes the result as normal
 *
 * The query server supports:
 * - GET /health — health check
 * - POST /query — execute a query against a local file
 * - GET /schema/:connectionId — return schema for a file
 */

import * as http from 'http';
import * as fs from 'fs';
import * as path from 'path';
import { loadState } from './state';
import { loadConfig } from './config';
import { getOrLoadParsedRows } from './file-cache';
import { runSpec, runPassthroughSql, isDuckdbAvailable } from './duckdb-engine';
import type { QuerySpec } from './query-spec';
import {
  normalizeColumnName,
  parseCsvLine,
  parseCsvFileBuffered,
  parseJsonFileBuffered,
} from './parsers';
import { getAgentVersion } from './version';

// Phase 0 (2026-06-07): file-size cap to prevent silent OOM crashes when
// the sidecar tries to read a file larger than its heap budget. Reads
// loadConfig().maxFileSize (default 50MB) — the same setting the uploader
// uses. Falls back to 50MB if config isn't loaded yet (e.g. health check).
function getMaxFileSize(): number {
  const cfg = loadConfig();
  return cfg?.maxFileSize ?? 50 * 1024 * 1024;
}

/**
 * Phase 0 (2026-06-07): structured error for file-size violations.
 * Platform catches this and surfaces a clear message to the user instead
 * of timing out after the agent silently OOM-crashes.
 */
class FileTooLargeError extends Error {
  readonly statusCode = 413;
  readonly fileSize: number;
  readonly limit: number;
  constructor(filePath: string, fileSize: number, limit: number) {
    super(
      `File "${path.basename(filePath)}" is ${(fileSize / 1024 / 1024).toFixed(1)}MB, ` +
      `exceeds the agent's ${(limit / 1024 / 1024).toFixed(0)}MB limit. ` +
      `Increase maxFileSize in ~/.universal-bi/config.json or split the file.`,
    );
    this.name = 'FileTooLargeError';
    this.fileSize = fileSize;
    this.limit = limit;
  }
}

/**
 * Phase 0 (2026-06-07): wraps fs.statSync + size enforcement before any
 * read. Returns nothing on success; throws FileTooLargeError on violation.
 * Use at the entry of every code path that's about to fs.readFileSync.
 */
function enforceFileSizeCap(filePath: string): void {
  const stats = fs.statSync(filePath);
  const limit = getMaxFileSize();
  if (stats.size > limit) {
    throw new FileTooLargeError(filePath, stats.size, limit);
  }
}

interface QueryRequest {
  connectionId: string;
  filePath: string;
  columns?: string[];       // Which columns to return (default: all)
  filters?: Array<{
    column: string;
    operator: 'equals' | 'notEquals' | 'contains' | 'notContains' | 'startsWith' | 'endsWith' | 'gt' | 'lt' | 'gte' | 'lte' | 'set' | 'notSet';
    value?: string | number;
    values?: Array<string | number>;  // multi-value (IN / any-of)
  }>;
  limit?: number;
  offset?: number;
  // Phase 1 agent aggregation pushdown: when present, group the FULL filtered
  // set by raw columns and return FINAL group rows. The math mirrors the
  // platform's applyAggregation (sum/count/min/max) byte-for-byte so the shadow
  // comparison holds. See docs/agent-aggregation-pushdown-design.md.
  aggregationSpec?: {
    contractVersion: number;
    groupBy: string[];
    aggregations: Array<{ type: string; column: string; alias: string }>;
    // When set, the platform is serving LIVE pushdown against a PROVEN file
    // fingerprint. If our current file's sig differs, we REFUSE to pre-aggregate
    // (the file changed since it was proven) — closing the stale-proven race.
    expectedFileSig?: string;
  };
  // Phase 2/3: neutral query-spec (contractVersion 2). When present AND DuckDB is
  // available the agent compiles it to DuckDB SQL and runs it over the file —
  // covering shapes the JS path can't (date_trunc trends, COUNT DISTINCT). Any
  // DuckDB-unavailable/error falls back to raw rows so the platform aggregates.
  querySpec?: QuerySpec;
  // Phase 1 SQL-passthrough: the platform-translated DuckDB SQL (references the
  // __agent_src__ view). When present AND DuckDB is available the agent registers the
  // file as that view and runs the SQL verbatim — DuckDB as a local drop-in for the
  // platform's pg-temp executor, covering shapes the querySpec can't express
  // (compound value-maps, canonical _cid dims, …). Any DuckDB-unavailable/error →
  // raw fallback. See docs/duckdb-sql-passthrough-plan.md.
  sqlPassthrough?: {
    sql: string;
    expectedFileSig?: string;  // staleness guard, same as aggregationSpec/querySpec
  };
}

interface QueryResponse {
  data: Record<string, unknown>[];
  totalRows: number;
  columns: Array<{ name: string; type: string }>;
  // Pushdown envelope (absent on legacy/raw responses → platform aggregates).
  aggregationApplied?: boolean;
  agentVersion?: string;
  pushdownContractVersion?: number;
  _diag?: Record<string, number | boolean | string>;
}

const SAMPLE_ROWS = 10;

/**
 * Normalize a column name to snake_case lowercase.
 * This ensures consistency between CSV headers (Sample_Name, Num_Reads),
 * the Universal Schema concepts (sample.name → sample_name), and
 * Cube.js field names (sample_name).
 *
 * Examples:
 *   "Sample_Name" → "sample_name"
 *   "Num_Reads" → "num_reads"
 *   "Percent_Q30" → "percent_q30"
 *   "OD_450nm" → "od_450nm"
 *   "sampleTypeID" → "sample_type_id"
 *   "firstName" → "first_name"
 */
/**
 * Parse a CSV file into rows with normalized column names.
 *
 * Phase 1 (2026-06-07, SCOPE.md): swapped from
 *   readFileSync → split('\n') → manual parseCsvLine per line
 * to a streaming csv-parse pipeline. Behavior gains:
 *   - UTF-8 BOM stripped (was silently embedded in first header)
 *   - Multi-line quoted fields parse correctly
 *   - TSV files use the correct delimiter (was hardcoded ',' regardless)
 *   - Type coercion preserved (empty/number/bool/string heuristic identical
 *     to the legacy parser — see parsers/stream-csv.ts legacyCoerce()).
 *
 * Phase 1 keeps the materialized-array return shape for caller compatibility
 * (applyFilters, slice, find all assume an Array). Phase 2 will switch the
 * downstream pipeline to stream-and-sample.
 */
async function parseCsvFile(filePath: string): Promise<Record<string, unknown>[]> {
  const ext = path.extname(filePath).toLowerCase();
  const rawRows = await parseCsvFileBuffered(filePath, {
    delimiter: ext === '.tsv' ? '\t' : undefined,
  });
  if (rawRows.length === 0) return [];

  // csv-parse with `columns: true` emits rows keyed by raw header strings.
  // The legacy parser normalized headers BEFORE building rows; preserve that
  // by remapping each row's keys through normalizeColumnName. Build the name
  // map once from the first row to avoid per-row recomputation.
  const firstRow = rawRows[0];
  const nameMap = new Map<string, string>();
  for (const key of Object.keys(firstRow)) {
    nameMap.set(key, normalizeColumnName(key));
  }
  const needsNormalization = Array.from(nameMap.entries()).some(([k, v]) => k !== v);
  if (!needsNormalization) return rawRows;

  return rawRows.map((row) => {
    const normalized: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      normalized[nameMap.get(key) || normalizeColumnName(key)] = value;
    }
    return normalized;
  });
}

/**
 * Parse a JSON file into rows.
 *
 * Phase 1 (2026-06-07, SCOPE.md): swapped from a single fs.readFileSync +
 * JSON.parse to a streaming stream-json pipeline. Shape sniffer peeks the
 * first 16KB to decide:
 *   - root [...]               → stream array elements
 *   - root { data: [...], ... } (or items/rows/results/records/value)
 *                              → stream the array under that key
 *   - root { ... no array }    → fall back to buffered JSON.parse + [parsed]
 *                                (preserves legacy parity; the consumer
 *                                needs the whole object regardless)
 *   - root scalar              → []
 *
 * Memory profile for row-bearing JSON files: peak ≈ size of the final rows
 * array (no more 2-3× multiplier from UTF-8 string + parsed graph +
 * extracted array). For a 50MB { data: [...] } file with 1KB rows, peak
 * drops from ~150MB to ~50MB.
 *
 * Return shape preserved: still Record<string,unknown>[]. The
 * normalizeRowColumns() wrapper around this call in loadFileData()
 * continues to apply column-name normalization on the result.
 */
async function parseJsonFile(filePath: string): Promise<Record<string, unknown>[]> {
  return parseJsonFileBuffered(filePath);
}

/**
 * Parse an Excel file into rows.
 *
 * Multi-sheet workbooks: previously this hardcoded SheetNames[0], which
 * silently returned an empty/cover sheet for files where the real data
 * lives on a later tab. The sync-time uploader uses AI #10 to pick the
 * right sheet, but query-time runs after-the-fact and doesn't have that
 * decision locally — so we mirror the same intent with a simple
 * heuristic: pick the sheet with the most populated rows. Ties favor
 * the first sheet (preserving existing behavior for single-table files).
 */
function parseExcelFile(filePath: string): Record<string, unknown>[] {
  const XLSX = require('xlsx');
  const buffer = fs.readFileSync(filePath);
  const workbook = XLSX.read(buffer, { type: 'buffer' });
  if (workbook.SheetNames.length === 0) return [];

  let bestSheetName = workbook.SheetNames[0];
  let bestRowCount = -1;
  let bestRows: Record<string, unknown>[] = [];

  for (const name of workbook.SheetNames) {
    const sheet = workbook.Sheets[name];
    if (!sheet) continue;
    const rows = XLSX.utils.sheet_to_json(sheet, { defval: null }) as Record<string, unknown>[];
    // Count rows that have at least one non-null/non-empty value. This
    // discards cover sheets, instructions, banners, etc.
    const populatedCount = rows.filter(r =>
      Object.values(r).some(v => v !== null && v !== undefined && String(v).trim() !== '')
    ).length;
    if (populatedCount > bestRowCount) {
      bestRowCount = populatedCount;
      bestSheetName = name;
      bestRows = rows;
    }
  }

  if (bestSheetName !== workbook.SheetNames[0]) {
    console.log(
      `[query-server] Excel "${path.basename(filePath)}": picked sheet "${bestSheetName}" ` +
      `(${bestRowCount} populated rows) over default first sheet "${workbook.SheetNames[0]}"`,
    );
  }
  return bestRows;
}

/**
 * Normalize all column names in a row set.
 * Applied after parsing to ensure consistency across CSV, JSON, and Excel.
 */
function normalizeRowColumns(rows: Record<string, unknown>[]): Record<string, unknown>[] {
  if (rows.length === 0) return rows;

  // Build name mapping from first row (avoids re-computing per row)
  const firstRow = rows[0];
  const nameMap = new Map<string, string>();
  for (const key of Object.keys(firstRow)) {
    nameMap.set(key, normalizeColumnName(key));
  }

  // If all names are already normalized, skip the remapping
  const needsNormalization = Array.from(nameMap.entries()).some(([k, v]) => k !== v);
  if (!needsNormalization) return rows;

  return rows.map(row => {
    const normalized: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      normalized[nameMap.get(key) || normalizeColumnName(key)] = value;
    }
    return normalized;
  });
}

/**
 * Load and parse a file based on extension.
 * All column names are normalized to snake_case lowercase.
 *
 * Phase 0 (2026-06-07): size-checked before any read. Throws
 * FileTooLargeError if the file exceeds the configured maxFileSize cap.
 * The HTTP handler catches it and returns 413 with a clear message.
 */
async function loadFileData(filePath: string): Promise<Record<string, unknown>[]> {
  enforceFileSizeCap(filePath);
  const ext = path.extname(filePath).toLowerCase();
  let rows: Record<string, unknown>[];
  if (ext === '.csv' || ext === '.tsv') rows = await parseCsvFile(filePath);
  else if (ext === '.json') rows = normalizeRowColumns(await parseJsonFile(filePath));
  else if (ext === '.xlsx' || ext === '.xls') rows = normalizeRowColumns(parseExcelFile(filePath));
  else throw new Error(`Unsupported file type: ${ext}`);
  return rows;
}

/**
 * Apply filters to rows.
 */
function applyFilters(rows: Record<string, unknown>[], filters: QueryRequest['filters']): Record<string, unknown>[] {
  if (!filters || filters.length === 0) return rows;

  return rows.filter(row => {
    return filters.every(f => {
      const val = row[f.column];
      // Normalize to a value list (multi-value IN / any-of); ordered ops use vals[0].
      const vals = (f.values && f.values.length) ? f.values : (f.value !== undefined ? [f.value] : []);
      const v0 = vals[0];
      // Ordered ops: mirror the DuckDB compiler (duckdb-engine.filterExpr) + the
      // platform's orderedCompare — an ISO-date value compares as a DATE, else
      // numeric. Without this, Number('2024-01-01')=NaN makes every date-range
      // filter false → wrong honesty counters (filteredRows/nullMeasureRows).
      const ordered = (cmp: (a: number, b: number) => boolean): boolean => {
        if (/^\d{4}-\d{2}-\d{2}/.test(String(v0))) {
          const a = Date.parse(String(val)), b = Date.parse(String(v0));
          return Number.isNaN(a) || Number.isNaN(b) ? false : cmp(a, b);
        }
        return cmp(Number(val), Number(v0));
      };
      switch (f.operator) {
        case 'equals': return vals.some(v => String(val) === String(v));
        case 'notEquals': return !vals.some(v => String(val) === String(v));
        case 'contains': return vals.some(v => String(val).toLowerCase().includes(String(v).toLowerCase()));
        case 'notContains': return val == null ? true : !vals.some(v => String(val).toLowerCase().includes(String(v).toLowerCase()));
        case 'startsWith': return vals.some(v => String(val).toLowerCase().startsWith(String(v).toLowerCase()));
        case 'endsWith': return vals.some(v => String(val).toLowerCase().endsWith(String(v).toLowerCase()));
        case 'gt': return ordered((a, b) => a > b);
        case 'lt': return ordered((a, b) => a < b);
        case 'gte': return ordered((a, b) => a >= b);
        case 'lte': return ordered((a, b) => a <= b);
        case 'set': return val !== null && val !== undefined && val !== '';
        case 'notSet': return val === null || val === undefined || val === '';
        default: return true;
      }
    });
  });
}

/**
 * Group-cardinality cap. If an aggregation produces more groups than this
 * (pathological near-unique key), bail to raw rows so the platform aggregates
 * the full set — never silently truncate groups. High enough to never trip on
 * real BI group-bys.
 */
const MAX_GROUP_KEYS = 500000;

/**
 * Agent aggregation pushdown (Phase 1). Groups the FULL filtered set by raw
 * columns and reduces with sum/count/min/max — the associative subset.
 *
 * CORRECTNESS CONTRACT: every formula + the group-key construction + the
 * keyValues shape MUST be byte-identical to the platform's
 * `applyAggregation`/`computeAggregate` in apps/cube/drivers/nango-driver.js,
 * or the shadow comparison diverges and pushdown never cuts over. Phase 1 is
 * the safe subset only (no avg/stddev/distinct/caseWhen/date-trunc — those are
 * gated out platform-side and never reach here).
 *
 * Honesty counters live in the RESPONSE _diag only (not as row columns) so the
 * group rows stay byte-identical to applyAggregation's output and never leak a
 * `_rowCount` column into the Cube.js result.
 *
 * Returns null when the group cap is exceeded → caller returns raw rows with
 * aggregationApplied:false.
 */
/**
 * Count rows whose measure column is null/blank, max over measures — the
 * null-rate disclosure signal the platform reconstructs. Mirrors the inline loop
 * in applyAggregations, exposed for the contractVersion-2 (DuckDB) path so its
 * honesty `_diag` counters match the JS path and the platform's silent-drop
 * disclosures are never lost.
 */
function countNullMeasureRows(
  rows: Record<string, unknown>[],
  aggs: Array<{ type: string; column: string; alias: string }>,
): number {
  let n = 0;
  for (const a of (aggs || [])) {
    if (a.column === '*' || a.column === '1') continue;
    let c = 0;
    for (const r of rows) {
      const v = r[a.column];
      if (v == null || v === '') c++;
    }
    if (c > n) n = c;
  }
  return n;
}

export function applyAggregations(
  rows: Record<string, unknown>[],
  spec: { groupBy: string[]; aggregations: Array<{ type: string; column: string; alias: string }> },
): { rows: Record<string, unknown>[]; nullMeasureRows: number } | null {
  const groupBy = spec.groupBy || [];
  const aggs = spec.aggregations || [];

  // Mirror the platform applyAggregation's empty-input early return (it returns
  // the input array — i.e. [] — regardless of groupBy, BEFORE the no-groupBy
  // total-row branch). Matching it keeps pushdown byte-identical to the raw path
  // and prevents a false shadow divergence on an empty / zero-row file.
  if (rows.length === 0) return { rows: [], nullMeasureRows: 0 };

  const computeAggregate = (
    groupRows: Record<string, unknown>[],
    agg: { type: string; column: string },
  ): number | null => {
    const get = (r: Record<string, unknown>) => r[agg.column];
    switch (agg.type) {
      case 'count':
        if (agg.column === '*' || agg.column === '1') return groupRows.length;
        return groupRows.filter(r => get(r) != null).length;
      case 'sum':
        return groupRows.reduce((s, r) => s + (parseFloat(get(r) as any) || 0), 0);
      case 'min': {
        const vals = groupRows.map(r => parseFloat(get(r) as any)).filter(v => !isNaN(v));
        return vals.length > 0 ? Math.min(...vals) : null;
      }
      case 'max': {
        const vals = groupRows.map(r => parseFloat(get(r) as any)).filter(v => !isNaN(v));
        return vals.length > 0 ? Math.max(...vals) : null;
      }
      default:
        // Unreachable: the platform gate only sends sum/count/min/max. Mirror
        // applyAggregation's default (rows.length) defensively.
        return groupRows.length;
    }
  };

  // null-measure counter for the high-null-rate disclosure the platform
  // reconstructs: max over measures of filtered rows whose measure col is
  // null/blank (i.e. excluded from sum/min/max).
  let nullMeasureRows = 0;
  for (const a of aggs) {
    if (a.column === '*' || a.column === '1') continue;
    let n = 0;
    for (const r of rows) { const v = r[a.column]; if (v == null || v === '') n++; }
    if (n > nullMeasureRows) nullMeasureRows = n;
  }

  // No GROUP BY → a single total row.
  if (groupBy.length === 0) {
    const out: Record<string, unknown> = {};
    for (const a of aggs) out[a.alias] = computeAggregate(rows, a);
    return { rows: [out], nullMeasureRows };
  }

  // GROUP BY — mirror applyAggregation's key + keyValues construction exactly.
  const groups = new Map<string, { rows: Record<string, unknown>[]; keyValues: Record<string, unknown> }>();
  for (const row of rows) {
    const groupKey = groupBy.map(col => String(row[col] ?? 'Unknown')).join('|||');
    let g = groups.get(groupKey);
    if (!g) {
      if (groups.size >= MAX_GROUP_KEYS) return null; // cap exceeded → raw fallback
      const keyValues: Record<string, unknown> = {};
      for (const col of groupBy) keyValues[col] = row[col] != null ? row[col] : 'Unknown';
      g = { rows: [], keyValues };
      groups.set(groupKey, g);
    }
    g.rows.push(row);
  }

  const out: Record<string, unknown>[] = [];
  for (const g of groups.values()) {
    const resultRow: Record<string, unknown> = { ...g.keyValues };
    for (const a of aggs) resultRow[a.alias] = computeAggregate(g.rows, a);
    out.push(resultRow);
  }
  return { rows: out, nullMeasureRows };
}

/**
 * Resolve file path from connectionId using agent state.
 */
function resolveFilePath(connectionId: string): string | null {
  const state = loadState();
  for (const [filePath, info] of Object.entries(state.files || {})) {
    if (info.connectionId === connectionId) return filePath;
  }
  return null;
}

/**
 * Infer column types from data.
 */
function inferColumns(rows: Record<string, unknown>[]): Array<{ name: string; type: string }> {
  if (rows.length === 0) return [];
  const colNames = new Set<string>();
  rows.slice(0, 100).forEach(r => Object.keys(r).forEach(k => colNames.add(k)));

  return Array.from(colNames).map(name => {
    const samples = rows.slice(0, 50).map(r => r[name]).filter(v => v !== null && v !== undefined && v !== '');
    if (samples.length === 0) return { name, type: 'string' };
    if (samples.every(s => typeof s === 'number')) return { name, type: Number.isInteger(samples[0] as number) ? 'integer' : 'float' };
    if (samples.every(s => typeof s === 'boolean')) return { name, type: 'boolean' };
    return { name, type: 'string' };
  });
}

let server: http.Server | null = null;

/**
 * Start the local query server.
 */
export function startQueryServer(port: number = 9322): Promise<void> {
  return new Promise((resolve, reject) => {
    server = http.createServer(async (req, res) => {
      // CORS headers for platform access
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }

      // Auth check — verify the request comes from our platform
      const config = loadConfig();
      const authHeader = req.headers.authorization;
      if (!config || authHeader !== `Bearer ${config.apiKey}`) {
        res.writeHead(401, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Unauthorized' }));
        return;
      }

      try {
        if (req.url === '/health' && req.method === 'GET') {
          // Phase 1 follow-up (2026-06-07): version was hardcoded '0.1.0' here
          // since the agent's first commit — drifted across 33 releases until
          // someone hit /health and realized the platform was reading a stale
          // value. Now reads from process.env.AGENT_VERSION (set by the sidecar
          // before this server starts). Throws if unset — see version.ts.
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            status: 'ok',
            version: getAgentVersion(),
            // Capability handshake: the platform only routes SQL-passthrough to agents
            // that advertise it (also echoed per-query in _diag.supportsSqlPassthrough +
            // pushdownContractVersion:3). DuckDB presence is required to actually run it.
            supportsSqlPassthrough: true,
            pushdownContractVersion: 3,
            duckdbAvailable: isDuckdbAvailable(),
          }));
          return;
        }

        if (req.url === '/query' && req.method === 'POST') {
          const body = await readBody(req);
          const query: QueryRequest = JSON.parse(body);

          // Resolve file path
          let filePath = query.filePath;
          if (!filePath && query.connectionId) {
            filePath = resolveFilePath(query.connectionId) || '';
          }

          if (!filePath || !fs.existsSync(filePath)) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: `File not found: ${filePath}` }));
            return;
          }

          // ── Phase 3: large DuckDB-readable files (gated by AGENT_DUCKDB_LARGE_FILES) ──
          // getOrLoadParsedRows below loads the WHOLE file into Node memory — that is the real
          // 50MB ceiling (a big file OOMs the SEA heap). But the in-process DuckDB engine
          // direct-scans large files from disk (streams, respecting memory_limit → spills). So
          // when enabled, a pushdown request on a large, DuckDB-readable file SKIPS the Node load
          // and aggregates via DuckDB directly. Success → the small group set (platform-compatible).
          // The platform has no alternate path to a LOCAL agent file (it can't pg-temp a file on
          // the user's machine), so a failure here is an HONEST 413 — never a wrong/empty result,
          // never GBs loaded into Node. Off by default until validated end-to-end with a real
          // >cap file; small files / xlsx / no-engine all take the existing path UNCHANGED.
          if (process.env.AGENT_DUCKDB_LARGE_FILES === 'true') {
            const lext = path.extname(filePath).toLowerCase();
            const duckReadable = lext === '.csv' || lext === '.tsv' || lext === '.json' || lext === '.parquet';
            let fileBytes = 0;
            try { fileBytes = fs.statSync(filePath).size; } catch { /* leave 0 */ }
            const isV2 = !!(query.querySpec && query.querySpec.contractVersion === 2 &&
              Array.isArray(query.querySpec.aggregations) && query.querySpec.aggregations.length > 0);
            const isSp = !!(query.sqlPassthrough && typeof query.sqlPassthrough.sql === 'string' && query.sqlPassthrough.sql.trim());
            if ((isV2 || isSp) && duckReadable && fileBytes > getMaxFileSize() && isDuckdbAvailable()) {
              // No Node load → no content hash; fall back to mtime:size for the staleness guard.
              let lfSig = '';
              try { const st = fs.statSync(filePath); lfSig = `${Math.round(st.mtimeMs)}:${st.size}`; } catch { /* */ }
              const ver = isSp ? 3 : 2;
              const sendLF = (r: QueryResponse) => { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(r)); };
              const expSig = query.querySpec?.expectedFileSig || query.sqlPassthrough?.expectedFileSig;
              if (expSig && expSig !== lfSig) {
                // Changed since proven → sig-mismatch (platform re-fetches/re-proves), no rows.
                sendLF({ data: [], totalRows: -1, columns: [], aggregationApplied: false, agentVersion: getAgentVersion(), pushdownContractVersion: ver, _diag: { largeFile: true, fileBytes, sigMismatch: true, fileSig: lfSig, engine: isSp ? 'duckdb-sql' : 'duckdb' } });
                return;
              }
              let lfRows: Record<string, unknown>[] | null = null;
              let lfErr = '';
              try {
                lfRows = isSp ? await runPassthroughSql(query.sqlPassthrough!.sql, filePath) : await runSpec(query.querySpec!, filePath);
              } catch (e) { lfErr = e instanceof Error ? e.message : String(e); }
              if (lfRows) {
                sendLF({ data: lfRows, totalRows: -1, columns: inferColumns(lfRows), aggregationApplied: true, agentVersion: getAgentVersion(), pushdownContractVersion: ver, _diag: { largeFile: true, fileBytes, fileSig: lfSig, engine: isSp ? 'duckdb-sql' : 'duckdb', groupCount: lfRows.length } });
                return;
              }
              // Could not aggregate a large file (engine miss/error). We CANNOT load it into Node
              // (OOM) and the platform has no other path to a local agent file → fail HONESTLY.
              res.writeHead(413, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({ error: `File too large to aggregate (${fileBytes} bytes) and the DuckDB engine could not process it${lfErr ? ': ' + lfErr.slice(0, 160) : ''}.` }));
              return;
            }
          }

          // File fingerprint for the platform's pushdown proven-state machine.
          // mtime:size is the FALLBACK; the authoritative value is the parse
          // cache's CONTENT hash (set below) so the fingerprint + the agent's
          // expectedFileSig staleness guard are content-exact — mtime:size can
          // collide on a same-size edit within one filesystem-timestamp tick,
          // which would let a changed file serve a stale proven aggregate.
          let fileSig = '';
          try { const fst = fs.statSync(filePath); fileSig = `${Math.round(fst.mtimeMs)}:${fst.size}`; } catch { /* leave empty */ }

          // Parse file (cached: one parse serves all pages / fan-out cubes /
          // repeat questions; keyed by content hash so any file change busts it).
          // READ-ONLY: `rows` may be a SHARED cached array — consumers below
          // (applyFilters/applyAggregations/projection/slice) all return new
          // arrays/objects and never mutate it in place. Keep it that way.
          const loaded = await getOrLoadParsedRows(filePath, loadFileData);
          let rows = loaded.rows;
          const cacheHit = loaded.cacheHit;
          const totalRows = rows.length;
          // Prefer the content hash when the cache produced one (it does unless
          // AGENT_FILECACHE_DISABLED). `sha:` prefix marks the scheme.
          if (loaded.sha256) fileSig = `sha:${loaded.sha256}`;

          // Apply filters
          rows = applyFilters(rows, query.filters);
          const filteredRows = rows.length;

          // ── Agent DuckDB SQL-passthrough (Phase 1) ────────────────────────
          // The platform sends the REAL Cube.js measure/dim SQL, translated to DuckDB
          // (pg-to-duckdb.js). We register the file as the __agent_src__ view and run
          // it — DuckDB as a local drop-in for the platform's pg-temp executor, covering
          // shapes the querySpec can't (compound value-maps, canonical _cid dims, …).
          // The result is byte-validated vs the pg-temp baseline by the platform shadow
          // before any serve; ANY DuckDB-unavailable / error → raw fallback (never wrong).
          const sp = query.sqlPassthrough;
          if (sp && typeof sp.sql === 'string' && sp.sql.trim()) {
            const src = loaded.rows;
            const send = (r: QueryResponse) => {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify(r));
            };
            // Staleness guard — file changed since the platform proved this shape.
            if (sp.expectedFileSig && sp.expectedFileSig !== fileSig) {
              send({
                data: [], totalRows: src.length, columns: [], aggregationApplied: false,
                agentVersion: getAgentVersion(), pushdownContractVersion: 3,
                _diag: { totalSourceRows: src.length, sigMismatch: true, fileSig, cacheHit, engine: 'duckdb-sql', supportsSqlPassthrough: true },
              });
              return;
            }
            let outRows: Record<string, unknown>[] | null = null;
            let sqlError = '';
            try {
              outRows = await runPassthroughSql(sp.sql, filePath);
            } catch (e) {
              sqlError = e instanceof Error ? e.message : String(e);
              console.warn('[query] DuckDB sql_passthrough failed — raw fallback:', sqlError.substring(0, 160));
            }
            if (outRows) {
              send({
                data: outRows, totalRows: src.length, columns: inferColumns(outRows),
                aggregationApplied: true, agentVersion: getAgentVersion(), pushdownContractVersion: 3,
                _diag: { totalSourceRows: src.length, fileSig, cacheHit, engine: 'duckdb-sql', groupCount: outRows.length, supportsSqlPassthrough: true },
              });
              return;
            }
            // DuckDB unavailable or errored → RAW (unfiltered) rows so the PLATFORM
            // applies its own filters + aggregates the full set (single source of truth),
            // mirroring the querySpec fallback. aggregationApplied:false → the platform
            // never records a shadow result from this (no false proof / no false divergence).
            send({
              data: src, totalRows: src.length, columns: inferColumns(src),
              aggregationApplied: false, agentVersion: getAgentVersion(), pushdownContractVersion: 3,
              _diag: { totalSourceRows: src.length, fileSig, cacheHit, engine: 'js-fallback', duckdbAvailable: isDuckdbAvailable(), supportsSqlPassthrough: true, ...(sqlError ? { sqlError: sqlError.substring(0, 200) } : {}) },
            });
            return;
          }

          // ── Agent DuckDB pushdown (contractVersion 2) ─────────────────────
          // A neutral query-spec (filters + group-by + aggregations + date_trunc)
          // the agent compiles to DuckDB SQL and runs over the file, returning
          // FINAL group rows. Covers shapes the JS path can't (date_trunc trends,
          // COUNT DISTINCT) and is byte-validated vs the JS reference under the
          // platform shadow gate before any live cutover. Honesty counters still
          // come from the (cached) JS parse so the platform's silent-drop
          // disclosures are never lost. ANY DuckDB-unavailable / error → raw
          // fallback (the platform aggregates the full set) — never silently wrong.
          const v2 = query.querySpec;
          // Require a well-formed spec (aggregations present) — a malformed v2
          // request (e.g. corruption / version skew) falls through to the raw
          // path rather than crashing countNullMeasureRows / the compiler.
          if (v2 && v2.contractVersion === 2 && Array.isArray(v2.aggregations) && v2.aggregations.length > 0) {
            const src = loaded.rows;
            const send = (r: QueryResponse) => {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify(r));
            };
            // Staleness guard — file changed since the platform proved this cube.
            if (v2.expectedFileSig && v2.expectedFileSig !== fileSig) {
              send({
                data: [], totalRows: src.length, columns: [], aggregationApplied: false,
                agentVersion: getAgentVersion(), pushdownContractVersion: 2,
                _diag: { totalSourceRows: src.length, filteredRows: 0, sigMismatch: true, fileSig, cacheHit, engine: 'duckdb' },
              });
              return;
            }
            const jsFiltered = applyFilters(src, v2.filters as QueryRequest['filters']);
            const counters: Record<string, number | boolean | string> = {
              totalSourceRows: src.length,
              filteredRows: jsFiltered.length,
              nullMeasureRows: countNullMeasureRows(jsFiltered, v2.aggregations),
              fileSig,
              cacheHit,
            };
            let duckRows: Record<string, unknown>[] | null = null;
            try {
              duckRows = await runSpec(v2, filePath);
            } catch (e) {
              console.warn('[query] DuckDB v2 spec failed — raw fallback:', e instanceof Error ? e.message : String(e));
            }
            if (duckRows) {
              send({
                data: duckRows, totalRows: src.length, columns: inferColumns(duckRows),
                aggregationApplied: true, agentVersion: getAgentVersion(), pushdownContractVersion: 2,
                _diag: { ...counters, engine: 'duckdb', groupCount: duckRows.length },
              });
              return;
            }
            // Fallback: DuckDB unavailable/errored → return the RAW (unfiltered)
            // rows so the PLATFORM applies its own authoritative filters + then
            // aggregates the full set. Returning jsFiltered would be DOUBLE-filtered
            // (the driver re-applies cubeQuery.filters on the raw branch) and could
            // diverge from the platform's filter semantics — raw src keeps the
            // platform's filter as the single source of truth. Counters below still
            // reflect the agent's filtered count for the honesty signal.
            send({
              data: src, totalRows: src.length, columns: inferColumns(src),
              aggregationApplied: false, agentVersion: getAgentVersion(), pushdownContractVersion: 2,
              _diag: { ...counters, engine: 'js-fallback', duckdbAvailable: isDuckdbAvailable() },
            });
            return;
          }

          // ── Agent aggregation pushdown (Phase 1) ──────────────────────────
          // When the platform sends an aggregationSpec, group the FULL filtered
          // set here and return FINAL group rows (byte-identical to the
          // platform's applyAggregation). Skips column projection + pagination:
          // groups are final and the platform applies LIMIT. On group-cap bail
          // (applyAggregations → null), return ALL filtered rows with
          // aggregationApplied:false so the platform aggregates the complete set.
          let aggregationApplied = false;
          let diag: Record<string, number | boolean | string> | undefined;
          const spec = query.aggregationSpec;
          if (spec && spec.contractVersion === 1 && Array.isArray(spec.aggregations) && spec.aggregations.length > 0) {
            if (spec.expectedFileSig && spec.expectedFileSig !== fileSig) {
              // The file changed since the platform PROVED this cube → REFUSE to
              // pre-aggregate (closes the stale-proven race). Return no rows + a
              // sig-mismatch marker; the platform re-fetches raw and re-proves.
              rows = [];
              diag = { totalSourceRows: totalRows, filteredRows, sigMismatch: true, fileSig };
            } else {
              const agg = applyAggregations(rows, spec);
              if (agg) {
                rows = agg.rows;
                aggregationApplied = true;
                diag = {
                  totalSourceRows: totalRows,
                  filteredRows,
                  groupCount: agg.rows.length,
                  keyCapHit: false,
                  nullMeasureRows: agg.nullMeasureRows,
                  fileSig,
                };
              } else {
                diag = { totalSourceRows: totalRows, filteredRows, groupCount: 0, keyCapHit: true, nullMeasureRows: 0, fileSig };
              }
            }
          }
          // Always expose fileSig in _diag (even on the raw path) so the platform
          // proven-state machine can fingerprint the file it just read.
          if (!diag) diag = { totalSourceRows: totalRows, filteredRows, fileSig };
          // Surface the parse-cache outcome for observability (confirms the cache
          // hits on the paging / fan-out case).
          diag.cacheHit = cacheHit;

          // Select columns — skip when aggregated (the output already holds only
          // groupBy + measure-alias columns; projecting would strip the measures).
          if (!aggregationApplied && query.columns && query.columns.length > 0) {
            rows = rows.map(row => {
              const filtered: Record<string, unknown> = {};
              for (const col of query.columns!) {
                if (col in row) filtered[col] = row[col];
              }
              return filtered;
            });
          }

          // Pagination — skip entirely when an aggregationSpec was requested
          // (aggregated groups are final → the platform applies LIMIT; a cap-bail
          // returns the full filtered set so the platform aggregates correctly).
          if (!spec) {
            const offset = query.offset || 0;
            const limit = query.limit || 10000;
            rows = rows.slice(offset, offset + limit);
          }

          const columns = inferColumns(rows);

          const response: QueryResponse = {
            data: rows,
            totalRows,
            columns,
            aggregationApplied,
            agentVersion: getAgentVersion(),
            pushdownContractVersion: 1,
            _diag: diag,
          };
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(response));
          return;
        }

        if (req.url === '/sequence-region' && req.method === 'POST') {
          const body = await readBody(req);
          const params = JSON.parse(body) as {
            connectionId?: string;
            filePath?: string;
            sampleName: string;
            sequenceColumn?: string;  // Column containing the sequence (default: auto-detect)
            start?: number;           // Start position (0-based, default: 0)
            end?: number;             // End position (exclusive, default: 2000)
            context?: number;         // Extra bases on each side for overlap (default: 100)
          };

          // Resolve file
          let filePath = params.filePath;
          if (!filePath && params.connectionId) {
            filePath = resolveFilePath(params.connectionId) || '';
          }

          if (!filePath || !fs.existsSync(filePath)) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'File not found' }));
            return;
          }

          const { rows } = await getOrLoadParsedRows(filePath, loadFileData);

          // Find the sample row
          const sampleNameLower = params.sampleName.toLowerCase();
          const sampleRow = rows.find(row => {
            return Object.values(row).some(v =>
              typeof v === 'string' && v.toLowerCase() === sampleNameLower
            );
          });

          if (!sampleRow) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              error: `Sample "${params.sampleName}" not found`,
              availableSamples: rows.slice(0, 20).map(r => {
                const nameCol = Object.keys(r).find(k => k.includes('name') || k.includes('sample'));
                return nameCol ? r[nameCol] : Object.values(r)[0];
              }),
            }));
            return;
          }

          // Find the sequence column (auto-detect if not specified)
          let seqColumn = params.sequenceColumn;
          if (!seqColumn) {
            // Look for columns with long string values (>100 chars) that look like sequences
            for (const [key, value] of Object.entries(sampleRow)) {
              if (typeof value === 'string' && value.length > 100 && /^[ATCGNatcgn\s]+$/.test(value.substring(0, 200))) {
                seqColumn = key;
                break;
              }
            }
          }

          if (!seqColumn || !sampleRow[seqColumn]) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
              error: 'No sequence column found',
              columns: Object.keys(sampleRow),
              hint: 'Specify sequenceColumn parameter or ensure file has a column with DNA sequence data (ATCG characters)',
            }));
            return;
          }

          const fullSequence = String(sampleRow[seqColumn]);
          const totalLength = fullSequence.length;
          const contextBases = params.context || 100;
          const start = Math.max(0, (params.start || 0) - contextBases);
          const end = Math.min(totalLength, (params.end || 2000) + contextBases);
          const region = fullSequence.substring(start, end);

          // Include all metadata columns (everything except the sequence itself)
          const metadata: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(sampleRow)) {
            if (key !== seqColumn) {
              metadata[key] = value;
            }
          }

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            sampleName: params.sampleName,
            sequenceColumn: seqColumn,
            totalSequenceLength: totalLength,
            regionStart: start,
            regionEnd: end,
            regionLength: region.length,
            sequence: region,
            metadata,
            hasMore: end < totalLength,
            nextStart: end - contextBases,  // Overlap for continuity
          }));
          return;
        }

        if (req.url?.startsWith('/schema/') && req.method === 'GET') {
          const connectionId = req.url.split('/schema/')[1];
          const filePath = resolveFilePath(connectionId);

          if (!filePath || !fs.existsSync(filePath)) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'File not found for connection' }));
            return;
          }

          const { rows } = await getOrLoadParsedRows(filePath, loadFileData);
          const columns = inferColumns(rows);

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ columns, rowCount: rows.length, filePath }));
          return;
        }

        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
      } catch (err) {
        console.error('[QueryServer] Error:', err);
        // Phase 0 (2026-06-07): distinguish structured errors (FileTooLargeError,
        // RequestTooLargeError) from generic crashes so the platform can
        // surface a clear user message instead of timing out on a silent OOM.
        if (err instanceof FileTooLargeError) {
          res.writeHead(413, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            error: err.message,
            code: 'FILE_TOO_LARGE',
            fileSize: err.fileSize,
            limit: err.limit,
          }));
          return;
        }
        if (err instanceof RequestTooLargeError) {
          res.writeHead(413, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({
            error: err.message,
            code: 'REQUEST_BODY_TOO_LARGE',
            limit: err.limit,
          }));
          return;
        }
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err instanceof Error ? err.message : 'Internal error' }));
      }
    });

    server.listen(port, '127.0.0.1', () => {
      console.error(`[QueryServer] Listening on http://localhost:${port}`);
      resolve();
    });

    server.on('error', (err) => {
      console.error('[QueryServer] Failed to start:', err);
      reject(err);
    });
  });
}

/**
 * Stop the query server.
 */
export function stopQueryServer(): void {
  if (server) {
    server.close();
    server = null;
  }
}

/**
 * Phase 0 (2026-06-07): structured error for HTTP body exceeding the cap.
 */
class RequestTooLargeError extends Error {
  readonly statusCode = 413;
  readonly limit: number;
  constructor(limit: number) {
    super(
      `Request body exceeds the agent's ${(limit / 1024 / 1024).toFixed(0)}MB limit. ` +
      `Split the request or increase maxFileSize in ~/.universal-bi/config.json.`,
    );
    this.name = 'RequestTooLargeError';
    this.limit = limit;
  }
}

/**
 * Read an HTTP request body into a string.
 *
 * Phase 0 (2026-06-07): bounded by the same maxFileSize cap as file reads.
 * Pre-fix: unbounded — a 1GB POST body would silently exhaust the heap
 * before any file is even opened. Post-fix: destroys the request and
 * throws RequestTooLargeError when accumulated bytes pass the cap.
 */
function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const limit = getMaxFileSize();
    let bytesReceived = 0;
    let body = '';
    req.on('data', (chunk: Buffer | string) => {
      const chunkSize = typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.length;
      bytesReceived += chunkSize;
      if (bytesReceived > limit) {
        req.destroy();
        reject(new RequestTooLargeError(limit));
        return;
      }
      body += chunk;
    });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}
