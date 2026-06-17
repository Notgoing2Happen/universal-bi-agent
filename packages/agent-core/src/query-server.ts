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
import { loadState, computeFileHash } from './state';
import { loadConfig } from './config';
import { getOrLoadParsedRows } from './file-cache';
import { runSpec, runSpecWithCounters, runRealignmentVerdict, selfVerifyStream, runPassthroughSql, isDuckdbAvailable } from './duckdb-engine';
import type { SpecCounters, SelfVerifyResult } from './duckdb-engine';
import type { QuerySpec } from './query-spec';
import {
  normalizeColumnName,
  parseCsvLine,
  streamCsvRows,
  parseCsvFileBuffered,
  parseJsonFileBuffered,
  coerceDetailRows,
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
    column?: string;  // present for representable filters; ABSENT for a caseWhenSpec value-map filter
    operator: 'equals' | 'notEquals' | 'contains' | 'notContains' | 'startsWith' | 'notStartsWith' | 'endsWith' | 'notEndsWith' | 'gt' | 'lt' | 'gte' | 'lte' | 'set' | 'notSet';
    value?: string | number;
    values?: Array<string | number>;  // multi-value (IN / any-of)
    // Streaming value-map (caseWhenSpec) filter: the agent evaluates the AI #7 _cid canonical
    // value-map per row (a faithful replica of the platform's applyFiltersInMemory caseWhenSpec
    // branch) so it can early-exit on MATCHED rows. The valueMap arrives as [[rawLowerTrimmed,
    // canonical], ...] (a Map doesn't JSON-serialize); the streaming branch rehydrates it to a Map
    // ONCE before the row loop. The platform RE-FILTERS the page with the original caseWhenSpec
    // (value source-of-truth), so this eval need only be "never stricter" — byte-exact parity is
    // the simplest guarantee. See docs/streaming-detail-value-map-design.md.
    caseWhenSpec?: { sourceCol: string; valueMap: Array<[string, string]> | Map<string, string>; everyRowNonNull?: boolean };
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
  // B3 DETAIL-passthrough lane: the platform sets this ONLY when it has gated a DETAIL query
  // (no aggregation) that the streaming lane CAN'T serve — a non-representable filter
  // (caseWhenSpec/compound) the agent can't apply in-stream but DuckDB runs as real inlined
  // CASE-WHEN SQL. We run the translated SELECT via runPassthroughSql (all_varchar) and then
  // coerceDetailRows() it so the page byte-matches the production raw-detail baseline (blank→'',
  // trim, legacyCoerce). aggregationApplied:true → the page is FINAL (the DuckDB SQL already did
  // WHERE+LIMIT+OFFSET); the platform serves it verbatim, gated by its ordered byte-exact shadow.
  detailPassthrough?: {
    sql: string;
    expectedFileSig?: string;  // staleness guard
  };
  // Big-file streaming detail lane: the platform sets this true ONLY when it has gated
  // the DETAIL query (no aggregation) as streaming-safe — CSV/TSV, NO ORDER BY (the agent
  // has no `order` field, so the platform owns that decision), and every filter is
  // representable (sent in `filters`). The agent then streams the file with the SAME parser
  // the whole-file path uses, applies the filters per row, and early-exits at offset+limit
  // MATCHED rows — never loading the whole file (bypasses the maxFileSize cap for detail).
  // See docs/persistent-duckdb-agent-design.md (detail lane). Ignored if any spec is present.
  streamingDetail?: boolean;
  // Phase B — big-file ORDER BY top-N. When the platform sets this (with streamingDetail), the
  // streaming branch maintains a bounded top-K heap (K = offset+limit) by these fields instead of a
  // file-order early-exit, so a >cap file's "ORDER BY x DESC LIMIT n" serves the true top-N in O(K)
  // memory. The fields are RESOLVED RAW columns the agent's rows actually carry (the platform
  // resolves the alias → raw col + only sends a sort on a plain raw column; canonical/compound sorts
  // decline). The platform re-sorts the returned page with applySort (the order source-of-truth).
  orderBy?: Array<{ field: string; direction: 'asc' | 'desc' }>;
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
/**
 * Rehydrate a streaming-detail filter set: convert each value-map (caseWhenSpec) filter's
 * `valueMap` from its JSON wire form ([[rawLowerTrimmed, canonical], ...]) into a `Map` ONCE,
 * so the per-row `applyFilters` does a cheap `Map.get()` rather than rebuilding the Map for every
 * streamed row. Idempotent (a `valueMap` already a Map is left as-is). Representable filters pass
 * through untouched. Returns a NEW array; never mutates the input.
 */
export function rehydrateStreamingFilters(filters: QueryRequest['filters']): QueryRequest['filters'] {
  if (!filters || filters.length === 0) return filters;
  return filters.map(f => {
    if (f.caseWhenSpec && !(f.caseWhenSpec.valueMap instanceof Map)) {
      return { ...f, caseWhenSpec: { ...f.caseWhenSpec, valueMap: new Map(f.caseWhenSpec.valueMap || []) } };
    }
    return f;
  });
}

/**
 * Comparator BYTE-IDENTICAL to the platform's applySort (nango-driver.js applySort), for ONE pair
 * of rows given resolvedOrder [{field, direction}]. Per field, in order: nulls sort LAST; numeric
 * (parseFloat) compare if BOTH parse; else String.localeCompare. Returns <0 / 0 / >0. A full tie
 * (0) is broken by the CALLER on stream index, matching applySort's stable (V8) sort.
 *
 * ⚠ localeCompare uses the runtime default locale (identical across our Node runtimes in practice).
 * It is only reached for non-numeric sort keys; and the PLATFORM RE-SORTS the returned page with
 * applySort (the order source-of-truth), so this comparator governs only the top-K SELECTION. The
 * parity test (value-map / top-N) pins selection == applySort byte-for-byte.
 */
type OrderSpec = { field: string; direction: 'asc' | 'desc' };
// ISO 8601 date prefix (YYYY-MM-DD) — drives the B0 date-aware sort branch. Mirrors the platform's
// ISO_DATE_PREFIX in applySort / applyFiltersInMemory's orderedCompare.
const ISO_DATE_PREFIX_SORT = /^\d{4}-\d{2}-\d{2}/;
export function compareByStreamingOrder(a: Record<string, unknown>, b: Record<string, unknown>, order: OrderSpec[]): number {
  for (const { field, direction } of order) {
    const aVal = a[field];
    const bVal = b[field];
    if (aVal == null && bVal == null) continue;
    if (aVal == null) return 1;
    if (bVal == null) return -1;
    // ISO-date-aware (B0): a date-shaped value sorts as a TIMESTAMP, not by parseFloat (year-only).
    // BYTE-IDENTICAL to the platform applySort's date branch (pinned by topn-heap-parity.test.cjs).
    const aStr = String(aVal);
    const bStr = String(bVal);
    if (ISO_DATE_PREFIX_SORT.test(aStr) || ISO_DATE_PREFIX_SORT.test(bStr)) {
      const aT = Date.parse(aStr);
      const bT = Date.parse(bStr);
      if (!isNaN(aT) && !isNaN(bT)) {
        if (aT !== bT) return direction === 'asc' ? aT - bT : bT - aT;
        continue;  // equal timestamps → next field
      }
      // one side isn't a parseable date → fall through to the legacy numeric/string compare
    }
    const aNum = parseFloat(aVal as string);
    const bNum = parseFloat(bVal as string);
    if (!isNaN(aNum) && !isNaN(bNum)) {
      if (aNum !== bNum) return direction === 'asc' ? aNum - bNum : bNum - aNum;
    } else {
      const cmp = String(aVal).localeCompare(String(bVal));
      if (cmp !== 0) return direction === 'asc' ? cmp : -cmp;
    }
  }
  return 0;
}

/**
 * Classify a sort-key value for CROSS-RUNTIME-SAFE streaming top-N selection (Phase B). The agent
 * (desktop, arbitrary TZ/locale) SELECTS the top-N; the platform (server) re-sorts but cannot
 * re-select — so the selection comparator must be DETERMINISTIC across runtimes. parseFloat/numeric
 * is identical everywhere; a date-shaped value (Date.parse is TZ-dependent for tz-less datetimes) or
 * a non-numeric string (localeCompare is ICU-dependent) is NOT — so those make the agent bail
 * (orderByNonNumeric) → platform falls back. null/blank are safe (nulls-last, value-independent).
 * Returns true = SAFE to stream-select; false = UNSAFE (bail). Exported for the parity test.
 */
export function isCrossRuntimeSafeSortValue(v: unknown): boolean {
  if (v === null || v === undefined || v === '') return true;
  const s = String(v);
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return false;   // date-shaped → TZ-dependent
  return !isNaN(parseFloat(s));                      // non-numeric string → ICU-dependent
}

/**
 * Bounded top-K selector (a max-heap keeping the K rows that rank FIRST by compareByStreamingOrder
 * + a stream-index tie-break). Streams any number of rows in O(rows·log K) time and O(K) memory —
 * so a >cap file's ORDER BY top-N never loads the whole file. The full comparator (order then index)
 * makes the boundary selection STABLE, identical to applySort's stable sort (so a tie at the Nth/N+1th
 * row keeps the SAME row the platform would). `result()` returns the kept rows sorted best-first; the
 * streaming branch then slices [offset, offset+limit]. The platform re-sorts the page (idempotent).
 */
export class BoundedTopK {
  private k: number;
  private order: OrderSpec[];
  private h: Array<{ row: Record<string, unknown>; idx: number }> = [];
  constructor(k: number, order: OrderSpec[]) { this.k = Math.max(0, k | 0); this.order = order; }
  // full comparator: <0 if x ranks BEFORE y (order, then earlier stream index wins ties).
  private cmp(x: { row: Record<string, unknown>; idx: number }, y: { row: Record<string, unknown>; idx: number }): number {
    const c = compareByStreamingOrder(x.row, y.row, this.order);
    return c !== 0 ? c : (x.idx - y.idx);
  }
  offer(row: Record<string, unknown>, idx: number): void {
    if (this.k === 0) return;
    const e = { row, idx };
    if (this.h.length < this.k) { this.h.push(e); this.up(this.h.length - 1); }
    else if (this.cmp(e, this.h[0]) < 0) { this.h[0] = e; this.down(0); } // e better than the worst (root) → evict root
  }
  // MAX-heap: the root is the WORST (ranks last) of the kept K — the eviction candidate.
  private up(i: number): void { while (i > 0) { const p = (i - 1) >> 1; if (this.cmp(this.h[i], this.h[p]) > 0) { [this.h[i], this.h[p]] = [this.h[p], this.h[i]]; i = p; } else break; } }
  private down(i: number): void { const n = this.h.length; for (;;) { const l = 2 * i + 1, r = 2 * i + 2; let m = i; if (l < n && this.cmp(this.h[l], this.h[m]) > 0) m = l; if (r < n && this.cmp(this.h[r], this.h[m]) > 0) m = r; if (m === i) break; [this.h[i], this.h[m]] = [this.h[m], this.h[i]]; i = m; } }
  result(): Record<string, unknown>[] { return this.h.slice().sort((a, b) => this.cmp(a, b)).map((e) => e.row); }
}

export function applyFilters(rows: Record<string, unknown>[], filters: QueryRequest['filters']): Record<string, unknown>[] {
  if (!filters || filters.length === 0) return rows;

  return rows.filter(row => {
    return filters.every(f => {
      // ── Value-map (caseWhenSpec) filter — BYTE-IDENTICAL to the platform's
      //    applyFiltersInMemory caseWhenSpec branch (nango-driver.js L5403-5457).
      //    Evaluate the AI #7 _cid canonical value-map on the RAW source column, then
      //    compare the canonical-resolved value with the operator. The valueMap is a Map
      //    after the streaming branch rehydrated it; tolerate an array form defensively
      //    (built once, not per row, since the rehydrate runs before the loop). ──
      if (f.caseWhenSpec) {
        const spec = f.caseWhenSpec;
        const vm = spec.valueMap instanceof Map ? spec.valueMap : new Map(spec.valueMap || []);
        const raw = row[spec.sourceCol];
        let caseResult: string | null;
        if (raw == null || raw === '') {
          caseResult = null;
        } else {
          const norm = String(raw).trim().toLowerCase();
          const matched = vm.get(norm);
          caseResult = matched !== undefined ? matched : (spec.everyRowNonNull ? String(raw) : null);
        }
        if (f.operator === 'set') return caseResult !== null && caseResult !== undefined && caseResult !== '';
        if (f.operator === 'notSet') return caseResult === null || caseResult === undefined || caseResult === '';
        const cv = caseResult == null ? null : String(caseResult);
        const fvals = (f.values && f.values.length) ? f.values : (f.value !== undefined ? [f.value] : []);
        switch (f.operator) {
          case 'equals':        return cv !== null && fvals.some(x => cv === String(x));
          case 'notEquals':     return cv === null || !fvals.some(x => cv === String(x));
          case 'contains':      return cv !== null && fvals.some(x => cv.toLowerCase().includes(String(x).toLowerCase()));
          case 'notContains':   return cv === null || !fvals.some(x => cv.toLowerCase().includes(String(x).toLowerCase()));
          case 'startsWith':    return cv !== null && fvals.some(x => cv.toLowerCase().startsWith(String(x).toLowerCase()));
          case 'notStartsWith': return cv === null || !fvals.some(x => cv.toLowerCase().startsWith(String(x).toLowerCase()));
          case 'endsWith':      return cv !== null && fvals.some(x => cv.toLowerCase().endsWith(String(x).toLowerCase()));
          case 'notEndsWith':   return cv === null || !fvals.some(x => cv.toLowerCase().endsWith(String(x).toLowerCase()));
          default:              return true;  // unknown op against a CASE WHEN → conservative pass (matches platform)
        }
      }
      const val = row[f.column as string];
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
  spec: { groupBy: string[]; aggregations: Array<{ type: string; column: string; alias: string; distinct?: boolean }> },
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
    agg: { type: string; column: string; distinct?: boolean },
  ): number | null => {
    const get = (r: Record<string, unknown>) => r[agg.column];
    switch (agg.type) {
      case 'count':
        if (agg.column === '*' || agg.column === '1') return groupRows.length;
        if (agg.distinct) {
          // COUNT(DISTINCT COALESCE(col,'')) — null AND blank collapse to one '' value
          // (byte-matches duckdb-engine aggExpr; DuckDB-streamed rows give null for blanks).
          const seen = new Set<string>();
          for (const r of groupRows) { const v = get(r); seen.add(v == null ? '' : String(v)); }
          return seen.size;
        }
        return groupRows.filter(r => get(r) != null).length;
      case 'sum':
        return groupRows.reduce((s, r) => s + (parseFloat(get(r) as any) || 0), 0);
      case 'avg': {
        // Match duckdb-engine aggExpr / platform computeAggregate EXACTLY: over rows where
        // the value is non-null AND non-blank, sum parseFloat(v)||0 (non-numeric text → 0 in
        // the numerator) and divide by the COUNT of those rows. Empty group → null. (DuckDB's
        // bare AVG(TRY_CAST) is WRONG here — it drops non-numeric from BOTH sum and count.)
        let num = 0;
        let cnt = 0;
        for (const r of groupRows) {
          const v = get(r);
          if (v != null && v !== '') { num += parseFloat(v as any) || 0; cnt++; }
        }
        return cnt > 0 ? num / cnt : null;
      }
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
            // Phase-1 (observe-only): agent computes a column-integrity verdict in DuckDB
            // when the platform sends `profiles` on a querySpec. The platform compares it to
            // the raw-row realigner; it gates NO serve yet. Capability-gated so the platform
            // only ships profiles to agents that emit the verdict.
            supportsRealignmentCheck: true,
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

          // ── Phase-1 column-integrity verdict (OBSERVE-ONLY) ───────────────────────────
          // When the platform ships per-column profiles on a querySpec, compute a DuckDB
          // realignment verdict (own-fit / shift-evidence over the warm file table) and stamp
          // it into the response _diag. The platform COMPARES it to the raw-row realigner; it
          // gates NO serve until proven to agree. SUB-CAP ONLY: that's where a raw-row Node
          // realigner baseline also exists, so the comparison is meaningful (big files have no
          // baseline to compare against in Phase 1). Engine-path only (no CLI) → 'unverified'
          // when the engine is down. Never throws into the query path.
          let realignDiag: Record<string, number | boolean | string> = {};
          if (query.querySpec?.profiles && query.querySpec.profiles.length) {
            let subCap = true;
            try { subCap = fs.statSync(filePath).size <= getMaxFileSize(); } catch { /* default subCap true */ }
            if (subCap) {
              try {
                const v = await runRealignmentVerdict(query.querySpec.profiles, filePath);
                realignDiag = {
                  realignmentChecked: v.checked,
                  realignmentVerdict: v.verdict,
                  realignmentTotalRows: v.totalRows,
                  realignmentMinOwnFrac: Number(v.minOwnFrac.toFixed(4)),
                  realignmentMaxShiftFrac: Number(v.maxShiftFrac.toFixed(4)),
                };
              } catch { realignDiag = { realignmentChecked: false, realignmentVerdict: 'unverified' }; }
            }
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
          // Content hash computed on the SUB-cap fast path (reused by the engine-miss
          // fall-through's getOrLoadParsedRows so the file isn't hashed twice). '' otherwise.
          let fastPathSha = '';
          // ── Engine fast path (broadened from the >cap large-file branch) ──────────────
          // AGENT_DUCKDB_ENGINE_FASTPATH: route ANY DuckDB-readable file through the in-process
          // engine WITHOUT the whole-file Node load (getOrLoadParsedRows) — the engine scans the
          // file from disk, so a 22-50MB CSV no longer times out loading into Node. The legacy
          // AGENT_DUCKDB_LARGE_FILES flag keeps the >cap-ONLY behavior (mtime:size sig, 413 on miss).
          //   • sub-cap engine MISS → FALL THROUGH to the normal path (lazy Node load + raw
          //     fallback), reusing the content hash we already computed (no 2nd hash).
          //   • >cap engine MISS → HONEST 413 (a >cap file cannot be loaded into Node).
          // xlsx / no-engine / direct-DB (no filePath → handler not invoked) take the path UNCHANGED.
          const fastPathFlag = process.env.AGENT_DUCKDB_ENGINE_FASTPATH === 'true';
          if (fastPathFlag || process.env.AGENT_DUCKDB_LARGE_FILES === 'true') {
            const lext = path.extname(filePath).toLowerCase();
            const duckReadable = lext === '.csv' || lext === '.tsv' || lext === '.json' || lext === '.parquet';
            let fileBytes = 0;
            try { fileBytes = fs.statSync(filePath).size; } catch { /* leave 0 */ }
            const isLargeFile = fileBytes > getMaxFileSize();
            // Legacy flag alone → preserve the >cap-only gate; fast-path flag → fire for ANY size.
            const sizeOK = fastPathFlag ? true : isLargeFile;
            const isV2 = !!(query.querySpec && query.querySpec.contractVersion === 2 &&
              Array.isArray(query.querySpec.aggregations) && query.querySpec.aggregations.length > 0);
            const isSp = !!(query.sqlPassthrough && typeof query.sqlPassthrough.sql === 'string' && query.sqlPassthrough.sql.trim());
            const isDp = !!(query.detailPassthrough && typeof query.detailPassthrough.sql === 'string' && query.detailPassthrough.sql.trim());
            if ((isV2 || isSp || isDp) && duckReadable && sizeOK && isDuckdbAvailable()) {
              // SIG: a >cap large file keeps mtime:size (legacy distinct contract — never loaded or
              // hashed; hashing a >cap file would itself be a full read we avoid). A SUB-cap fast-path
              // serve MUST carry the CONTENT hash, or the platform's shadow same-file guard
              // (meta.fileSig === fetchSig, where fetchSig is `sha:…`) skips every comparison and the
              // cube never proves. computeFileHash is a streaming SHA-256 (no parse) — one cheap read.
              let sig = '';
              if (isLargeFile) {
                try { const st = fs.statSync(filePath); sig = `${Math.round(st.mtimeMs)}:${st.size}`; } catch { /* */ }
              } else {
                try { fastPathSha = await computeFileHash(filePath); sig = `sha:${fastPathSha}`; }
                catch { try { const st = fs.statSync(filePath); sig = `${Math.round(st.mtimeMs)}:${st.size}`; } catch { /* */ } }
              }
              const ver = (isSp || isDp) ? 3 : 2;
              const engineName = (isSp || isDp) ? 'duckdb-sql' : 'duckdb';
              const sendFP = (r: QueryResponse) => { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(r)); };
              // Staleness guard — content-exact for sub-cap (sig is `sha:…`); a mismatch means the
              // file changed since the platform proved this shape → no rows, the platform re-proves.
              const expSig = query.querySpec?.expectedFileSig || query.sqlPassthrough?.expectedFileSig || query.detailPassthrough?.expectedFileSig;
              if (expSig && sig && expSig !== sig) {
                sendFP({ data: [], totalRows: -1, columns: [], aggregationApplied: false, agentVersion: getAgentVersion(), pushdownContractVersion: ver, _diag: { ...(isLargeFile ? { largeFile: true } : { enginePushdown: true }), fileBytes, sigMismatch: true, fileSig: sig, engine: engineName } });
                return;
              }
              let fpRows: Record<string, unknown>[] | null = null;
              let fpCounters: SpecCounters | null = null;
              let fpSelfVerify: SelfVerifyResult | null = null;
              let fpErr = '';
              const selfVerifyOn = process.env.AGENT_DUCKDB_V2_SELFVERIFY === 'true'
                && !!query.querySpec?.fullProfiles
                && Object.keys(query.querySpec.fullProfiles).length > 0;
              try {
                if (isDp) {
                  // B3 DETAIL page: run the translated SELECT (WHERE+LIMIT+OFFSET already in the SQL),
                  // then coerceDetailRows so the all_varchar page byte-matches the production raw-detail
                  // baseline (blank→'', trim, legacyCoerce). The platform's ordered byte-exact shadow
                  // gates any serve, so a coercion residual (quoted intentional whitespace) only declines.
                  fpRows = coerceDetailRows(await runPassthroughSql(query.detailPassthrough!.sql, filePath));
                } else if (isSp) {
                  // Passthrough counters aren't derivable from a spec → deferred (parity: today's
                  // sub-cap passthrough serve already omits filtered/null counters).
                  fpRows = await runPassthroughSql(query.sqlPassthrough!.sql, filePath);
                } else if (selfVerifyOn) {
                  // Self-verify serve path: DuckDB aggregate + one shared stream → exact realigner
                  // (Leg 3) + JS reference agg (Leg 1) + dirty-cast (Leg 2). Runs for sub-cap AND
                  // warm large files (the stream handles either; >256MB direct-scan → checked:false).
                  // The platform only sends fullProfiles when building/serving the self-verify streak,
                  // so the stream cost is platform-gated. Always returns the DuckDB rows to serve.
                  fpSelfVerify = await selfVerifyStream(
                    query.querySpec!,
                    query.querySpec!.fullProfiles as Parameters<typeof selfVerifyStream>[1],
                    filePath,
                  );
                  fpRows = fpSelfVerify.rows;
                  fpCounters = fpSelfVerify.counters;
                } else if (isLargeFile) {
                  // >cap: no counters companion (a 2nd COUNT(*) scan of a huge file) — legacy behavior.
                  fpRows = await runSpec(query.querySpec!, filePath);
                } else {
                  const r = await runSpecWithCounters(query.querySpec!, filePath);
                  fpRows = r.rows; fpCounters = r.counters;
                }
              } catch (e) { fpErr = e instanceof Error ? e.message : String(e); }
              if (fpRows) {
                const diag: Record<string, number | boolean | string> = {
                  ...(isLargeFile ? { largeFile: true } : { enginePushdown: true }),
                  fileBytes, fileSig: sig, engine: engineName, groupCount: fpRows.length,
                  ...(isSp ? { supportsSqlPassthrough: true } : {}),
                  ...(isDp ? { detailPassthrough: true, supportsSqlPassthrough: true } : {}),
                  ...realignDiag,
                };
                if (fpCounters) {
                  diag.totalSourceRows = fpCounters.totalSourceRows;
                  diag.filteredRows = fpCounters.filteredRows;
                  diag.nullMeasureRows = fpCounters.nullMeasureRows;
                } else {
                  diag.countersDeferred = true;
                }
                // Self-verify flags (the platform serve gate's self-verify branch reads these).
                // checked:false (incomplete stream / >cap) → flags stay false → no proof, never wrong.
                if (fpSelfVerify) {
                  diag.selfVerifyChecked = fpSelfVerify.checked;
                  diag.realignmentClean = fpSelfVerify.realignmentClean;
                  diag.aggregateSelfVerified = fpSelfVerify.aggregateSelfVerified;
                  diag.dirtyCastCount = fpSelfVerify.dirtyCastCount;
                  if (query.querySpec?.selfVerifyShapeSig) diag.selfVerifyShapeSig = query.querySpec.selfVerifyShapeSig;
                  if (fpSelfVerify.realignSummary) {
                    diag.svRealigned = fpSelfVerify.realignSummary.realigned;
                    diag.svSuspectedShift = fpSelfVerify.realignSummary.suspectedShift;
                    diag.svTotal = fpSelfVerify.realignSummary.total;
                  }
                }
                sendFP({ data: fpRows, totalRows: fpCounters ? fpCounters.totalSourceRows : -1, columns: inferColumns(fpRows), aggregationApplied: true, agentVersion: getAgentVersion(), pushdownContractVersion: ver, _diag: diag });
                return;
              }
              // Engine miss/error. >cap → HONEST 413 (cannot load into Node). Sub-cap → FALL THROUGH
              // to the normal path below (getOrLoadParsedRows + raw fallback), reusing fastPathSha.
              if (isLargeFile) {
                res.writeHead(413, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: `File too large to aggregate (${fileBytes} bytes) and the DuckDB engine could not process it${fpErr ? ': ' + fpErr.slice(0, 160) : ''}.` }));
                return;
              }
              if (fpErr) console.warn('[query] engine fast-path miss — falling through to Node load:', fpErr.slice(0, 160));
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

          // ── Streaming detail lane (big-file CSV/TSV detail, no spec) ──────────────
          // The platform sets streamingDetail:true ONLY when it has gated this DETAIL
          // query as streaming-safe (CSV/TSV, no ORDER BY, all filters representable +
          // sent in query.filters). We stream the file with streamCsvRows — which shares
          // buildParser/legacyCoerce with parseCsvFileBuffered, so a streamed row is
          // BYTE-IDENTICAL (value+type+null) to the whole-file path (proven by
          // streaming-detail-soundness.test.cjs) — apply the filters per row, skip
          // `offset` MATCHED rows, collect the next `limit`, and break. Native backpressure
          // tears down the read on break → early exit, NO whole-file load, so it never calls
          // getOrLoadParsedRows→enforceFileSizeCap (a >maxFileSize file serves its page
          // instead of 413). aggregationApplied:false → the platform realigns + treats the
          // rows as raw, identical to the small-file detail path. CSV/TSV + no-spec only;
          // JSON/XLSX have no streaming-row coercion path.
          {
            const sdExt = path.extname(filePath).toLowerCase();
            // Stream ONLY when the file is over the whole-file load cap — a sub-cap file
            // keeps the cached-load path (shared parse cache across fan-out/pages + a real
            // totalRows). The platform may hint streamingDetail for any size (it can't see
            // the agent's file size); the agent decides big-vs-small here.
            let sdBig = false;
            try { sdBig = fs.statSync(filePath).size > getMaxFileSize(); } catch { sdBig = false; }
            if (query.streamingDetail === true && sdBig
                && !query.aggregationSpec && !query.querySpec && !query.sqlPassthrough && !query.detailPassthrough
                && (sdExt === '.csv' || sdExt === '.tsv')) {
              const offset = query.offset || 0;
              const limit = query.limit || 10000;
              // Rehydrate any value-map (caseWhenSpec) filter's valueMap from its wire form
              // ([[raw, canonical], ...]) to a Map ONCE — so the per-row applyFilters does a
              // cheap Map.get() instead of rebuilding the Map for every streamed row.
              const filters = rehydrateStreamingFilters(query.filters);
              const orderBy = Array.isArray(query.orderBy) ? query.orderBy : null;
              let collected: Record<string, unknown>[];
              if (orderBy && orderBy.length > 0) {
                // ── Phase B: ORDER BY top-N — bounded heap, NO early-exit ──
                // A sorted top-N needs the TRUE top-K across the whole file, so we cannot stop at
                // offset+limit MATCHED rows (file order ≠ sorted order). Stream every (filtered) row
                // through a size-(offset+limit) max-heap keyed by compareByStreamingOrder + stream
                // index (stable, == applySort). O(rows·log K) time, O(K) memory — a >cap file never
                // loads whole. The platform re-sorts the returned page (applySort) as source of truth.
                const K = offset + limit;
                const heap = new BoundedTopK(K, orderBy);
                let idx = 0;
                const fieldSeen = orderBy.map(() => false);   // PER-FIELD presence (not "any field")
                let unsafeSortKey = false;                     // a date-shaped / non-numeric sort value
                for await (const row of streamCsvRows(filePath, { delimiter: sdExt === '.tsv' ? '\t' : undefined })) {
                  if (filters && filters.length && applyFilters([row], filters).length === 0) continue;
                  for (let fi = 0; fi < orderBy.length; fi++) {
                    if (!(orderBy[fi].field in row)) continue;
                    fieldSeen[fi] = true;
                    // SELECTION must be cross-runtime DETERMINISTIC → numeric keys only (parseFloat is
                    // identical everywhere; date/string compare is TZ/ICU-dependent). Phase B v1 = numeric
                    // ORDER BY; date/string top-N deferred until TZ/locale are pinned in both runtimes.
                    if (!unsafeSortKey && !isCrossRuntimeSafeSortValue(row[orderBy[fi].field])) unsafeSortKey = true;
                  }
                  heap.offer(row, idx++);
                }
                const bailStreaming = (diagKey: string) => {
                  // Fail HONESTLY (no platform selection backstop) — the platform throws / falls back
                  // (for a >cap file the raw fallback 413s, which is the honest outcome).
                  res.writeHead(200, { 'Content-Type': 'application/json' });
                  res.end(JSON.stringify({
                    data: [], totalRows: -1, columns: [], aggregationApplied: false,
                    agentVersion: getAgentVersion(), pushdownContractVersion: 1,
                    _diag: { streamingDetail: true, [diagKey]: true, fileSig },
                  }));
                };
                // FIELD-ABSENT GUARD (PER-FIELD): ANY ordered field absent across ALL rows → the heap
                // order is bogus for that field (an alias≠raw mismatch, or an absent secondary key). Don't
                // serve a wrong top-N.
                if (idx > 0 && fieldSeen.some((seen) => !seen)) { bailStreaming('orderByFieldAbsent'); return; }
                // NUMERIC-ONLY GUARD: a date/string sort key → cross-runtime (TZ/ICU) selection divergence.
                if (idx > 0 && unsafeSortKey) { bailStreaming('orderByNonNumeric'); return; }
                collected = heap.result().slice(offset, offset + limit);   // sorted top-K, then the page
              } else {
                // ── Filter-only detail: file-order early-exit at offset+limit MATCHED rows ──
                collected = [];
                let matched = 0;
                for await (const row of streamCsvRows(filePath, { delimiter: sdExt === '.tsv' ? '\t' : undefined })) {
                  // Reuse the EXACT whole-file filter semantics (applyFilters) per row.
                  if (filters && filters.length && applyFilters([row], filters).length === 0) continue;
                  if (matched++ < offset) continue;        // skip `offset` MATCHED rows
                  collected.push(row);
                  if (collected.length >= limit) break;    // early exit — tears down the read
                }
              }
              let outRows = collected;
              if (query.columns && query.columns.length > 0) {
                outRows = collected.map(row => {
                  const f: Record<string, unknown> = {};
                  for (const col of query.columns!) if (col in row) f[col] = row[col];
                  return f;
                });
              }
              // CAPABILITY ECHO (Phase A safety): when this request carried a value-map (caseWhenSpec)
              // filter, echo valueMapApplied:true — this build HAS the caseWhenSpec evaluator
              // (applyFilters branch). An OLD agent lacks this echo, so the platform can refuse to
              // serve a value-map streamed page unless the echo is present (→ no silent-empty serve
              // from an old agent that read row[undefined] and dropped every row).
              const valueMapApplied = !!(filters && filters.some((f) => f && f.caseWhenSpec));
              // CAPABILITY ECHO (Phase B safety): when this >cap stream carried an ORDER BY, the heap
              // above computed the TRUE bounded top-N (numeric — the orderByNonNumeric/Field-absent bails
              // returned earlier). Echo orderByApplied:true so the platform's Path-B gate SERVES it (vs an
              // old agent that lacks this echo → the platform throws agent_orderby_unsupported, never a
              // silent first-N serve). Only set when orderBy was actually applied (not the filter-only lane).
              const orderByApplied = !!(orderBy && orderBy.length > 0);
              const response: QueryResponse = {
                data: outRows,
                totalRows: -1, // streamed: whole-file count not read (would defeat the early exit)
                columns: inferColumns(outRows),
                aggregationApplied: false,
                agentVersion: getAgentVersion(),
                pushdownContractVersion: 1,
                _diag: { streamingDetail: true, fileSig, returnedRows: outRows.length, ...(valueMapApplied ? { valueMapApplied: true } : {}), ...(orderByApplied ? { orderByApplied: true } : {}) },
              };
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify(response));
              return;
            }
          }

          // Parse file (cached: one parse serves all pages / fan-out cubes /
          // repeat questions; keyed by content hash so any file change busts it).
          // READ-ONLY: `rows` may be a SHARED cached array — consumers below
          // (applyFilters/applyAggregations/projection/slice) all return new
          // arrays/objects and never mutate it in place. Keep it that way.
          // `fastPathSha` is set only when the sub-cap engine fast path hashed this file and then
          // missed → reuse it so we don't read+hash a 2nd time (loader still reads current bytes).
          const loaded = await getOrLoadParsedRows(filePath, loadFileData, fastPathSha || undefined);
          let rows = loaded.rows;
          const cacheHit = loaded.cacheHit;
          const totalRows = rows.length;
          // Prefer the content hash when the cache produced one (it does unless
          // AGENT_FILECACHE_DISABLED). `sha:` prefix marks the scheme.
          if (loaded.sha256) fileSig = `sha:${loaded.sha256}`;

          // Apply filters
          rows = applyFilters(rows, query.filters);
          const filteredRows = rows.length;

          // ── Phase B sub-cap: ORDER BY top-N over the whole-file set (wf wnjexl3wa BLOCKER-3) ──
          // The >cap streaming branch (above) handles big files via BoundedTopK and returns early. A
          // SUB-cap ordered-detail query reaches here with the full file parsed + filtered (`rows`).
          // We MUST handle the ORDER BY and RETURN our own response BEFORE the file-order slice at the
          // bottom (L~1320, which does NOT sort) — else the agent ships first-N FILE-order rows and the
          // platform sorts a wrong subset (the exact bug Path A fixed). Mirrors the >cap lane's guards.
          if (query.streamingDetail === true && Array.isArray(query.orderBy) && query.orderBy.length > 0
              && !query.aggregationSpec && !query.querySpec && !query.sqlPassthrough && !query.detailPassthrough) {
            const sdOrderBy = query.orderBy;
            const sdOffset = query.offset || 0;                                    // platform declines offset>0 (B1); defensive
            const sdLimit = query.limit != null ? query.limit : rows.length;       // platform declines null limit (M6); defensive
            const sdFieldSeen = sdOrderBy.map(() => false);                        // PER-field presence (== >cap lane)
            let sdUnsafe = false;                                                  // a date-shaped / non-numeric sort value
            for (const row of rows) {
              for (let fi = 0; fi < sdOrderBy.length; fi++) {
                if (!(sdOrderBy[fi].field in row)) continue;
                sdFieldSeen[fi] = true;
                // SELECTION must be cross-runtime DETERMINISTIC → numeric keys only (parseFloat identical
                // everywhere; date/string compare is TZ/ICU-dependent). Matches the >cap guard exactly.
                if (!sdUnsafe && !isCrossRuntimeSafeSortValue(row[sdOrderBy[fi].field])) sdUnsafe = true;
              }
            }
            const sdProject = (rs: Record<string, unknown>[]) =>
              (query.columns && query.columns.length > 0)
                ? rs.map((row) => { const f: Record<string, unknown> = {}; for (const col of query.columns!) if (col in row) f[col] = row[col]; return f; })
                : rs;
            const sendSd = (data: Record<string, unknown>[], extraDiag: Record<string, boolean>) => {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify({
                data, totalRows, columns: inferColumns(data), aggregationApplied: false,
                agentVersion: getAgentVersion(), pushdownContractVersion: 1,
                _diag: { streamingDetail: true, fileSig, returnedRows: data.length, ...extraDiag },
              }));
            };
            // FIELD-ABSENT → the platform can't sort by it either → honest bail (platform throws
            // agent_orderby_unsortable). Mirrors the >cap orderByFieldAbsent guard.
            if (rows.length > 0 && sdFieldSeen.some((s) => !s)) { sendSd([], { orderByFieldAbsent: true }); return; }
            if (!sdUnsafe) {
              // NUMERIC-safe → the agent selects the TRUE global top-N. CLONE (MINOR-8): applyFilters with
              // no filters returns the shared cached parse by reference; an in-place sort would corrupt it.
              // V8 sort is stable + `rows` is in filtered-file order, so ties match applySort's tie-break.
              const sdSorted = [...rows].sort((a, b) => compareByStreamingOrder(a, b, sdOrderBy));
              sendSd(sdProject(sdSorted.slice(sdOffset, sdOffset + sdLimit)), { orderByApplied: true });
              return;
            }
            // DATE/STRING (cross-runtime-unsafe) → DEFER: return ALL filtered rows (projected to
            // query.columns, which the driver widened to include the sort key) so the driver's applySort +
            // slice selects the true top-N in ONE runtime. Do NOT slice here.
            sendSd(sdProject(rows), { orderByDeferred: true });
            return;
          }

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

          // ── Agent DuckDB DETAIL-passthrough (B3, sub-cap) ─────────────────
          // Mirror of the sp branch for a DETAIL SELECT (no aggregation): run the translated
          // SELECT, then coerceDetailRows so the all_varchar page byte-matches the production
          // raw-detail baseline. aggregationApplied:true → the platform serves the page as FINAL
          // (the SQL did WHERE+LIMIT+OFFSET), gated by its ordered byte-exact shadow. On a DuckDB
          // miss → raw `src` + aggregationApplied:false so the platform records no shadow result.
          const dp = query.detailPassthrough;
          if (dp && typeof dp.sql === 'string' && dp.sql.trim()) {
            const src = loaded.rows;
            const send = (r: QueryResponse) => {
              res.writeHead(200, { 'Content-Type': 'application/json' });
              res.end(JSON.stringify(r));
            };
            if (dp.expectedFileSig && dp.expectedFileSig !== fileSig) {
              send({
                data: [], totalRows: src.length, columns: [], aggregationApplied: false,
                agentVersion: getAgentVersion(), pushdownContractVersion: 3,
                _diag: { totalSourceRows: src.length, sigMismatch: true, fileSig, cacheHit, engine: 'duckdb-sql', detailPassthrough: true, supportsSqlPassthrough: true },
              });
              return;
            }
            let outRows: Record<string, unknown>[] | null = null;
            let sqlError = '';
            try {
              outRows = coerceDetailRows(await runPassthroughSql(dp.sql, filePath));
            } catch (e) {
              sqlError = e instanceof Error ? e.message : String(e);
              console.warn('[query] DuckDB detail_passthrough failed — raw fallback:', sqlError.substring(0, 160));
            }
            if (outRows) {
              send({
                data: outRows, totalRows: src.length, columns: inferColumns(outRows),
                aggregationApplied: true, agentVersion: getAgentVersion(), pushdownContractVersion: 3,
                _diag: { totalSourceRows: src.length, fileSig, cacheHit, engine: 'duckdb-sql', groupCount: outRows.length, detailPassthrough: true, supportsSqlPassthrough: true },
              });
              return;
            }
            send({
              data: src, totalRows: src.length, columns: inferColumns(src),
              aggregationApplied: false, agentVersion: getAgentVersion(), pushdownContractVersion: 3,
              _diag: { totalSourceRows: src.length, fileSig, cacheHit, engine: 'js-fallback', duckdbAvailable: isDuckdbAvailable(), detailPassthrough: true, supportsSqlPassthrough: true, ...(sqlError ? { sqlError: sqlError.substring(0, 200) } : {}) },
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
                _diag: { ...counters, ...realignDiag, engine: 'duckdb', groupCount: duckRows.length },
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
              _diag: { ...counters, ...realignDiag, engine: 'js-fallback', duckdbAvailable: isDuckdbAvailable() },
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
