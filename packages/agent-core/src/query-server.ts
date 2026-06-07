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
import { normalizeColumnName, parseCsvLine, parseCsvFileBuffered } from './parsers';

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
    operator: 'equals' | 'notEquals' | 'contains' | 'gt' | 'lt' | 'gte' | 'lte';
    value: string | number;
  }>;
  limit?: number;
  offset?: number;
}

interface QueryResponse {
  data: Record<string, unknown>[];
  totalRows: number;
  columns: Array<{ name: string; type: string }>;
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
 */
function parseJsonFile(filePath: string): Record<string, unknown>[] {
  const text = fs.readFileSync(filePath, 'utf-8');
  const parsed = JSON.parse(text);
  if (Array.isArray(parsed)) return parsed;
  if (typeof parsed === 'object' && parsed !== null) {
    const arrayKey = Object.keys(parsed).find(k => Array.isArray(parsed[k]));
    return arrayKey ? parsed[arrayKey] : [parsed];
  }
  return [];
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
  else if (ext === '.json') rows = normalizeRowColumns(parseJsonFile(filePath));
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
      switch (f.operator) {
        case 'equals': return val == f.value;
        case 'notEquals': return val != f.value;
        case 'contains': return String(val).toLowerCase().includes(String(f.value).toLowerCase());
        case 'gt': return Number(val) > Number(f.value);
        case 'lt': return Number(val) < Number(f.value);
        case 'gte': return Number(val) >= Number(f.value);
        case 'lte': return Number(val) <= Number(f.value);
        default: return true;
      }
    });
  });
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
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'ok', version: '0.1.0' }));
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

          // Parse file
          let rows = await loadFileData(filePath);
          const totalRows = rows.length;

          // Apply filters
          rows = applyFilters(rows, query.filters);

          // Select columns
          if (query.columns && query.columns.length > 0) {
            rows = rows.map(row => {
              const filtered: Record<string, unknown> = {};
              for (const col of query.columns!) {
                if (col in row) filtered[col] = row[col];
              }
              return filtered;
            });
          }

          // Pagination
          const offset = query.offset || 0;
          const limit = query.limit || 10000;
          rows = rows.slice(offset, offset + limit);

          const columns = inferColumns(rows);

          const response: QueryResponse = { data: rows, totalRows, columns };
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

          const rows = await loadFileData(filePath);

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

          const rows = await loadFileData(filePath);
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
