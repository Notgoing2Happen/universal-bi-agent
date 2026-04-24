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
 * Parse a CSV file into rows.
 */
function parseCsvFile(filePath: string): Record<string, unknown>[] {
  const text = fs.readFileSync(filePath, 'utf-8');
  const lines = text.split(/\r?\n/).filter(l => l.trim());
  if (lines.length < 2) return [];

  const headers = parseCsvLine(lines[0]);
  const rows: Record<string, unknown>[] = [];

  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i]);
    const row: Record<string, unknown> = {};
    for (let j = 0; j < headers.length; j++) {
      const val = values[j] ?? '';
      const num = Number(val);
      if (val !== '' && !isNaN(num)) {
        row[headers[j]] = num;
      } else if (val.toLowerCase() === 'true') {
        row[headers[j]] = true;
      } else if (val.toLowerCase() === 'false') {
        row[headers[j]] = false;
      } else {
        row[headers[j]] = val;
      }
    }
    rows.push(row);
  }

  return rows;
}

function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') { current += '"'; i++; }
      else { inQuotes = !inQuotes; }
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  result.push(current.trim());
  return result;
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
 */
function parseExcelFile(filePath: string): Record<string, unknown>[] {
  const XLSX = require('xlsx');
  const buffer = fs.readFileSync(filePath);
  const workbook = XLSX.read(buffer, { type: 'buffer' });
  if (workbook.SheetNames.length === 0) return [];
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
}

/**
 * Load and parse a file based on extension.
 */
function loadFileData(filePath: string): Record<string, unknown>[] {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.csv' || ext === '.tsv') return parseCsvFile(filePath);
  if (ext === '.json') return parseJsonFile(filePath);
  if (ext === '.xlsx' || ext === '.xls') return parseExcelFile(filePath);
  throw new Error(`Unsupported file type: ${ext}`);
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
          let rows = loadFileData(filePath);
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

        if (req.url?.startsWith('/schema/') && req.method === 'GET') {
          const connectionId = req.url.split('/schema/')[1];
          const filePath = resolveFilePath(connectionId);

          if (!filePath || !fs.existsSync(filePath)) {
            res.writeHead(404, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'File not found for connection' }));
            return;
          }

          const rows = loadFileData(filePath);
          const columns = inferColumns(rows);

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ columns, rowCount: rows.length, filePath }));
          return;
        }

        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
      } catch (err) {
        console.error('[QueryServer] Error:', err);
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

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => resolve(body));
    req.on('error', reject);
  });
}
