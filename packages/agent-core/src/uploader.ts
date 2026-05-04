/**
 * Agent Schema Uploader
 *
 * Parses local files and sends **schema metadata only** to the platform.
 * The full file stays on the user's machine — zero data uploaded.
 *
 * Flow:
 * 1. Parse file locally (CSV, Excel, JSON)
 * 2. Extract column names, types, and ~10 sample values per column
 * 3. Compute schema hash for change detection
 * 4. POST schema metadata to /api/agent/sync (JSON, not multipart)
 * 5. Platform runs AI mapping on the metadata
 * 6. Sample values used in-memory for mapping, then discarded
 */

import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { AgentConfig } from './config';
import { recordSync, getFileState, removeFileState, loadState, saveState } from './state';

export interface UploadResult {
  success: boolean;
  unchanged?: boolean;
  connectionId?: string;
  isNew?: boolean;
  error?: string;
  retries?: number;
}

interface ParsedColumn {
  name: string;
  type: string;
  samples: unknown[];
}

interface TableStructureDecision {
  selectedSheet: string;
  headerRowIdx: number;
  dataStartRowIdx: number;
  dataEndRowIdx: number | null;
  delimiter?: ',' | ';' | '|' | '\t';
  confidence: number;
  reasoning: string;
  needsUserConfirmation: boolean;
}

const SAMPLE_ROWS = 10;

/**
 * Ask the platform's AI #10 endpoint to decide how to parse this tabular
 * file: which sheet (Excel), which header row, which delimiter (CSV-family).
 *
 * Returns null if the platform isn't reachable, the AI fails, or the agent
 * config doesn't have credentials. Callers fall back to heuristic parsing.
 */
async function getStructureDecisionFromAI(
  buffer: Buffer,
  fileType: 'csv' | 'excel' | 'tsv' | 'json',
  fileName: string,
  contentHash: string,
  config: AgentConfig,
): Promise<TableStructureDecision | null> {
  if (!config.platformUrl || !config.apiKey) return null;
  if (fileType === 'json') return null; // JSON has its own structure, not tabular

  type RawSheet = { name: string; rows: unknown[][] };
  let sheets: RawSheet[];
  let rawTextSample: string | undefined;

  if (fileType === 'excel') {
    const XLSX = require('xlsx');
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const sheetNames: string[] = workbook.SheetNames || [];
    sheets = sheetNames.map((name: string) => ({
      name,
      rows: (XLSX.utils.sheet_to_json(workbook.Sheets[name], {
        header: 1,
        defval: null,
        raw: false,
      }) as unknown[][]).slice(0, 30),
    }));
  } else {
    // CSV / TSV / delimited — read first 30 lines and pre-split optimistically
    const text = buffer.toString('utf-8').replace(/^﻿/, '');
    rawTextSample = text.slice(0, 2000);
    const lines = text.split(/\r?\n/).filter(l => l.length > 0).slice(0, 30);
    // Pre-split by comma; AI will detect actual delimiter from rawTextSample
    sheets = [{ name: 'default', rows: lines.map(l => parseCsvLine(l)) }];
  }

  try {
    const url = `${config.platformUrl.replace(/\/$/, '')}/api/agent/detect-table-structure`;
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${config.apiKey}`,
      },
      body: JSON.stringify({ fileType, fileName, contentHash, sheets, rawTextSample }),
    });
    if (!res.ok) {
      console.warn(`[AI #10] Platform returned ${res.status} — falling back to heuristic for ${fileName}`);
      return null;
    }
    const json = await res.json() as { decision: TableStructureDecision; fromCache?: boolean; fallback?: boolean };
    if (json.fallback) {
      console.warn(`[AI #10] Platform returned fallback decision (low confidence) for ${fileName}`);
    }
    console.log(
      `[AI #10] ${fileName}: sheet="${json.decision.selectedSheet}" ` +
      `header=row${json.decision.headerRowIdx} ` +
      (json.decision.delimiter ? `delim="${json.decision.delimiter === '\t' ? '\\t' : json.decision.delimiter}" ` : '') +
      `conf=${json.decision.confidence.toFixed(2)}` +
      (json.fromCache ? ' (cached)' : ''),
    );
    return json.decision;
  } catch (err) {
    console.warn(`[AI #10] Network error for ${fileName} — falling back to heuristic:`, err instanceof Error ? err.message : err);
    return null;
  }
}

/** Coerce a string sample value into number/boolean/string. */
function coerceValue(val: string | unknown): unknown {
  if (val === null || val === undefined) return null;
  if (typeof val !== 'string') return val;
  const trimmed = val.trim();
  if (trimmed === '') return null;
  const num = Number(trimmed);
  if (!isNaN(num) && /^-?[\d.,]+$/.test(trimmed)) return num;
  if (trimmed.toLowerCase() === 'true') return true;
  if (trimmed.toLowerCase() === 'false') return false;
  return trimmed;
}

/** Split a delimited line by an arbitrary single-character delimiter. */
function splitByDelimiter(line: string, delim: string): string[] {
  if (delim === ',') return parseCsvLine(line);
  // Other delimiters typically don't quote — simple split is fine for TSV/pipe/semicolon
  return line.split(delim).map(s => s.trim());
}

/**
 * Normalize a column name to snake_case lowercase.
 * Ensures consistency between source headers and Cube.js field names.
 */
function normalizeColumnName(name: string): string {
  return name
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .replace(/[\s-]+/g, '_')
    .toLowerCase()
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '');
}

/**
 * Parse a CSV file and extract schema + sample values.
 *
 * When `decision` is provided (from AI #10), uses its delimiter, header
 * row index, and data range. Otherwise falls back to the original behavior
 * (row 0 = header, comma delimiter).
 */
function parseCsvSchema(
  buffer: Buffer,
  decision?: TableStructureDecision | null,
): { columns: ParsedColumn[]; rowCount: number } {
  // Strip UTF-8 BOM if present (common in Excel-exported CSVs)
  const text = buffer.toString('utf-8').replace(/^﻿/, '');
  const lines = text.split(/\r?\n/).filter(l => l.length > 0);

  if (lines.length === 0) return { columns: [], rowCount: 0 };

  const delimiter = decision?.delimiter || ',';
  const headerRowIdx = decision?.headerRowIdx ?? 0;
  const dataStartRowIdx = decision?.dataStartRowIdx ?? (headerRowIdx + 1);
  const dataEndRowIdx = decision?.dataEndRowIdx ?? null;

  if (headerRowIdx >= lines.length) return { columns: [], rowCount: 0 };

  const headers = splitByDelimiter(lines[headerRowIdx], delimiter);

  const dataLines = lines.slice(
    dataStartRowIdx,
    dataEndRowIdx === null ? lines.length : Math.min(lines.length, dataEndRowIdx + 1),
  );

  const sampleRows: string[][] = dataLines
    .slice(0, SAMPLE_ROWS)
    .map(l => splitByDelimiter(l, delimiter));

  // Dedup repeated header names so duplicates don't collide silently
  const seen = new Map<string, number>();
  const finalNames = headers.map((rawName, idx) => {
    const base = normalizeColumnName(rawName) || `column_${idx + 1}`;
    const count = seen.get(base) || 0;
    seen.set(base, count + 1);
    return count === 0 ? base : `${base}_${count}`;
  });

  const columns: ParsedColumn[] = finalNames.map((name, idx) => {
    const samples = sampleRows.map(row => coerceValue(row[idx] ?? ''));
    const type = inferType(samples);
    return { name, type, samples };
  });

  return { columns, rowCount: dataLines.length };
}

/**
 * Parse a single CSV line handling quoted fields.
 */
function parseCsvLine(line: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
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
 * Parse a JSON file and extract schema + sample values.
 */
function parseJsonSchema(buffer: Buffer): { columns: ParsedColumn[]; rowCount: number } {
  const text = buffer.toString('utf-8');
  const parsed = JSON.parse(text);

  let rows: Record<string, unknown>[];
  if (Array.isArray(parsed)) {
    rows = parsed;
  } else if (typeof parsed === 'object' && parsed !== null) {
    // Try to find an array in the first level
    const arrayKey = Object.keys(parsed).find(k => Array.isArray(parsed[k]));
    rows = arrayKey ? parsed[arrayKey] : [parsed];
  } else {
    return { columns: [], rowCount: 0 };
  }

  if (rows.length === 0) return { columns: [], rowCount: 0 };

  // Collect all column names
  const colNames = new Set<string>();
  rows.forEach(row => Object.keys(row).forEach(k => colNames.add(k)));

  const columns: ParsedColumn[] = Array.from(colNames).map(rawName => {
    const name = normalizeColumnName(rawName);
    const samples = rows.slice(0, SAMPLE_ROWS).map(row => row[rawName] ?? null);
    const type = inferType(samples);
    return { name, type, samples };
  });

  return { columns, rowCount: rows.length };
}

/**
 * Find the header row in a 2D array of cell values.
 * Walks down rows looking for a row that "looks like a real table header":
 *   - At least 4 non-empty cells (skips title rows + button-label rows like
 *     "Add Inventory" / "Edit Inventory" that only fill 1-2 cells)
 *   - At least 25% of the row's width is non-empty (skips sparse layout rows)
 *   - Cells are short labels, not long instructional text (>200 chars = paragraph,
 *     skip it — that's a description not a column name)
 *   - Has data BELOW it (at least one row with non-empty cells)
 *
 * Returns -1 if no header-like row is found.
 */
function findHeaderRow(rows: Array<Array<unknown>>): number {
  if (rows.length < 2) return -1;
  const maxWidth = Math.max(...rows.map(r => (r || []).length), 1);

  for (let i = 0; i < rows.length - 1; i++) {
    const row = rows[i] || [];
    const nonEmpty = row.filter(c => c !== null && c !== undefined && String(c).trim() !== '');
    if (nonEmpty.length < 4) continue;
    if (nonEmpty.length / maxWidth < 0.25) continue;

    // Skip rows containing long-text cells (likely instructional paragraphs)
    const hasLongText = nonEmpty.some(c => String(c).length > 200);
    if (hasLongText) continue;

    // Confirm there's data BELOW this row
    const nextRow = rows[i + 1] || [];
    const nextNonEmpty = nextRow.filter(c => c !== null && c !== undefined && String(c).trim() !== '');
    if (nextNonEmpty.length === 0) continue;

    return i;
  }
  return -1;
}

/**
 * Score how "tabular" a sheet is — used to pick the best sheet in a multi-tab
 * Excel file when the first sheet is something other than the data table
 * (e.g. instructions, cover page, summary).
 *
 * Higher score = more likely to be the real data sheet.
 */
function scoreSheetForDataLikelihood(rows: Array<Array<unknown>>): number {
  const headerIdx = findHeaderRow(rows);
  if (headerIdx < 0) return 0;
  const header = rows[headerIdx] || [];
  const headerCells = header.filter(c => c !== null && c !== undefined && String(c).trim() !== '').length;
  const dataRows = rows.slice(headerIdx + 1)
    .filter(row => Array.isArray(row) && row.some(c => c !== null && c !== undefined && String(c).trim() !== ''))
    .length;
  // Prefer sheets with many header cells AND many data rows
  return headerCells * Math.min(dataRows, 100); // cap data-row contribution so a 50k-row mostly-empty file doesn't dominate
}

/** Sanitize a header cell into a column key. Empty → "column_<idx>". */
function cleanHeaderName(cell: unknown, idx: number): string {
  const raw = cell == null ? '' : String(cell).trim();
  if (!raw) return `column_${idx + 1}`;
  return normalizeColumnName(raw) || `column_${idx + 1}`;
}

/**
 * Parse an Excel file and extract schema + sample values.
 *
 * Primary path: when `decision` is provided (from AI #10 on the platform),
 * use it to pick the sheet, header row, and data range. This handles any
 * file structure the AI can reason about — instructions tabs, banners,
 * footers, multi-tab workbooks, etc.
 *
 * Fallback path: when no decision is provided (offline, AI failed, or
 * config missing), fall back to a heuristic that walks all sheets and
 * picks the one with the cleanest tabular structure. Less robust but
 * keeps the agent functional when disconnected.
 */
function parseExcelSchema(
  buffer: Buffer,
  decision?: TableStructureDecision | null,
): { columns: ParsedColumn[]; rowCount: number; sheets: string[]; selectedSheet?: string } {
  const XLSX = require('xlsx');
  const workbook = XLSX.read(buffer, { type: 'buffer' });
  const sheets: string[] = workbook.SheetNames || [];

  if (sheets.length === 0) return { columns: [], rowCount: 0, sheets: [] };

  // PRIMARY: AI-driven decision
  if (decision && sheets.includes(decision.selectedSheet)) {
    const ws = workbook.Sheets[decision.selectedSheet];
    const rows: unknown[][] = XLSX.utils.sheet_to_json(ws, {
      header: 1,
      defval: null,
      raw: false,
    });

    const headerCells = rows[decision.headerRowIdx] || [];
    const seen = new Map<string, number>();
    const columnNames = headerCells.map((c, i) => {
      const base = cleanHeaderName(c, i);
      const count = seen.get(base) || 0;
      seen.set(base, count + 1);
      return count === 0 ? base : `${base}_${count}`;
    });

    const dataEndExclusive = decision.dataEndRowIdx === null
      ? rows.length
      : Math.min(rows.length, decision.dataEndRowIdx + 1);
    const dataRows = rows.slice(decision.dataStartRowIdx, dataEndExclusive)
      .filter(row => Array.isArray(row) && row.some(c => c !== null && c !== undefined && String(c).trim() !== ''));

    const columns: ParsedColumn[] = columnNames.map((name, colIdx) => {
      const samples = dataRows.slice(0, SAMPLE_ROWS).map(row => row[colIdx] ?? null);
      return { name, type: inferType(samples), samples };
    });

    return { columns, rowCount: dataRows.length, sheets, selectedSheet: decision.selectedSheet };
  }

  // FALLBACK: no AI decision — score sheets heuristically
  type SheetCandidate = { name: string; rows: unknown[][]; score: number };
  const candidates: SheetCandidate[] = sheets.map(name => {
    const ws = workbook.Sheets[name];
    const rows: unknown[][] = XLSX.utils.sheet_to_json(ws, {
      header: 1,
      defval: null,
      raw: false,
    });
    return { name, rows, score: scoreSheetForDataLikelihood(rows) };
  });
  candidates.sort((a, b) => b.score - a.score);
  const best = candidates[0];

  if (!best || best.score === 0) {
    // No sheet has a clear tabular header — last-resort fall back to first sheet, row 0
    const fallbackSheet = sheets[0];
    const rows: unknown[][] = XLSX.utils.sheet_to_json(workbook.Sheets[fallbackSheet], {
      header: 1, defval: null, raw: false,
    });
    if (rows.length === 0) return { columns: [], rowCount: 0, sheets, selectedSheet: fallbackSheet };
    const headerCells = rows[0] || [];
    const seen = new Map<string, number>();
    const columnNames = headerCells.map((c, i) => {
      const base = cleanHeaderName(c, i);
      const count = seen.get(base) || 0;
      seen.set(base, count + 1);
      return count === 0 ? base : `${base}_${count}`;
    });
    const dataRows = rows.slice(1).filter(r => Array.isArray(r) && r.some(c => c !== null && c !== undefined && String(c).trim() !== ''));
    const columns: ParsedColumn[] = columnNames.map((name, colIdx) => {
      const samples = dataRows.slice(0, SAMPLE_ROWS).map(row => row[colIdx] ?? null);
      return { name, type: inferType(samples), samples };
    });
    console.log(`[parseExcelSchema/fallback] No clear tabular sheet; using first sheet "${fallbackSheet}" row 0`);
    return { columns, rowCount: dataRows.length, sheets, selectedSheet: fallbackSheet };
  }

  const headerRowIdx = findHeaderRow(best.rows);
  const effectiveHeaderIdx = headerRowIdx >= 0 ? headerRowIdx : 0;
  const headerCells = best.rows[effectiveHeaderIdx] || [];

  const seen = new Map<string, number>();
  const columnNames = headerCells.map((c, i) => {
    const base = cleanHeaderName(c, i);
    const count = seen.get(base) || 0;
    seen.set(base, count + 1);
    return count === 0 ? base : `${base}_${count}`;
  });

  const dataRows = best.rows.slice(effectiveHeaderIdx + 1)
    .filter(row => Array.isArray(row) && row.some(c => c !== null && c !== undefined && String(c).trim() !== ''));

  console.log(`[parseExcelSchema/fallback] AI unavailable; heuristic picked "${best.name}", header row ${effectiveHeaderIdx}; ${columnNames.length} columns, ${dataRows.length} data rows`);

  const columns: ParsedColumn[] = columnNames.map((name, colIdx) => {
    const samples = dataRows.slice(0, SAMPLE_ROWS).map(row => row[colIdx] ?? null);
    return { name, type: inferType(samples), samples };
  });

  return { columns, rowCount: dataRows.length, sheets, selectedSheet: best.name };
}

/**
 * Infer column type from sample values.
 */
function inferType(samples: unknown[]): string {
  const nonNull = samples.filter(s => s !== null && s !== undefined && s !== '');
  if (nonNull.length === 0) return 'string';

  const allNumbers = nonNull.every(s => typeof s === 'number');
  if (allNumbers) {
    return nonNull.every(s => Number.isInteger(s)) ? 'integer' : 'float';
  }

  const allBooleans = nonNull.every(s => typeof s === 'boolean');
  if (allBooleans) return 'boolean';

  // Check for date patterns
  const datePattern = /^\d{4}-\d{2}-\d{2}|^\d{1,2}\/\d{1,2}\/\d{2,4}/;
  if (nonNull.every(s => typeof s === 'string' && datePattern.test(s))) return 'datetime';

  return 'string';
}

/**
 * Detect file type from extension.
 */
function detectFileType(fileName: string): string {
  const ext = fileName.toLowerCase().split('.').pop();
  if (ext === 'csv' || ext === 'tsv') return 'csv';
  if (ext === 'json') return 'json';
  if (ext === 'xlsx' || ext === 'xls') return 'excel';
  return 'unknown';
}

/**
 * Upload file schema (metadata only) to the platform.
 * The full file stays on the user's machine.
 */
export async function uploadFile(
  filePath: string,
  config: AgentConfig
): Promise<UploadResult> {
  const resolved = path.resolve(filePath);

  // Check file exists
  if (!fs.existsSync(resolved)) {
    return { success: false, error: `File not found: ${resolved}` };
  }

  const stats = fs.statSync(resolved);
  const fileName = path.basename(resolved);
  const fileType = detectFileType(fileName);

  if (fileType === 'unknown') {
    return { success: false, error: `Unsupported file type: ${fileName}` };
  }

  // Read and parse the file locally
  const buffer = fs.readFileSync(resolved);

  // Compute a content hash from raw file bytes — used as the AI #10 cache key
  // so re-uploads of the same file content reuse the cached structure decision.
  const fileBytesHash = crypto.createHash('sha256').update(buffer).digest('hex');

  // Ask AI #10 (on the platform) how to parse this file: which sheet, which
  // header row, which delimiter. Returns null if offline or AI fails — parsers
  // fall back to heuristics.
  let structureDecision: TableStructureDecision | null = null;
  if (fileType === 'csv' || fileType === 'excel') {
    structureDecision = await getStructureDecisionFromAI(
      buffer,
      fileType,
      fileName,
      fileBytesHash,
      config,
    );
  }

  let columns: ParsedColumn[];
  let rowCount: number;

  try {
    if (fileType === 'csv') {
      const result = parseCsvSchema(buffer, structureDecision);
      columns = result.columns;
      rowCount = result.rowCount;
    } else if (fileType === 'json') {
      const result = parseJsonSchema(buffer);
      columns = result.columns;
      rowCount = result.rowCount;
    } else if (fileType === 'excel') {
      const result = parseExcelSchema(buffer, structureDecision);
      columns = result.columns;
      rowCount = result.rowCount;
    } else {
      return { success: false, error: `Unsupported file type: ${fileType}` };
    }
  } catch (err) {
    return { success: false, error: `Failed to parse file: ${err instanceof Error ? err.message : err}` };
  }

  if (columns.length === 0) {
    return { success: false, error: 'No columns found in file' };
  }

  // Compute two hashes for change detection:
  // 1. Schema hash (columns + types) — triggers AI re-mapping when columns change
  // 2. Content hash (schema + row count + file size) — triggers metadata update for new rows
  const schemaFingerprint = columns.map(c => `${c.name}:${c.type}`).join('|');
  const schemaHash = crypto.createHash('sha256').update(schemaFingerprint).digest('hex');
  const contentFingerprint = `${schemaFingerprint}|rows:${rowCount}|size:${stats.size}`;
  const contentHash = crypto.createHash('sha256').update(contentFingerprint).digest('hex');

  // Check local state
  const existing = getFileState(resolved);
  const schemaChanged = !existing || existing.hash !== schemaHash;
  const contentChanged = !existing || (existing as any).contentHash !== contentHash;

  if (!schemaChanged && !contentChanged) {
    // Verify the server connection still exists before skipping.
    // If the user deleted the connection from univintel.com, we need to re-sync.
    if (existing?.connectionId && config.platformUrl && config.apiKey) {
      try {
        const checkRes = await fetch(`${config.platformUrl}/api/agent/health`, {
          headers: { 'Authorization': `Bearer ${config.apiKey}` },
          signal: AbortSignal.timeout(5000),
        });
        if (checkRes.ok) {
          // Server is reachable — check if this specific connection exists
          const syncRes = await fetch(`${config.platformUrl}/api/agent/sync`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${config.apiKey}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ fileName, fileType, schemaHash, contentHash, agentFilePath: resolved, columns, rowCount, fileSize: stats.size }),
            signal: AbortSignal.timeout(10000),
          });
          if (syncRes.ok) {
            const syncData = await syncRes.json();
            if (syncData.unchanged) {
              return { success: true, unchanged: true, connectionId: existing.connectionId };
            }
            // Connection was recreated or schema changed on server — update local state
            recordSync(resolved, schemaHash, syncData.connectionId || null, stats.size);
            const state = loadState();
            if (state.files[resolved]) {
              (state.files[resolved] as any).contentHash = contentHash;
              saveState(state);
            }
            return { success: true, unchanged: false, connectionId: syncData.connectionId, isNew: syncData.isNew };
          }
        }
      } catch {
        // Server unreachable — skip silently, try again next sync
      }
    }
    return { success: true, unchanged: true, connectionId: existing?.connectionId || undefined };
  }

  // Send schema metadata (NOT the file) to the platform
  let lastError = '';
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      const response = await fetch(`${config.platformUrl}/api/agent/sync`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${config.apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          fileName,
          fileType,
          schemaHash,
          contentHash,
          schemaChanged,        // true = new columns, needs AI re-mapping
          agentFilePath: resolved,
          columns,
          rowCount,
          fileSize: stats.size,
        }),
      });

      if (response.ok) {
        const result = await response.json();

        // Record successful sync with both hashes
        recordSync(resolved, schemaHash, result.connectionId || null, stats.size);
        // Store content hash for row-change detection (extend state)
        const state = loadState();
        if (state.files[resolved]) {
          (state.files[resolved] as any).contentHash = contentHash;
          saveState(state);
        }

        return {
          success: true,
          unchanged: result.unchanged || false,
          connectionId: result.connectionId,
          isNew: result.isNew,
          retries: attempt,
        };
      }

      if (response.status >= 400 && response.status < 500) {
        const errorBody = await response.json().catch(() => ({ error: response.statusText }));
        return { success: false, error: errorBody.error || `HTTP ${response.status}` };
      }

      lastError = `HTTP ${response.status}: ${response.statusText}`;
    } catch (err) {
      lastError = err instanceof Error ? err.message : 'Unknown network error';
    }

    if (attempt < config.maxRetries) {
      const delay = config.retryBaseDelay * Math.pow(2, attempt);
      console.log(`  Retry ${attempt + 1}/${config.maxRetries} in ${delay}ms...`);
      await sleep(delay);
    }
  }

  return {
    success: false,
    error: `Failed after ${config.maxRetries + 1} attempts: ${lastError}`,
    retries: config.maxRetries,
  };
}

/**
 * Upload all supported files from a directory (one-time sync).
 */
/**
 * Notify the platform that a local file was deleted.
 * Deactivates the connection on the server side.
 */
export async function notifyFileDeletion(
  filePath: string,
  config: AgentConfig
): Promise<{ success: boolean; error?: string }> {
  const resolved = path.resolve(filePath);
  const existing = getFileState(resolved);
  if (!existing?.connectionId) {
    return { success: true }; // No connection to deactivate
  }

  try {
    const response = await fetch(`${config.platformUrl}/api/agent/sync`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${config.apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        fileName: path.basename(resolved),
        agentFilePath: resolved,
        connectionId: existing.connectionId,
        deleted: true,
      }),
    });

    if (response.ok) {
      removeFileState(resolved);
      return { success: true };
    }

    return { success: false, error: `HTTP ${response.status}` };
  } catch (err) {
    return { success: false, error: err instanceof Error ? err.message : 'Network error' };
  }
}

export async function syncDirectory(
  dirPath: string,
  config: AgentConfig,
  options?: { extensions?: string[]; recursive?: boolean }
): Promise<{ synced: number; unchanged: number; failed: number; errors: string[] }> {
  const extensions = options?.extensions || ['.csv', '.xlsx', '.xls', '.json'];
  const recursive = options?.recursive ?? true;

  const files = collectFiles(dirPath, extensions, recursive);

  let synced = 0;
  let unchanged = 0;
  let failed = 0;
  const errors: string[] = [];

  for (const file of files) {
    console.log(`Syncing: ${path.relative(dirPath, file)}`);
    const result = await uploadFile(file, config);

    if (result.success) {
      if (result.unchanged) {
        unchanged++;
      } else {
        synced++;
        console.log(`  ${result.isNew ? 'Created' : 'Updated'}: ${result.connectionId}`);
      }
    } else {
      failed++;
      errors.push(`${path.basename(file)}: ${result.error}`);
      console.error(`  Failed: ${result.error}`);
    }
  }

  return { synced, unchanged, failed, errors };
}

function collectFiles(dirPath: string, extensions: string[], recursive: boolean): string[] {
  const results: string[] = [];
  const resolved = path.resolve(dirPath);

  if (!fs.existsSync(resolved)) return results;

  const entries = fs.readdirSync(resolved, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(resolved, entry.name);
    if (entry.isDirectory() && recursive) {
      results.push(...collectFiles(fullPath, extensions, true));
    } else if (entry.isFile()) {
      const ext = path.extname(entry.name).toLowerCase();
      if (extensions.includes(ext) && !entry.name.startsWith('.') && !entry.name.startsWith('~')) {
        results.push(fullPath);
      }
    }
  }

  return results;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
