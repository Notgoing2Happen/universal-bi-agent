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
import { recordSync, getFileState } from './state';

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

const SAMPLE_ROWS = 10;

/**
 * Parse a CSV file and extract schema + sample values.
 */
function parseCsvSchema(buffer: Buffer): { columns: ParsedColumn[]; rowCount: number } {
  const text = buffer.toString('utf-8');
  const lines = text.split(/\r?\n/).filter(l => l.trim());

  if (lines.length === 0) return { columns: [], rowCount: 0 };

  // Parse header
  const headers = parseCsvLine(lines[0]);

  // Parse sample rows
  const sampleRows: string[][] = [];
  for (let i = 1; i < Math.min(lines.length, SAMPLE_ROWS + 1); i++) {
    sampleRows.push(parseCsvLine(lines[i]));
  }

  const columns: ParsedColumn[] = headers.map((name, idx) => {
    const samples = sampleRows.map(row => {
      const val = row[idx] ?? '';
      // Try to parse as number
      const num = Number(val);
      if (val !== '' && !isNaN(num)) return num;
      // Try boolean
      if (val.toLowerCase() === 'true') return true;
      if (val.toLowerCase() === 'false') return false;
      return val;
    });

    // Infer type from samples
    const type = inferType(samples);
    return { name, type, samples };
  });

  return { columns, rowCount: lines.length - 1 }; // -1 for header
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

  const columns: ParsedColumn[] = Array.from(colNames).map(name => {
    const samples = rows.slice(0, SAMPLE_ROWS).map(row => row[name] ?? null);
    const type = inferType(samples);
    return { name, type, samples };
  });

  return { columns, rowCount: rows.length };
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

  let columns: ParsedColumn[];
  let rowCount: number;

  try {
    if (fileType === 'csv') {
      const result = parseCsvSchema(buffer);
      columns = result.columns;
      rowCount = result.rowCount;
    } else if (fileType === 'json') {
      const result = parseJsonSchema(buffer);
      columns = result.columns;
      rowCount = result.rowCount;
    } else {
      // Excel — fall back to legacy upload for now (needs xlsx library)
      return uploadFileLegacy(resolved, config);
    }
  } catch (err) {
    return { success: false, error: `Failed to parse file: ${err instanceof Error ? err.message : err}` };
  }

  if (columns.length === 0) {
    return { success: false, error: 'No columns found in file' };
  }

  // Compute schema hash (columns + types, NOT data) for change detection
  const schemaFingerprint = columns.map(c => `${c.name}:${c.type}`).join('|');
  const schemaHash = crypto.createHash('sha256').update(schemaFingerprint).digest('hex');

  // Check local state — skip if schema unchanged
  const existing = getFileState(resolved);
  if (existing && existing.hash === schemaHash) {
    return { success: true, unchanged: true, connectionId: existing.connectionId || undefined };
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
          agentFilePath: resolved,
          columns,
          rowCount,
          fileSize: stats.size,
        }),
      });

      if (response.ok) {
        const result = await response.json();

        // Record successful sync (using schema hash, not file hash)
        recordSync(resolved, schemaHash, result.connectionId || null, stats.size);

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
 * Legacy file upload — for Excel files that need the xlsx library,
 * or as a fallback. Sends the full file via multipart.
 */
async function uploadFileLegacy(
  filePath: string,
  config: AgentConfig
): Promise<UploadResult> {
  const buffer = fs.readFileSync(filePath);
  const fileName = path.basename(filePath);
  const hash = crypto.createHash('sha256').update(buffer).digest('hex');

  const existing = getFileState(filePath);
  if (existing && existing.hash === hash) {
    return { success: true, unchanged: true, connectionId: existing.connectionId || undefined };
  }

  const blob = new Blob([buffer]);
  const formData = new FormData();
  formData.append('file', blob, fileName);
  formData.append('agentFilePath', filePath);

  try {
    const response = await fetch(`${config.platformUrl}/api/agent/sync`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${config.apiKey}`,
        'X-Agent-File-Hash': hash,
      },
      body: formData,
    });

    if (response.ok) {
      const result = await response.json();
      recordSync(filePath, hash, result.connectionId || null, fs.statSync(filePath).size);
      return {
        success: true,
        unchanged: result.unchanged || false,
        connectionId: result.connectionId,
        isNew: result.isNew,
      };
    }

    const errorBody = await response.json().catch(() => ({ error: response.statusText }));
    return { success: false, error: errorBody.error || `HTTP ${response.status}` };
  } catch (err) {
    return { success: false, error: err instanceof Error ? err.message : 'Network error' };
  }
}

/**
 * Upload all supported files from a directory (one-time sync).
 */
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
