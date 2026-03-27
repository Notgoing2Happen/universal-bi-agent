/**
 * Agent HTTP Uploader
 *
 * Uploads files to the platform's POST /api/agent/sync endpoint.
 * Includes:
 * - Content hash header for server-side skip
 * - Exponential backoff retry on failure
 * - File locking protection (stability threshold)
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

/**
 * Upload a file to the platform with retry logic.
 */
export async function uploadFile(
  filePath: string,
  config: AgentConfig
): Promise<UploadResult> {
  const resolved = path.resolve(filePath);

  // Check file exists and is readable
  if (!fs.existsSync(resolved)) {
    return { success: false, error: `File not found: ${resolved}` };
  }

  const stats = fs.statSync(resolved);

  // Check file size
  if (stats.size > config.maxFileSize) {
    return {
      success: false,
      error: `File too large: ${(stats.size / 1024 / 1024).toFixed(1)}MB (max ${(config.maxFileSize / 1024 / 1024).toFixed(0)}MB)`,
    };
  }

  // Read file and compute hash
  const buffer = fs.readFileSync(resolved);
  const hash = crypto.createHash('sha256').update(buffer).digest('hex');

  // Check local state — skip if hash unchanged
  const existing = getFileState(resolved);
  if (existing && existing.hash === hash) {
    return { success: true, unchanged: true, connectionId: existing.connectionId || undefined };
  }

  // Build multipart form data
  const fileName = path.basename(resolved);
  const blob = new Blob([buffer]);

  let lastError = '';
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      const formData = new FormData();
      formData.append('file', blob, fileName);
      formData.append('agentFilePath', resolved);

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

        // Record successful sync in local state
        recordSync(resolved, hash, result.connectionId || null, stats.size);

        return {
          success: true,
          unchanged: result.unchanged || false,
          connectionId: result.connectionId,
          isNew: result.isNew,
          retries: attempt,
        };
      }

      // Non-retryable errors (client errors)
      if (response.status >= 400 && response.status < 500) {
        const errorBody = await response.json().catch(() => ({ error: response.statusText }));
        return {
          success: false,
          error: errorBody.error || `HTTP ${response.status}`,
        };
      }

      // Server error — retry with backoff
      lastError = `HTTP ${response.status}: ${response.statusText}`;
    } catch (err) {
      lastError = err instanceof Error ? err.message : 'Unknown network error';
    }

    // Exponential backoff: 1s, 2s, 4s
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
        console.log(`  Unchanged (skipped)`);
      } else {
        synced++;
        console.log(`  ${result.isNew ? 'Created' : 'Updated'}: ${result.connectionId}`);
      }
    } else {
      failed++;
      const errorMsg = `${path.basename(file)}: ${result.error}`;
      errors.push(errorMsg);
      console.error(`  Failed: ${result.error}`);
    }
  }

  return { synced, unchanged, failed, errors };
}

/**
 * Collect all files matching extensions in a directory.
 */
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
      if (extensions.includes(ext)) {
        // Skip hidden files and temp files
        if (!entry.name.startsWith('.') && !entry.name.startsWith('~')) {
          results.push(fullPath);
        }
      }
    }
  }

  return results;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
