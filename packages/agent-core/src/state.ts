/**
 * Agent Local State Tracker
 *
 * Tracks file hashes and connection IDs locally so the agent knows
 * which files have already been synced and can skip unchanged files
 * without contacting the server.
 *
 * State is stored at ~/.universal-bi/state.json
 */

import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { getConfigDir } from './config';

const STATE_FILE = path.join(getConfigDir(), 'state.json');

export interface FileState {
  /** SHA-256 hash of last synced file content */
  hash: string;
  /** Connection ID on the platform (null if not yet created) */
  connectionId: string | null;
  /** Last successful sync timestamp */
  lastSyncedAt: string;
  /** File size at last sync */
  size: number;
}

export interface AgentState {
  files: Record<string, FileState>; // key = absolute file path
  lastHeartbeat?: string;
}

/**
 * Load the agent state. Returns empty state if file doesn't exist.
 */
export function loadState(): AgentState {
  if (!fs.existsSync(STATE_FILE)) {
    return { files: {} };
  }

  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    return JSON.parse(raw) as AgentState;
  } catch {
    return { files: {} };
  }
}

/**
 * Save the agent state.
 */
export function saveState(state: AgentState): void {
  const dir = getConfigDir();
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), 'utf-8');
}

/**
 * Compute SHA-256 hash of a file (streaming for large files).
 */
export function computeFileHash(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);
    stream.on('data', chunk => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

/**
 * Check if a file has changed since last sync.
 * Returns true if the file needs syncing.
 */
export async function hasFileChanged(filePath: string): Promise<boolean> {
  const state = loadState();
  const resolved = path.resolve(filePath);
  const existing = state.files[resolved];

  if (!existing) return true; // Never synced

  try {
    const currentHash = await computeFileHash(resolved);
    return currentHash !== existing.hash;
  } catch {
    return true; // Can't read file — let uploader handle the error
  }
}

/**
 * Record a successful file sync.
 */
export function recordSync(
  filePath: string,
  hash: string,
  connectionId: string | null,
  size: number
): void {
  const state = loadState();
  const resolved = path.resolve(filePath);

  state.files[resolved] = {
    hash,
    connectionId,
    lastSyncedAt: new Date().toISOString(),
    size,
  };

  saveState(state);
}

/**
 * Remove a file from state (when file is deleted).
 */
export function removeFileState(filePath: string): void {
  const state = loadState();
  const resolved = path.resolve(filePath);
  delete state.files[resolved];
  saveState(state);
}

/**
 * Get sync state for a specific file.
 */
export function getFileState(filePath: string): FileState | null {
  const state = loadState();
  return state.files[path.resolve(filePath)] || null;
}

/**
 * Get summary of all tracked files.
 */
export function getStateSummary(): {
  totalFiles: number;
  totalSize: number;
  oldestSync: string | null;
  newestSync: string | null;
} {
  const state = loadState();
  const files = Object.values(state.files);

  if (files.length === 0) {
    return { totalFiles: 0, totalSize: 0, oldestSync: null, newestSync: null };
  }

  const sorted = files.sort(
    (a, b) => new Date(a.lastSyncedAt).getTime() - new Date(b.lastSyncedAt).getTime()
  );

  return {
    totalFiles: files.length,
    totalSize: files.reduce((sum, f) => sum + f.size, 0),
    oldestSync: sorted[0].lastSyncedAt,
    newestSync: sorted[sorted.length - 1].lastSyncedAt,
  };
}
