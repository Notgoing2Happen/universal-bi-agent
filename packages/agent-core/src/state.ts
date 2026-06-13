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
  files: Record<string, FileState>; // key = canonicalPathKey(absolute file path)
  lastHeartbeat?: string;
}

/**
 * Canonical key for a file path used to index agent state.
 *
 * A single physical file reaches the agent through several code paths —
 * drag-drop import (path.resolve of the OS drop path), the chokidar watcher
 * (chokidar's emitted path), and directory scans (syncDirectory). On a
 * case-insensitive filesystem (Windows) these can differ only by drive-letter
 * case (`C:\` vs `c:\`) or separators, which would otherwise create TWO
 * `state.files` entries — and TWO rows in the agent UI — for the same file
 * (the duplicates that appear during a sync and "resolve" once everything
 * converges on one representation).
 *
 * Keying every read/write by this canonical form collapses all representations
 * to one entry STRUCTURALLY — no enumeration of which writer produced which
 * casing. `path.resolve()` makes it absolute and normalizes separators / `..`;
 * on win32 we case-fold because the filesystem is case-insensitive. POSIX paths
 * are case-sensitive, so they're left untouched.
 */
export function canonicalPathKey(filePath: string): string {
  const resolved = path.resolve(filePath);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

/** Prefer the entry that is "more synced": one with a connectionId wins, then
 * the more recently synced. Used when collapsing duplicate path representations. */
function preferFileState(a: FileState, b: FileState): FileState {
  const aHas = a.connectionId ? 1 : 0;
  const bHas = b.connectionId ? 1 : 0;
  if (aHas !== bHas) return aHas > bHas ? a : b;
  const at = a.lastSyncedAt ? new Date(a.lastSyncedAt).getTime() : 0;
  const bt = b.lastSyncedAt ? new Date(b.lastSyncedAt).getTime() : 0;
  return bt >= at ? b : a;
}

/**
 * Collapse any file entries that differ only by path representation (drive-letter
 * case, separators) into a single canonical-keyed entry. Pure + idempotent:
 * re-keying an already-canonical map is a no-op. Exposed for unit testing.
 */
export function dedupeFiles(
  files: Record<string, FileState>
): Record<string, FileState> {
  const out: Record<string, FileState> = {};
  for (const [rawKey, info] of Object.entries(files || {})) {
    const key = canonicalPathKey(rawKey);
    out[key] = out[key] ? preferFileState(out[key], info) : info;
  }
  return out;
}

/**
 * Load the agent state. Returns empty state if file doesn't exist.
 *
 * Normalizes on read: any legacy duplicate keys left by an older agent (before
 * canonical keying) are collapsed here, so callers — and the UI's file list —
 * see exactly one entry per physical file even before the next save rewrites
 * the canonicalized map to disk.
 */
export function loadState(): AgentState {
  if (!fs.existsSync(STATE_FILE)) {
    return { files: {} };
  }

  try {
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    const parsed = JSON.parse(raw) as AgentState;
    return { ...parsed, files: dedupeFiles(parsed.files || {}) };
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
  const real = path.resolve(filePath);          // real path for file I/O
  const existing = state.files[canonicalPathKey(filePath)]; // canonical key for lookup

  if (!existing) return true; // Never synced

  try {
    const currentHash = await computeFileHash(real);
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

  state.files[canonicalPathKey(filePath)] = {
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
  delete state.files[canonicalPathKey(filePath)];
  saveState(state);
}

/**
 * Get sync state for a specific file.
 */
export function getFileState(filePath: string): FileState | null {
  const state = loadState();
  return state.files[canonicalPathKey(filePath)] || null;
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
