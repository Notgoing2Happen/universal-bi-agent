/**
 * Parsed-file cache — Phase 1 of the DuckDB local-passthrough plan
 * (docs/duckdb-local-passthrough-plan.md in the platform repo).
 *
 * WHY: the /query handler re-parses the ENTIRE file on every request, and the
 * platform pages a large aggregation by re-POSTing with successive offsets — so a
 * 220K-row file was re-parsed ~5× per query, and an N-cube fan-out parsed the same
 * file N times. This caches the PARSED ROW ARRAY so one parse serves all pages,
 * all fan-out cubes, and repeat questions. Pure parse-skip: it feeds the SAME rows
 * to the SAME aggregation, so it changes no computed number — only latency.
 *
 * CORRECTNESS (never silently wrong — the whole point of the design):
 * the cache is keyed by the file's CONTENT HASH (streaming SHA-256), recomputed on
 * EVERY request — NOT mtime:size, which is defeated by equal-length content swaps
 * (e.g. a vendor rename or a corrected amount of equal digit count — exactly the
 * "spend by vendor" headline case), copy-preserving-mtime (cp -p, robocopy /COPY:T,
 * git checkout, backup-restore), coarse filesystem timestamps (exFAT/FAT/network),
 * and OneDrive/iCloud placeholder hydration. An entry is reused ONLY when the
 * file's current content hash equals the hash its rows were parsed from, so ANY
 * content change — however it touched (or didn't touch) mtime/size — busts it.
 * Hashing reads the file but skips the far heavier PARSE → still a large net win.
 * The chokidar watcher additionally evicts on change/unlink (defense-in-depth +
 * prompt memory release), but correctness does NOT depend on the watcher running.
 *
 * READ-ONLY CONTRACT: the returned rows array and its row objects are SHARED
 * across consumers and cached — callers MUST NOT mutate them in place (clone
 * first if you need to mutate). Today's consumers honor this: applyFilters →
 * new array, applyAggregations → reads only, column projection → new objects,
 * slice → new array.
 *
 * Kill switch: set AGENT_FILECACHE_DISABLED=1 to bypass entirely (always parse).
 */
import { canonicalPathKey, computeFileHash } from './state';

type Rows = Record<string, unknown>[];

interface CacheEntry {
  rows: Rows;
  sha256: string;
  bytes: number; // coarse retained-size estimate for the LRU byte cap
  lastUsed: number;
}

const DISABLED =
  process.env.AGENT_FILECACHE_DISABLED === 'true' || process.env.AGENT_FILECACHE_DISABLED === '1';
const MAX_ENTRIES = Math.max(1, Number(process.env.AGENT_FILECACHE_MAX_ENTRIES) || 8);
const MAX_BYTES = Math.max(
  16 * 1024 * 1024,
  Number(process.env.AGENT_FILECACHE_MAX_BYTES) || 512 * 1024 * 1024,
);

const cache = new Map<string, CacheEntry>();
// Single-flight: dedupe concurrent parses of the SAME (path, content) so a
// fan-out / paging burst on a cold file triggers one parse, not N.
const inflight = new Map<string, Promise<Rows>>();

/**
 * Coarse, column-aware retained-size estimate for the LRU byte cap. Samples up to
 * 200 rows (JSON length) ×2 for UTF-16 + object/key-string overhead. Bounded, not
 * exact — a static fileSize×multiplier under-counts because JS object overhead
 * scales with column count × row count, so we sample real rows instead.
 */
function estimateBytes(rows: Rows): number {
  if (!rows.length) return 0;
  const n = Math.min(rows.length, 200);
  let sample = 0;
  for (let i = 0; i < n; i++) {
    try {
      sample += JSON.stringify(rows[i]).length;
    } catch {
      sample += 256;
    }
  }
  return Math.round((sample / n) * rows.length * 2);
}

function totalBytes(): number {
  let t = 0;
  for (const e of cache.values()) t += e.bytes;
  return t;
}

function evictIfNeeded(): void {
  // Evict least-recently-used until within BOTH caps. Never evict the last
  // remaining entry on the byte cap alone (a single big-but-allowed file).
  while (cache.size > MAX_ENTRIES || (cache.size > 1 && totalBytes() > MAX_BYTES)) {
    let lruKey: string | undefined;
    let lru = Infinity;
    for (const [k, e] of cache) {
      if (e.lastUsed < lru) {
        lru = e.lastUsed;
        lruKey = k;
      }
    }
    if (lruKey === undefined) break;
    cache.delete(lruKey);
  }
}

/**
 * Return the parsed rows for `filePath`, reusing a cached parse iff the file's
 * current content hash matches the cached one. `loader` performs the real parse
 * on a miss. Returns `{ rows, cacheHit }` — `cacheHit` surfaces to `_diag` for
 * observability (confirming the cache hits on the paging / fan-out case).
 */
export async function getOrLoadParsedRows(
  filePath: string,
  loader: (fp: string) => Promise<Rows>,
  precomputedSha?: string,
): Promise<{ rows: Rows; cacheHit: boolean; sha256?: string }> {
  if (DISABLED) return { rows: await loader(filePath), cacheHit: false };

  const key = canonicalPathKey(filePath);
  // Authoritative content identity — recomputed every request so ANY content
  // change busts the entry, regardless of what it did to mtime/size. Also
  // surfaced as the pushdown `fileSig` so the proven-state + expectedFileSig
  // guard are CONTENT-exact (mtime:size can collide on a same-size edit within
  // one second → a stale serve; the content hash cannot).
  // `precomputedSha`: when the engine fast path already hashed this file moments
  // ago (streaming SHA-256, no parse) and then MISSED, it threads that hash here so
  // the engine-miss fall-through doesn't read+hash the file a 2nd time. The window is
  // ≤ the engine timeout (10s) and the chokidar watcher invalidates on change, so a
  // mid-flight content swap is caught on the NEXT request; the loader still reads the
  // current file bytes either way — only the cache KEY reuses the fresh-enough hash.
  const sha = precomputedSha || await computeFileHash(filePath);

  const hit = cache.get(key);
  if (hit && hit.sha256 === sha) {
    hit.lastUsed = Date.now();
    return { rows: hit.rows, cacheHit: true, sha256: sha };
  }

  const flightKey = `${key}\u0000${sha}`;
  let p = inflight.get(flightKey);
  if (!p) {
    p = (async () => {
      const rows = await loader(filePath);
      const bytes = estimateBytes(rows);
      // Don't let a single oversized file evict everything (and itself) — cache
      // only if it fits the byte budget; otherwise it just re-parses each time.
      if (bytes <= MAX_BYTES) {
        cache.set(key, { rows, sha256: sha, bytes, lastUsed: Date.now() });
        evictIfNeeded();
      }
      return rows;
    })();
    inflight.set(flightKey, p);
    void p.finally(() => {
      if (inflight.get(flightKey) === p) inflight.delete(flightKey);
    });
  }
  return { rows: await p, cacheHit: false, sha256: sha };
}

/**
 * Evict a file from the cache. Wired to the chokidar watcher's change/unlink
 * events — defense-in-depth + prompt memory release. Correctness does not depend
 * on this (the per-request content-hash check is authoritative).
 */
export function invalidateFileCache(filePath: string): void {
  cache.delete(canonicalPathKey(filePath));
}

/** Test/diagnostic helpers. */
export function __fileCacheSize(): number {
  return cache.size;
}
export function __fileCacheClear(): void {
  cache.clear();
  inflight.clear();
}
