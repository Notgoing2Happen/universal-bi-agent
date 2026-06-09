/**
 * Reservoir sampling — Algorithm R (Vitter, 1985).
 *
 * Phase 2 (2026-06-08, SCOPE.md): drains an async iterable to produce a
 * fixed-size uniform random sample WITHOUT holding more than k items in
 * memory. Used by the schema-extraction path (uploader.ts) so a 50MB
 * JSON file with 1M rows doesn't allocate 1M parsed objects just to
 * pick K sample rows for AI #1 / type inference.
 *
 * Memory profile:
 *   - Reservoir: exactly k items, fixed regardless of stream length
 *   - Per-row peak: 1 parsed row (released after the reservoir decision)
 *   - Total: O(k), independent of stream size
 *
 * Uniformity guarantee:
 *   - First k items always retained
 *   - Item i (i > k) replaces a random reservoir slot with probability k/i
 *   - Net: every item has identical k/n probability of being in the final
 *     sample, where n = total stream length
 *
 * Determinism:
 *   - Pass a custom `rng` to make the sample deterministic for tests.
 *   - Default uses Math.random() — non-deterministic but uniform.
 *
 * Why this and not "first k rows"?
 *   - Heterogeneous JSON files (e.g. API exports) often put the longest /
 *     most-complete records at the end. First-k biases the type inference
 *     toward truncated header records. Reservoir keeps it uniform.
 *
 * Why this and not "every Nth row"?
 *   - Files with periodic structure (every 100th row is a section header,
 *     etc.) would resonance-sample the periodic value. Reservoir is
 *     resistant to any structural pattern.
 */

export interface ReservoirResult<T> {
  /** The final sample. May be smaller than k if the stream had fewer items. */
  sample: T[];
  /** Total number of items observed from the stream. */
  total: number;
}

export interface ReservoirOptions {
  /** Override Math.random for deterministic sampling. Returns [0, 1). */
  rng?: () => number;
  /** Optional cap on total items processed (for safety against unbounded streams). */
  maxTotal?: number;
}

/**
 * Drain `iter`, keep a uniform random k-sample, return {sample, total}.
 *
 * Throws if the stream's underlying source throws (does NOT swallow stream
 * errors — matches the "throw, don't silently return empty" rule).
 */
export async function reservoirSample<T>(
  iter: AsyncIterable<T>,
  k: number,
  opts: ReservoirOptions = {},
): Promise<ReservoirResult<T>> {
  if (k < 0) throw new Error(`reservoirSample: k must be >= 0 (got ${k})`);
  if (k === 0) {
    // Still drain the iterator to count rows. Callers may want `total` even
    // when they don't need samples.
    let totalDrained = 0;
    const maxTotalDrain = opts.maxTotal ?? Infinity;
    for await (const _ of iter) {
      void _;
      totalDrained++;
      if (totalDrained >= maxTotalDrain) break;
    }
    return { sample: [], total: totalDrained };
  }

  const rng = opts.rng ?? Math.random;
  const maxTotal = opts.maxTotal ?? Infinity;
  const reservoir: T[] = [];
  let total = 0;

  for await (const item of iter) {
    if (total < k) {
      reservoir.push(item);
    } else {
      // Pick a random index in [0, total]. If it falls in [0, k), replace
      // that reservoir slot. Probability of replacement = k / (total + 1)
      // for the i-th item, which gives uniform k/n at end of stream.
      const j = Math.floor(rng() * (total + 1));
      if (j < k) {
        reservoir[j] = item;
      }
    }
    total++;
    if (total >= maxTotal) break;
  }

  return { sample: reservoir, total };
}

/**
 * Reservoir sampling for column-value extraction. Specialized for the
 * uploader pipeline: given a stream of row objects, returns
 *   - sample: up to k sample rows (random)
 *   - allColumnNames: superset of all keys seen across ALL rows (NOT just
 *     the reservoir — JSON files can have heterogeneous schemas, and
 *     missing a column from the reservoir sample is worse than missing
 *     a sample value for an observed column)
 *   - rowCount: total rows in the stream
 *
 * Memory: reservoir size (k rows) + column-name set. Independent of file
 * size for the sample; column-name set grows with schema heterogeneity
 * (a row object with N keys adds at most N strings).
 */
export interface SchemaSamplingResult {
  sample: Record<string, unknown>[];
  allColumnNames: string[];
  rowCount: number;
}

export async function reservoirSampleWithColumnDiscovery(
  iter: AsyncIterable<Record<string, unknown>>,
  k: number,
  opts: ReservoirOptions = {},
): Promise<SchemaSamplingResult> {
  if (k < 0) throw new Error(`reservoirSampleWithColumnDiscovery: k must be >= 0 (got ${k})`);
  const rng = opts.rng ?? Math.random;
  const maxTotal = opts.maxTotal ?? Infinity;
  const reservoir: Record<string, unknown>[] = [];
  const columnNames = new Set<string>();
  let total = 0;

  for await (const row of iter) {
    // Always update column-name set — independent of reservoir membership.
    // Heterogeneous JSON: row 1 might have {a, b}, row 999999 might have
    // {a, c}. We want both b AND c reflected in the schema even if only
    // one ends up in the reservoir sample.
    if (row && typeof row === 'object') {
      for (const key of Object.keys(row)) {
        columnNames.add(key);
      }
    }
    // Reservoir update — same logic as reservoirSample.
    if (total < k) {
      reservoir.push(row);
    } else {
      const j = Math.floor(rng() * (total + 1));
      if (j < k) {
        reservoir[j] = row;
      }
    }
    total++;
    if (total >= maxTotal) break;
  }

  return {
    sample: reservoir,
    allColumnNames: Array.from(columnNames),
    rowCount: total,
  };
}
