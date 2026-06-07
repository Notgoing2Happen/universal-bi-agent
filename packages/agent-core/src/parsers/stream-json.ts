/**
 * Streaming JSON reader for the agent.
 *
 * Phase 1 (2026-06-07, SCOPE.md): replaces the synchronous
 *   const text = fs.readFileSync(path, 'utf-8');
 *   const parsed = JSON.parse(text);
 * pattern that allocated the full UTF-8 string AND the parsed object graph
 * before yielding a single row. For row-bearing JSON files (the common
 * shape for API exports — { data: [...] } or a root array) memory peaked
 * at ~2-3× file size.
 *
 * Behavior preserved from the legacy parseJsonFile():
 *   - Root array  → rows = parsed
 *   - Root object → rows = the first array-valued top-level key, OR
 *                   [parsed] if no top-level array exists
 *   - Root scalar → rows = []
 *
 * The shape decision is made by sniffing a 16KB head slice (cheap regex,
 * no JSON parse), keeping the streaming path bounded even for pathological
 * inputs. Falls back to fully-buffered JSON.parse for the no-top-level-array
 * branch since that branch's output is the WHOLE parsed object wrapped in
 * a one-element array — there's no streaming win when the consumer needs
 * the whole object anyway.
 *
 * Shape sniffer recognized row keys (a top-level JSON object whose value
 * at one of these keys is an array): `data`, `items`, `rows`, `results`,
 * `records`, `value`. Matches the conventions of common REST API exports
 * (HubSpot, Stripe, OData, JSON:API). Files using a custom key fall
 * through to the buffered path — still correct, just not streamed.
 *
 * Module loading: stream-json 3.3.0 is ESM-only and declares a subpath
 * exports map that the CJS TS compiler can't statically resolve. We load
 * its submodules via require() at runtime (same pattern as parseExcelFile
 * uses for xlsx in query-server.ts). Type bindings are minimal — the
 * package's runtime contract is stable across versions.
 */

import * as fs from 'node:fs';

// Minimal type shapes for what we use from stream-json + stream-chain.
// The runtime is loaded via require() below to avoid TS module-resolution
// issues with the package's ESM-only exports map under moduleResolution=node.
type Duplex = NodeJS.ReadWriteStream & {
  destroyed: boolean;
  destroy(error?: Error): void;
  on(event: 'error', listener: (err: Error) => void): Duplex;
  [Symbol.asyncIterator](): AsyncIterableIterator<{ key: number; value: unknown }>;
};

interface StreamJsonModules {
  // stream-chain default export: chain(stages) → Duplex
  chain: (stages: unknown[]) => Duplex;
  // stream-json parser factory
  parser: () => unknown;
  // stream-json/streamers/stream-array.js
  streamArray: () => unknown;
  // stream-json/filters/pick.js
  pick: (opts: { filter: string }) => unknown;
}

let cachedModules: StreamJsonModules | null = null;

/**
 * Load stream-json + stream-chain via require() exactly once.
 *
 * The packages ship CJS shims for require() interop; we use them to dodge
 * the TS moduleResolution mismatch (agent-core compiles to CJS, the
 * packages are ESM with subpath exports the CJS resolver can't see
 * statically).
 */
function loadModules(): StreamJsonModules {
  if (cachedModules) return cachedModules;

  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const streamChainMod = require('stream-chain');
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const streamJsonMod = require('stream-json');
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const streamArrayMod = require('stream-json/streamers/stream-array');
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const pickMod = require('stream-json/filters/pick');

  const chain = streamChainMod.chain ?? streamChainMod.default?.chain ?? streamChainMod;
  const parser = streamJsonMod.parser ?? streamJsonMod.default?.parser ?? streamJsonMod;
  const streamArray =
    streamArrayMod.streamArray ?? streamArrayMod.default?.streamArray ?? streamArrayMod;
  const pick = pickMod.pick ?? pickMod.default?.pick ?? pickMod;

  cachedModules = {
    chain: chain as StreamJsonModules['chain'],
    parser: parser as StreamJsonModules['parser'],
    streamArray: streamArray as StreamJsonModules['streamArray'],
    pick: pick as StreamJsonModules['pick'],
  };
  return cachedModules;
}

const ROW_KEY_CANDIDATES = ['data', 'items', 'rows', 'results', 'records', 'value'] as const;

type RootShape =
  | { kind: 'array' }
  | { kind: 'object-with-rows'; rowKey: string }
  | { kind: 'object-no-array' }
  | { kind: 'scalar' };

/**
 * Peek the first ~16KB of the file to decide which streaming pipeline to
 * build. Pure string ops on a head slice — no JSON parse, no allocation
 * of the whole file. Tolerates leading UTF-8 BOM and whitespace.
 */
async function sniffShape(filePath: string): Promise<RootShape> {
  const fd = await fs.promises.open(filePath, 'r');
  try {
    const buf = Buffer.alloc(16 * 1024);
    const { bytesRead } = await fd.read(buf, 0, buf.length, 0);
    let head = buf.subarray(0, bytesRead).toString('utf8');
    // Strip BOM + leading whitespace
    if (head.charCodeAt(0) === 0xfeff) head = head.slice(1);
    head = head.replace(/^\s+/, '');
    if (head.length === 0) return { kind: 'scalar' };
    if (head.startsWith('[')) return { kind: 'array' };
    if (!head.startsWith('{')) return { kind: 'scalar' };
    // Look for a top-level key whose value opens with `[`. Cheap heuristic;
    // correct for the well-known REST API shapes. Doesn't try to be smart
    // about deeply nested arrays — those fall through to the buffered path.
    for (const k of ROW_KEY_CANDIDATES) {
      const re = new RegExp(`"${k}"\\s*:\\s*\\[`);
      if (re.test(head)) return { kind: 'object-with-rows', rowKey: k };
    }
    return { kind: 'object-no-array' };
  } finally {
    await fd.close();
  }
}

/**
 * Stream a JSON file row-by-row. The shape is detected once at the start;
 * the streaming pipeline is built accordingly:
 *   - kind=array:            file → parser → streamArray
 *   - kind=object-with-rows: file → parser → pick(rowKey) → streamArray
 *   - kind=object-no-array:  falls back to buffered JSON.parse + yield [parsed]
 *   - kind=scalar:           emits nothing
 *
 * Errors from any stage (malformed JSON, I/O) propagate to the caller as
 * a thrown exception. No silent-empty: if the file ends mid-array the
 * caller sees an error AFTER receiving any rows that were already valid
 * (matches the platform's "throw, don't silently swallow" rule).
 */
export async function* streamJsonRows<TRow = Record<string, unknown>>(
  filePath: string,
): AsyncGenerator<TRow, void, void> {
  const shape = await sniffShape(filePath);

  if (shape.kind === 'scalar') {
    return;
  }

  if (shape.kind === 'object-no-array') {
    // No row-bearing array — preserve legacy [parsed] behavior. Memory cost
    // is bounded by the Phase 0 file-size cap; no streaming win available
    // for this shape because the consumer needs the whole object anyway.
    const text = await fs.promises.readFile(filePath, 'utf8');
    const parsed = JSON.parse(text);
    yield parsed as TRow;
    return;
  }

  const mods = loadModules();
  const stages: unknown[] = [fs.createReadStream(filePath)];
  stages.push(mods.parser());
  if (shape.kind === 'object-with-rows') {
    stages.push(mods.pick({ filter: shape.rowKey }));
  }
  stages.push(mods.streamArray());

  const pipeline = mods.chain(stages);

  let streamError: Error | null = null;
  const onError = (err: Error) => {
    streamError = err;
  };
  pipeline.on('error', onError);

  try {
    for await (const item of pipeline) {
      if (streamError) throw streamError;
      yield item.value as TRow;
    }
    if (streamError) throw streamError;
  } finally {
    if (!pipeline.destroyed) pipeline.destroy();
  }
}

/**
 * Drain a JSON file into a materialized array. Phase 1 keeps the legacy
 * `Record<string, unknown>[]` return contract so loadFileData() stays
 * shape-compatible with /query, /sequence-region, /schema handlers.
 *
 * Memory profile: peak ≈ size of the final rows array (no longer
 * "UTF-8 string + parsed graph + extracted array" triple-allocation).
 */
export async function parseJsonFileBuffered(
  filePath: string,
): Promise<Record<string, unknown>[]> {
  const shape = await sniffShape(filePath);

  if (shape.kind === 'scalar') return [];

  if (shape.kind === 'object-no-array') {
    // Legacy parity: wrap the whole object in [parsed]. Buffered because the
    // consumer needs the whole object regardless.
    const text = await fs.promises.readFile(filePath, 'utf8');
    const parsed = JSON.parse(text);
    if (parsed === null || typeof parsed !== 'object') return [];
    return [parsed as Record<string, unknown>];
  }

  const rows: Record<string, unknown>[] = [];
  for await (const row of streamJsonRows<Record<string, unknown>>(filePath)) {
    rows.push(row);
  }
  return rows;
}
