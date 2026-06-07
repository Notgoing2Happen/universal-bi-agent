/**
 * Shared parser primitives for the agent.
 *
 * Phase 1 (2026-06-07, SCOPE.md): both query-server.ts (live query path) and
 * uploader.ts (schema upload path) historically carried verbatim duplicates of
 * normalizeColumnName + parseCsvLine. When one path fixed an edge case
 * (UTF-8 BOM, header dedup, etc.) the other silently drifted. The Phase 1
 * audit explicitly flagged this as a class-shape risk — addressed here by
 * making both paths import from this single module.
 *
 * Phase 2 will extend this module with reservoir-sampling primitives.
 */

export { normalizeColumnName } from './normalize';
export { parseCsvLine } from './csv-line';
export {
  streamCsvRows,
  parseCsvFileBuffered,
  type StreamCsvOptions,
} from './stream-csv';
