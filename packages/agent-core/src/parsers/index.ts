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
 * Phase 2 (2026-06-08): reservoir-sampling primitives added. The uploader's
 * JSON schema-extraction path now uses streamJsonRows + reservoirSample
 * to bound memory by sample size rather than file size. CSV uploader
 * streaming is a follow-up (needs StreamCsvOptions to gain headerRowIdx /
 * from_line support).
 */

export { normalizeColumnName } from './normalize';
export { parseCsvLine } from './csv-line';
export {
  streamCsvRows,
  parseCsvFileBuffered,
  legacyCoerce,
  coerceDetailRows,
  type StreamCsvOptions,
} from './stream-csv';
export {
  streamJsonRows,
  parseJsonFileBuffered,
} from './stream-json';
export {
  reservoirSample,
  reservoirSampleWithColumnDiscovery,
  type ReservoirResult,
  type ReservoirOptions,
  type SchemaSamplingResult,
} from './reservoir';
