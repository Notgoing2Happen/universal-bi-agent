/**
 * Streaming CSV reader for the agent.
 *
 * Phase 1 (2026-06-07, SCOPE.md): replaces the synchronous
 *   const text = fs.readFileSync(path, 'utf-8');
 *   const lines = text.split(/\r?\n/);
 *   for (const line of lines) parseCsvLine(line);
 *
 * pattern that historically pinned peak RSS at ~3× file size during parse
 * (UTF-8 string + lines array + parsed object array) — large CSVs OOM'd
 * the sidecar even under the 50MB Phase 0 cap because of the multiplier.
 *
 * Behavior gains over the previous parseCsvLine path:
 *   - UTF-8 BOM stripped via `bom: true` (was silently embedded in first header)
 *   - Multi-line quoted fields (cells with embedded \n) parse correctly
 *     (the old split('\n') path exploded quoted cells into multiple rows)
 *   - Ragged rows tolerated (`relax_column_count: true`) instead of throwing
 *   - Configurable delimiter — TSV is no longer parsed as a single-column CSV
 *     (the old code hardcoded ',' and routed `.tsv` files through it anyway)
 *
 * Behaviors preserved for class-shape parity with the legacy parser:
 *   - Type coercion: empty string stays '', numeric-looking values cast to
 *     Number, 'true'/'false' (case-insensitive) cast to booleans, everything
 *     else stays a string. csv-parse's built-in `cast: true` would parse some
 *     date-shaped strings via Date() — we explicitly opt out with a custom
 *     cast callback to match the prior heuristic exactly.
 *   - Trim: per-field trim (matches the old parseCsvLine `.trim()` on each
 *     assembled cell).
 *   - Skip empty lines.
 *
 * The exported `streamCsvRows()` is a true AsyncIterable; callers control
 * memory via reservoir sampling (Phase 2) or — for Phase 1 — accumulate
 * into a materialized array via `parseCsvFileBuffered()` (the same return
 * shape as the legacy parser, so Phase 1 is a drop-in replacement).
 */

import { createReadStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { parse, type CsvError, type Parser } from 'csv-parse';

export interface StreamCsvOptions {
  /** Custom delimiter (e.g., '\t' for TSV). Defaults to ',' if undefined. */
  delimiter?: string | string[];
  /** Abort signal — cancels file read + parser cleanup. */
  signal?: AbortSignal;
  /**
   * If true (default), normalize empty/number/bool values like the legacy parser.
   * If false, all cell values stay as raw strings (caller does its own coercion).
   */
  coerceTypes?: boolean;
  /**
   * Cap on cell size. Defaults to 1 MB (csv-parse's default is 128 KB which is
   * too low for the platform's known compound-text columns).
   */
  maxRecordSize?: number;
  /**
   * Phase 2b (2026-06-08): start parsing from this 1-indexed line. Used by the
   * uploader's schema-extraction path to skip preamble rows when AI #10
   * decided the header is NOT at line 1. The line at `fromLine` becomes the
   * HEADER (because the parser is built with `columns: true`). Data rows
   * begin at `fromLine + 1`.
   *
   * Example: AI #10 sees an Excel-exported CSV with 4 cover-page rows then
   * a real header at row 5. Caller passes `fromLine: 5`; csv-parse skips
   * rows 1-4, uses row 5 as the header, yields rows 6..N as data.
   *
   * Default: 1 (header at first non-empty line — legacy behavior).
   */
  fromLine?: number;
  /**
   * Phase 2b (2026-06-08): stop parsing AT this 1-indexed line. Used by the
   * uploader's schema-extraction path to skip footer rows. Default: parse
   * to EOF.
   */
  toLine?: number;
}

/**
 * Replicates the legacy parseCsvFile() coercion: empty string stays '',
 * numeric-looking values cast via Number(), 'true'/'false' cast to booleans.
 * Anything else stays a string.
 */
function legacyCoerce(value: string): string | number | boolean {
  if (value === '') return '';
  const num = Number(value);
  if (!isNaN(num) && value.trim() !== '') return num;
  const lower = value.toLowerCase();
  if (lower === 'true') return true;
  if (lower === 'false') return false;
  return value;
}

/**
 * Build a parser configured to match the legacy parseCsvFile behavior. Used
 * by both streamCsvRows() and parseCsvFileBuffered() so the two share the
 * same coercion + edge-case rules.
 */
function buildParser(opts: StreamCsvOptions): Parser {
  const coerce = opts.coerceTypes !== false;
  return parse({
    bom: true,
    columns: true,
    delimiter: opts.delimiter,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    trim: true,
    max_record_size: opts.maxRecordSize ?? 1_000_000,
    // Phase 2b: pass through from_line / to_line for preamble + footer
    // handling. csv-parse treats `from_line` as the new "header line" when
    // columns:true, so callers should pass fromLine = headerRowIdx + 1
    // (1-indexed) when AI #10 says the header isn't at line 1.
    ...(opts.fromLine !== undefined ? { from_line: opts.fromLine } : {}),
    ...(opts.toLine !== undefined ? { to_line: opts.toLine } : {}),
    cast: coerce
      ? (value: string, ctx: { header: boolean }) => (ctx.header ? value : legacyCoerce(value))
      : false,
  });
}

/**
 * Wrap any error thrown during streaming so the message includes parser
 * location info (line, record). csv-parse's CsvError already carries a `.code`
 * but the default message is terse; this surfaces a clearer failure to the
 * /query handler's catch.
 */
function rethrowCsvError(err: unknown, parser: Parser, filePath: string): never {
  const code = (err as CsvError)?.code;
  if (typeof code === 'string' && code.startsWith('CSV_')) {
    const csvErr = err as CsvError;
    throw new Error(
      `CSV parse failed in "${filePath}" at line ${parser.info?.lines ?? '?'} ` +
        `(record ${parser.info?.records ?? '?'}): ${code} — ${csvErr.message}`,
      { cause: csvErr },
    );
  }
  throw err;
}

/**
 * Stream a CSV file row-by-row. Yields one row at a time; backpressure is
 * native (slow consumer pauses the file read at the OS level via the
 * Transform's internal high-water mark).
 *
 * The legacy `normalizeColumnName()` is NOT applied here — callers wrap with
 * `normalizeRowColumns()` if they want it. This keeps the streaming reader
 * neutral about caller-specific naming conventions.
 */
export async function* streamCsvRows(
  filePath: string,
  opts: StreamCsvOptions = {},
): AsyncGenerator<Record<string, unknown>, void, void> {
  const fileStream = createReadStream(filePath, { highWaterMark: 64 * 1024 });
  const parser = buildParser(opts);

  // We need to bridge a Parser (Transform stream) into a generator. The pattern
  // below uses pipeline() under the hood via .pipe() + manual error propagation,
  // because using pipeline() with a consumer fn inside a generator would
  // require yielding from inside the consumer (not possible).
  fileStream.pipe(parser);

  let streamError: Error | null = null;
  const onError = (err: Error) => {
    streamError = err;
  };
  fileStream.on('error', onError);
  parser.on('error', onError);

  // AbortSignal: tear down the file stream + parser on abort.
  let onAbort: (() => void) | undefined;
  if (opts.signal) {
    if (opts.signal.aborted) {
      fileStream.destroy(new Error('Aborted'));
      parser.destroy(new Error('Aborted'));
      throw new Error('Aborted');
    }
    onAbort = () => {
      fileStream.destroy(new Error('Aborted'));
      parser.destroy(new Error('Aborted'));
    };
    opts.signal.addEventListener('abort', onAbort, { once: true });
  }

  try {
    for await (const row of parser as AsyncIterable<Record<string, unknown>>) {
      if (streamError) rethrowCsvError(streamError, parser, filePath);
      yield row;
    }
    if (streamError) rethrowCsvError(streamError, parser, filePath);
  } catch (err) {
    rethrowCsvError(err, parser, filePath);
  } finally {
    if (opts.signal && onAbort) opts.signal.removeEventListener('abort', onAbort);
    if (!fileStream.destroyed) fileStream.destroy();
    if (!parser.destroyed) parser.destroy();
  }
}

/**
 * Drain a CSV file into a materialized array. Phase 1 keeps the legacy
 * `{ rows: Record<string, unknown>[] }` return contract — Phase 2 will
 * swap callers to streaming / reservoir sampling.
 *
 * Memory profile: peak ≈ size of the final rows array. The historic 3×
 * multiplier (UTF-8 string + lines array + parsed array) is gone — the
 * file is streamed, parsed token-by-token, and only the assembled rows
 * are retained.
 */
export async function parseCsvFileBuffered(
  filePath: string,
  opts: StreamCsvOptions = {},
): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  for await (const row of streamCsvRows(filePath, opts)) {
    rows.push(row);
  }
  return rows;
}
