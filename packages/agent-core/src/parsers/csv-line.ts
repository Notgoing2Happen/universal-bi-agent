/**
 * Parse a single CSV line, respecting double-quoted fields.
 *
 * Phase 1 (2026-06-07): canonical source of truth. Previously duplicated
 * verbatim in query-server.ts:155 and uploader.ts:255. Kept as a thin
 * helper because uploader.ts's parseCsvSchema still uses it during the
 * Phase 1 transition (it pre-splits lines before per-line parsing); the
 * streaming csv-parse-driven path doesn't need it because csv-parse owns
 * tokenization end-to-end.
 *
 * Handles:
 *   - Comma-separated values
 *   - Double-quoted fields containing commas
 *   - Escaped double-quotes ("")
 *   - Trims whitespace around assembled fields
 *
 * Known limitations (preserved for behavioral parity with the pre-Phase-1
 * implementation — fixing these is Phase 2 territory):
 *   - Hardcoded comma delimiter (TSV / pipe / semicolon need a different splitter)
 *   - Quoted fields with embedded newlines are NOT supported; the caller
 *     splits on `\n` before this is invoked, so multi-line quoted cells
 *     explode into multiple malformed rows. The csv-parse streaming reader
 *     does support multi-line quotes — Phase 1's streaming CSV path uses it.
 */
export function parseCsvLine(line: string): string[] {
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
