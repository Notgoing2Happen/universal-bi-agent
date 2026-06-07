/**
 * Normalize a column name to snake_case lowercase.
 *
 * Phase 1 (2026-06-07): canonical source of truth. Previously duplicated
 * verbatim in query-server.ts:105 and uploader.ts:188 — the two copies
 * could and did drift. Both paths now import this single function.
 *
 * Examples:
 *   "Percent_Q30"      → "percent_q30"
 *   "OD_450nm"         → "od_450nm"
 *   "sampleTypeID"     → "sample_type_id"
 *   "firstName"        → "first_name"
 *   "HTMLParser"       → "html_parser"
 *   "Sample Name"      → "sample_name"
 *   "first-name"       → "first_name"
 *   "_leading"         → "leading"
 *   "trailing_"        → "trailing"
 */
export function normalizeColumnName(name: string): string {
  return name
    // camelCase → camel_case: lower followed by upper
    .replace(/([a-z])([A-Z])/g, '$1_$2')
    // HTMLParser → HTML_Parser: upper run followed by upper+lower
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    // Replace spaces and hyphens with underscores
    .replace(/[\s-]+/g, '_')
    // Lowercase everything
    .toLowerCase()
    // Collapse multiple underscores
    .replace(/_+/g, '_')
    // Remove leading/trailing underscores
    .replace(/^_|_$/g, '');
}
