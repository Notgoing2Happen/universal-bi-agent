/**
 * String normalization for sync/profile-time data quality.
 *
 * Catches the broad class of "invisible / look-alike Unicode corruption" bugs:
 *   - Zero-width spaces (U+200B, U+200C, U+200D, U+2060, U+FEFF) inside values
 *   - Non-breaking spaces (U+00A0, U+202F, U+2007) where regular space expected
 *   - Fullwidth / halfwidth duplicates (via NFKC) - e.g. fullwidth digits
 *   - Ligature compatibility forms (fi ligature -> fi) via NFKC
 *   - Soft hyphens (U+00AD), bidi marks (U+200E, U+200F)
 *   - Trailing / leading regular + non-breaking whitespace
 *
 * Used by:
 *   - buildValueProfile - to flag columns whose samples carry invisible chars
 *   - AI #8 row anomaly scan - to flag rows where normalization would change
 *     the value (data corruption signal)
 *   - Realignment engine - to compare values fairly across shift candidates
 *
 * Design: pure function, no AI. Same pattern as the rest of the integrity
 * pipeline - deterministic checks layered before AI fallback.
 */

// Codepoints we strip entirely. Built via String.fromCodePoint() so the source
// is auditable and immune to file-encoding surprises. Each entry corresponds
// to a well-known invisible / control character that never carries meaning in
// typical business data.
const STRIP_CODEPOINTS: number[] = [
  0x00AD, // soft hyphen
  0x200B, // zero-width space
  0x200C, // zero-width non-joiner
  0x200D, // zero-width joiner
  0x200E, // left-to-right mark
  0x200F, // right-to-left mark
  0x202A, // left-to-right embedding
  0x202B, // right-to-left embedding
  0x202C, // pop directional formatting
  0x202D, // left-to-right override
  0x202E, // right-to-left override
  0x2060, // word joiner
  0x2066, // left-to-right isolate
  0x2067, // right-to-left isolate
  0x2068, // first strong isolate
  0x2069, // pop directional isolate
  0xFEFF, // zero-width no-break space (BOM)
];

// Whitespace forms that should collapse to a single regular space.
const WHITESPACE_CODEPOINTS: number[] = [
  0x00A0, // non-breaking space
  0x1680, // ogham space mark
  0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, // en-quad, em-quad, en-space, em-space, three-per-em, four-per-em
  0x2006, 0x2007, 0x2008, 0x2009, 0x200A, // six-per-em, figure, punctuation, thin, hair
  0x2028, // line separator
  0x2029, // paragraph separator
  0x202F, // narrow no-break space
  0x205F, // medium mathematical space
  0x3000, // ideographic space
];

function buildCharClassRegex(codepoints: number[], flags: string): RegExp {
  // Escape any regex-meaningful chars in the resulting char class. None of our
  // codepoints are regex-special, but using fromCodePoint + char-class form is
  // safe even if a future codepoint added were special.
  const chars = codepoints.map((cp) => String.fromCodePoint(cp)).join('');
  return new RegExp('[' + chars + ']', flags);
}

const STRIP_RE = buildCharClassRegex(STRIP_CODEPOINTS, 'g');
const WHITESPACE_RE = buildCharClassRegex(WHITESPACE_CODEPOINTS, 'g');

/** Result of normalizing a single value. */
export interface NormalizationResult {
  /** The normalized string (NFKC + invisible-char stripped + whitespace collapsed) */
  normalized: string;
  /** True iff the normalized form differs from the input */
  changed: boolean;
  /** Codepoints we found and stripped (for surfacing in anomalies) */
  strippedCodepoints: string[];
  /** True iff input contained a non-regular whitespace form */
  hadInvisibleWhitespace: boolean;
}

/**
 * Normalize a single string value. Returns the normalized form plus signals
 * about what changed - callers decide whether to use the normalized version,
 * flag the row, or both.
 *
 * Safe for non-strings: numbers, booleans, null, undefined are returned as
 * `{ normalized: String(value), changed: false }`. The integrity layer cares
 * only about string corruption.
 */
export function normalizeString(value: unknown): NormalizationResult {
  if (value === null || value === undefined) {
    return { normalized: '', changed: false, strippedCodepoints: [], hadInvisibleWhitespace: false };
  }
  if (typeof value !== 'string') {
    return { normalized: String(value), changed: false, strippedCodepoints: [], hadInvisibleWhitespace: false };
  }

  const original = value;

  // Collect codepoints that will be stripped - surfacing these helps anomaly
  // diagnostics ("ZWSP between 0008 and the next char")
  const strippedCodepoints: string[] = [];
  STRIP_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = STRIP_RE.exec(original)) !== null) {
    const cp = original.codePointAt(m.index);
    if (cp !== undefined) strippedCodepoints.push(`U+${cp.toString(16).toUpperCase().padStart(4, '0')}`);
  }

  // Apply transforms in order: NFKC (canonical compose + compatibility),
  // strip invisible control codepoints, collapse non-regular whitespace,
  // trim leading/trailing whitespace.
  let normalized = original.normalize('NFKC');
  normalized = normalized.replace(STRIP_RE, '');

  // Check whitespace BEFORE replacing so we can record the signal.
  WHITESPACE_RE.lastIndex = 0;
  const hadInvisibleWhitespace = WHITESPACE_RE.test(normalized);
  WHITESPACE_RE.lastIndex = 0;
  normalized = normalized.replace(WHITESPACE_RE, ' ');

  // Collapse internal runs of regular whitespace to a single space, then trim.
  normalized = normalized.replace(/[ \t]+/g, ' ').trim();

  return {
    normalized,
    changed: normalized !== original,
    strippedCodepoints,
    hadInvisibleWhitespace,
  };
}

/**
 * Probe a sample of values: what fraction carry invisible-char corruption?
 *
 * Used by buildValueProfile to set `invisibleCharRate` so downstream consumers
 * (AI #8, realignment, AI #1 fit warnings) know whether a column tends to
 * have corruption - and so they can flag NEW values introducing it.
 */
export function probeInvisibleCharRate(samples: unknown[]): {
  invisibleCharRate: number;
  sampleSize: number;
} {
  if (samples.length === 0) return { invisibleCharRate: 0, sampleSize: 0 };
  let affected = 0;
  let stringCount = 0;
  for (const v of samples) {
    if (typeof v !== 'string') continue;
    stringCount++;
    const r = normalizeString(v);
    if (r.changed) affected++;
  }
  if (stringCount === 0) return { invisibleCharRate: 0, sampleSize: 0 };
  return { invisibleCharRate: affected / stringCount, sampleSize: stringCount };
}
