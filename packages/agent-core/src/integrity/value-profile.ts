/**
 * Value Profile — column-level fingerprint of "what values belong here."
 *
 * Built deterministically from sample values during AI #1 column mapping (no
 * extra AI call — uses samples AI #1 already collects). Stored on
 * `ColumnMapping.valueProfile` and consumed by the realignment engine to
 * detect rows where values landed in the wrong column (column-shift bugs).
 *
 * Example: for a Status column with rows {Delivered, Pending, Ordered}, the
 * profile captures `primaryType: 'string'` + `enumValues: [...]`. At sync
 * time, a value like `"2027-09-01"` fails both the enum check (not in set)
 * and looks like a date (pattern signature). Realignment engine then tries
 * shift candidates to find which column the date actually belongs in.
 *
 * Design principle: AI builds the profile once at setup time; deterministic
 * code uses it for every row at sync/query time. Same pattern as the AI #4
 * self-healing pipeline already in production.
 */
import { z } from 'zod';
import { probeInvisibleCharRate } from './normalize-string';

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

/** Pattern signatures recognized by the deterministic profile builder. */
export interface ValuePatterns {
  /** ISO-like dates: YYYY-MM-DD, MM/DD/YYYY, DD-MMM-YYYY */
  isDate?: boolean;
  /** Date + time: YYYY-MM-DDTHH:MM:SS or similar */
  isDatetime?: boolean;
  /** RFC 5322 email shape */
  isEmail?: boolean;
  /** UUID v4 shape */
  isUuid?: boolean;
  /** Currency: $1,234.56 / 1234.56 USD / etc. */
  isCurrency?: boolean;
  /** Percentage: 12.5% / 0.125 (with column-level context) */
  isPercent?: boolean;
  /** http(s):// URL */
  isUrl?: boolean;
  /** Phone number (loose pattern) */
  isPhone?: boolean;
  /** AI-derived custom regex (optional, populated by AI #1 when it spots a structured pattern) */
  customRegex?: string;
}

export type PrimaryType =
  | 'string'
  | 'number'
  | 'integer'
  | 'date'
  | 'datetime'
  | 'boolean'
  | 'mixed';

/**
 * EmissionStrategy — instructs the cube generator how to parse the column's
 * text values into the typed concept when generating dim SQL.
 *
 * Class-shaped per CLAUDE.md: deterministic detection via regex on FORMAT
 * SHAPE (not value enumeration). AI fallback (via AIClassificationCache)
 * handles novel formats; once classified, the strategy is cached forever.
 *
 * Examples driving the design (production 2026-05-31):
 *   - "Unknown" in numeric column → with_sentinels(canonical)
 *   - "Unknown" in date column → with_sentinels(canonical)
 *   - "8/4/2025" in expiration date → us_date_mdy
 *   - "$1,234.56" in spend column → currency_prefixed
 *   - "12.5%" in conversion rate → percentage_string
 *   - "yes"/"no" in flags → boolean_text
 */
export type EmissionStrategyKind =
  | 'canonical'           // ISO date / plain numeric — bare `::TYPE` cast works as-is
  | 'us_date_mdy'         // 8/4/2025 → TO_TIMESTAMP(col, 'MM/DD/YYYY')
  | 'us_date_mdy_dash'    // 8-4-2025 → TO_TIMESTAMP(col, 'MM-DD-YYYY')
  | 'eu_date_dmy'         // 4.8.2025 → TO_TIMESTAMP(col, 'DD.MM.YYYY')
  | 'eu_date_dmy_slash'   // 4/8/2025 in EU locale (disambiguated via AI when ambiguous)
  | 'text_date_natural'   // "Aug 4, 2025" → AI-driven format mask, cached
  | 'currency_prefixed'   // "$1,234.56" / "€1.234,56" → strip symbols + commas, cast numeric
  | 'percentage_string'   // "12.5%" → strip % + cast (caller decides /100 vs raw)
  | 'thousand_separated'  // "1,234,567" → strip commas, cast numeric
  | 'boolean_text'        // "yes"/"no"/"y"/"n"/"true"/"false" → CASE WHEN
  | 'unknown';            // AI couldn't classify — emit bare cast as fallback (current behavior)

export interface EmissionStrategy {
  /** Which parsing strategy to apply at cube emission time. */
  kind: EmissionStrategyKind;

  /**
   * When kind='with_sentinels' wrapping applies, the column has placeholder
   * values that should be NULLed before applying the inner strategy. Listed
   * here so the cube generator can emit the right CASE WHEN guard.
   *
   * Sentinels are detected language-agnostically: deterministic English/
   * Western defaults + AI fallback (cached globally via
   * AIClassificationCache, discriminator 'sentinel_value'). Per CLAUDE.md
   * "AI is the DEFAULT tool" for user-defined data.
   */
  sentinelValues?: string[];

  /**
   * For text_date_natural / currency_prefixed strategies that needed AI to
   * resolve, the format mask or rule the AI returned. Caches the parsing
   * logic without re-running AI per query.
   */
  formatMask?: string;

  /** Confidence in the strategy classification (deterministic = 1.0). */
  confidence?: number;

  /** How the strategy was detected — for observability + audit. */
  detectedVia?: 'deterministic' | 'ai_fallback' | 'cache';
}

/** Full value profile persisted to ColumnMapping.valueProfile (JSONB). */
export interface ValueProfile {
  /** Dominant value type across samples */
  primaryType: PrimaryType;

  /**
   * Low-cardinality enum members. Populated when distinctValues ≤ 30 AND
   * those values cover ≥ 90% of non-null samples. Used to catch "wrong-value"
   * shifts (e.g., a date in a status enum column).
   */
  enumValues?: string[];

  /** Structural pattern signatures (any-of) */
  patterns?: ValuePatterns;

  /** Fraction of samples that are null/empty (0..1) */
  nullRate: number;

  /** Fraction of distinct values relative to sample size (0..1) — 1.0 = all unique (likely an ID) */
  uniqueRate: number;

  /** How many samples this profile was built from */
  sampleSize: number;

  /** For numeric columns — observed value range */
  numericRange?: { min: number; max: number };

  /** For string columns — observed length range */
  lengthRange?: { min: number; max: number };

  /**
   * Fraction of string samples that contained invisible-char / look-alike
   * Unicode corruption (zero-width spaces, non-breaking spaces, ligatures,
   * fullwidth duplicates, etc.). Built via normalize-string.ts.
   *
   * Used by:
   *  - AI #8 row scan: flag rows whose values would normalize differently
   *    (PO-2026-0008 ZWSP corruption class)
   *  - Realignment: compare normalized forms across shift candidates
   *  - AI #1 fit warnings: low rate in profile + high rate in sample = signal
   *
   * Typically 0 for clean data. A persistently nonzero rate (>0.05) means the
   * source carries invisible chars systematically (e.g. exports from a system
   * that injects BOMs); not necessarily a bug.
   */
  invisibleCharRate?: number;

  /**
   * How the cube generator should emit dim SQL for this column when the
   * mapped concept is typed (date/number/boolean). Deterministically
   * detected from sample shapes during AI #1; AI fallback covers novel
   * formats (cached via AIClassificationCache, discriminator
   * 'value_format'). When absent, cube generator falls back to the bare
   * `::TYPE` cast (current behavior). Per CLAUDE.md class-shaped rule:
   * format detection is STRUCTURAL (regex on shape), not enumeration of
   * specific values — works for any tenant in any language with any
   * date/currency convention.
   */
  emissionStrategy?: EmissionStrategy;

  /**
   * FK-candidate signal (Item B, 2026-06-03). Populated by callers via
   * `detectFkShape(columnName, profile)` when they have both — buildValueProfile
   * itself doesn't have column-name context.
   *
   * Compound structural signal: name suffix + value shape + uniqueness. When
   * all three agree, confidence is high enough to auto-emit a
   * UniversalRelationship. When only the shape agrees (no suffix match), the
   * confidence drops and the caller routes to AI fallback for ambiguous
   * target resolution.
   *
   * Class-shape per CLAUDE.md: the suffix list (_id, _uid, _fk, _key, _ref,
   * _code, _email) is hardcoded BECAUSE it's a platform invariant — the set
   * of canonical FK shapes we recognize, not user-defined data. Industry-
   * agnostic (works for lab science, finance, manufacturing). Language-
   * agnostic on shape detection (UUID/email patterns are universal).
   */
  fkCandidate?: FkCandidate;
}

/**
 * FK candidate metadata. Populated by `detectFkShape`. Consumers (column-
 * mapper.ts pre-pass, cube generator Stage A.5) read this field to decide
 * whether to auto-emit UniversalRelationship rows or queue for AI fallback.
 */
export interface FkCandidate {
  /** 0..1 — how confident we are this column is an FK. >= 0.85 → auto-emit; 0.60-0.85 → AI fallback; < 0.60 → ignore. */
  confidence: number;
  /** Which structural signal(s) fired. */
  signal:
    | 'uuid_shape'           // values match UUID regex
    | 'email_shape'          // values match email regex
    | 'integer_id_shape'     // integer column with high uniqueness + name suffix
    | 'short_code_shape'     // short string column with high uniqueness + name suffix
    | 'name_suffix_only'     // name suffix but no clear value shape (e.g. mixed)
    | 'mixed';
  /**
   * Inferred target entity (extracted from the column-name prefix).
   * E.g. `person_email` → "person.email", `incident_id` → "incident.id".
   * Caller verifies this against existing concepts before auto-emit;
   * if not found, AI fallback or auto-PK-create may apply.
   */
  suggestedTarget?: string;
  /** Plain-English reasoning for audit/observability. */
  reasoning: string;
}

/** Zod schema for runtime validation when reading profiles back from JSONB. */
export const ValueProfileSchema: z.ZodType<ValueProfile> = z.object({
  primaryType: z.enum(['string', 'number', 'integer', 'date', 'datetime', 'boolean', 'mixed']),
  enumValues: z.array(z.string()).optional(),
  patterns: z
    .object({
      isDate: z.boolean().optional(),
      isDatetime: z.boolean().optional(),
      isEmail: z.boolean().optional(),
      isUuid: z.boolean().optional(),
      isCurrency: z.boolean().optional(),
      isPercent: z.boolean().optional(),
      isUrl: z.boolean().optional(),
      isPhone: z.boolean().optional(),
      customRegex: z.string().optional(),
    })
    .optional(),
  nullRate: z.number().min(0).max(1),
  uniqueRate: z.number().min(0).max(1),
  sampleSize: z.number().int().nonnegative(),
  numericRange: z.object({ min: z.number(), max: z.number() }).optional(),
  lengthRange: z.object({ min: z.number().int(), max: z.number().int() }).optional(),
  invisibleCharRate: z.number().min(0).max(1).optional(),
  emissionStrategy: z
    .object({
      kind: z.enum([
        'canonical',
        'us_date_mdy',
        'us_date_mdy_dash',
        'eu_date_dmy',
        'eu_date_dmy_slash',
        'text_date_natural',
        'currency_prefixed',
        'percentage_string',
        'thousand_separated',
        'boolean_text',
        'unknown',
      ]),
      sentinelValues: z.array(z.string()).optional(),
      formatMask: z.string().optional(),
      confidence: z.number().min(0).max(1).optional(),
      detectedVia: z.enum(['deterministic', 'ai_fallback', 'cache']).optional(),
    })
    .optional(),
  fkCandidate: z
    .object({
      confidence: z.number().min(0).max(1),
      signal: z.enum([
        'uuid_shape',
        'email_shape',
        'integer_id_shape',
        'short_code_shape',
        'name_suffix_only',
        'mixed',
      ]),
      suggestedTarget: z.string().optional(),
      reasoning: z.string(),
    })
    .optional(),
});

// ─────────────────────────────────────────────────────────────────────────────
// Pattern detectors (deterministic)
// ─────────────────────────────────────────────────────────────────────────────

const RE_DATE_ISO = /^\d{4}-\d{2}-\d{2}$/;
const RE_DATE_US = /^\d{1,2}\/\d{1,2}\/\d{2,4}$/;
const RE_DATE_DMY = /^\d{1,2}-[A-Za-z]{3,}-\d{2,4}$/;
const RE_DATETIME_ISO = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?/;
const RE_EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
const RE_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const RE_CURRENCY = /^[$€£¥]?\s?-?\d{1,3}(,\d{3})*(\.\d+)?(\s?(USD|EUR|GBP|JPY|CAD|AUD))?$/i;
const RE_PERCENT = /^-?\d+(\.\d+)?\s?%$/;
const RE_URL = /^https?:\/\/[^\s]+$/i;
const RE_PHONE = /^[+(]?[\d\s\-().]{7,}$/;
const RE_INTEGER = /^-?\d+$/;
const RE_NUMBER = /^-?\d+(\.\d+)?$/;
const RE_BOOLEAN = /^(true|false|yes|no|t|f|y|n|1|0)$/i;

// ─────────────────────────────────────────────────────────────────────────────
// Emission-strategy format detectors
// ─────────────────────────────────────────────────────────────────────────────
// Detect the FORMAT SHAPE of a value (regex), not specific values. Class-shaped
// per CLAUDE.md: works for any tenant in any language with any date convention.
// Non-matching formats go to AI fallback (cached globally).

/** Slash-delimited dates, ambiguous between US M/D/YYYY and EU D/M/YYYY. */
const RE_FMT_DATE_SLASH = /^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/;
/** Dash-delimited dates, US M-D-YYYY. */
const RE_FMT_DATE_DASH = /^(\d{1,2})-(\d{1,2})-(\d{2,4})$/;
/** Dot-delimited dates, EU D.M.YYYY. */
const RE_FMT_DATE_DOT = /^(\d{1,2})\.(\d{1,2})\.(\d{2,4})$/;
/** Natural-language dates: "Aug 4, 2025", "4 Aug 2025", "August 4, 2025". */
const RE_FMT_DATE_NATURAL = /^(\d{1,2}\s+)?[A-Za-z]{3,9},?\s+\d{1,2},?\s+\d{2,4}$|^[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{2,4}$/;
/** Currency with leading or trailing symbol/code. */
const RE_FMT_CURRENCY_PREFIXED = /^[$€£¥₹]\s?-?\d{1,3}(,\d{3})*(\.\d+)?$/;
const RE_FMT_CURRENCY_TRAILING = /^-?\d{1,3}(,\d{3})*(\.\d+)?\s?[€£¥₹$]$|^-?\d{1,3}(,\d{3})*(\.\d+)?\s?(USD|EUR|GBP|JPY|CAD|AUD|CHF|INR)$/i;
/** Percentage strings like "12.5%" or "-0.5 %". */
const RE_FMT_PERCENTAGE_STRING = /^-?\d+(\.\d+)?\s?%$/;
/** Numbers with comma thousand separators: "1,234,567" or "1,234.56". */
const RE_FMT_THOUSAND_SEPARATED = /^-?\d{1,3}(,\d{3})+(\.\d+)?$/;
/** Boolean-as-text values. */
const RE_FMT_BOOLEAN_TEXT = /^(yes|no|y|n|true|false|t|f)$/i;

/**
 * English/Western default sentinel values. Deterministic-first; novel
 * placeholders in other languages (Spanish "sin datos", German "unbekannt",
 * Japanese "不明") get routed to AI fallback in detect-emission-strategy.ts
 * and cached globally via AIClassificationCache. Per CLAUDE.md, the
 * enumerated list is intentionally narrow + AI-extendable, not a
 * substitute for AI.
 */
const DEFAULT_SENTINEL_LITERALS = new Set<string>([
  'unknown',
  'n/a',
  'na',
  'tbd',
  'tbc',
  '—',
  '-',
  '?',
  '??',
  'null',
  'none',
  'pending',
  'unspecified',
  'unset',
  'undefined',
  'not applicable',
  'not available',
  'no data',
  'no value',
]);

/**
 * Detect whether a single sample value looks like a sentinel/placeholder
 * after lowercase + trim. Pure-deterministic; the AI fallback layer extends
 * this set on demand.
 */
function isSentinelDefault(s: string): boolean {
  return DEFAULT_SENTINEL_LITERALS.has(s.toLowerCase().trim());
}

/** Classify a single non-null value into a coarse type tag. */
function classifyValue(v: unknown): {
  type: PrimaryType;
  patterns: Set<keyof ValuePatterns>;
  asString: string;
} {
  const patterns = new Set<keyof ValuePatterns>();

  if (typeof v === 'boolean') {
    return { type: 'boolean', patterns, asString: String(v) };
  }
  if (typeof v === 'number') {
    return {
      type: Number.isInteger(v) ? 'integer' : 'number',
      patterns,
      asString: String(v),
    };
  }
  if (v instanceof Date) {
    return { type: 'datetime', patterns: new Set<keyof ValuePatterns>(['isDatetime']), asString: v.toISOString() };
  }

  const s = typeof v === 'string' ? v.trim() : JSON.stringify(v);

  if (RE_DATETIME_ISO.test(s)) {
    patterns.add('isDatetime');
    return { type: 'datetime', patterns, asString: s };
  }
  if (RE_DATE_ISO.test(s) || RE_DATE_US.test(s) || RE_DATE_DMY.test(s)) {
    patterns.add('isDate');
    return { type: 'date', patterns, asString: s };
  }
  if (RE_EMAIL.test(s)) patterns.add('isEmail');
  if (RE_UUID.test(s)) patterns.add('isUuid');
  if (RE_URL.test(s)) patterns.add('isUrl');
  if (RE_CURRENCY.test(s) && /\d/.test(s)) patterns.add('isCurrency');
  if (RE_PERCENT.test(s)) patterns.add('isPercent');
  if (RE_PHONE.test(s) && /\d{7,}/.test(s.replace(/\D/g, ''))) patterns.add('isPhone');

  if (RE_INTEGER.test(s)) return { type: 'integer', patterns, asString: s };
  if (RE_NUMBER.test(s)) return { type: 'number', patterns, asString: s };
  if (RE_BOOLEAN.test(s) && s.length <= 5) return { type: 'boolean', patterns, asString: s };

  return { type: 'string', patterns, asString: s };
}

// ─────────────────────────────────────────────────────────────────────────────
// Profile builder
// ─────────────────────────────────────────────────────────────────────────────

/** Treat these as null/empty for profiling purposes. */
function isNullish(v: unknown): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === 'string' && v.trim() === '') return true;
  return false;
}

/**
 * Build a ValueProfile from a sample of column values.
 *
 * Deterministic (no AI call). Used by AI #1 column-mapper after it gathers
 * samples for a column. Can also be re-built later from a fresh sample
 * during re-mapping or schema refresh.
 *
 * @param samples — raw column values (typically 30-200 rows)
 * @returns Profile capturing type, enum (if low-cardinality), patterns, ranges
 */
export function buildValueProfile(samples: unknown[]): ValueProfile {
  const sampleSize = samples.length;
  if (sampleSize === 0) {
    return { primaryType: 'string', nullRate: 1, uniqueRate: 0, sampleSize: 0 };
  }

  const nonNull = samples.filter((v) => !isNullish(v));
  const nullRate = (sampleSize - nonNull.length) / sampleSize;

  if (nonNull.length === 0) {
    return { primaryType: 'string', nullRate, uniqueRate: 0, sampleSize };
  }

  // Classify each non-null value
  const typeCounts: Record<PrimaryType, number> = {
    string: 0,
    number: 0,
    integer: 0,
    date: 0,
    datetime: 0,
    boolean: 0,
    mixed: 0,
  };
  const patternCounts: Record<keyof ValuePatterns, number> = {
    isDate: 0,
    isDatetime: 0,
    isEmail: 0,
    isUuid: 0,
    isCurrency: 0,
    isPercent: 0,
    isUrl: 0,
    isPhone: 0,
    customRegex: 0,
  };
  const distinct = new Set<string>();
  const stringLengths: number[] = [];
  const numericValues: number[] = [];

  for (const v of nonNull) {
    const { type, patterns, asString } = classifyValue(v);
    typeCounts[type]++;
    for (const p of patterns) patternCounts[p]++;
    distinct.add(asString);

    if (type === 'integer' || type === 'number') {
      const n = Number(asString);
      if (!Number.isNaN(n)) numericValues.push(n);
    } else {
      stringLengths.push(asString.length);
    }
  }

  // Determine primary type: the most common type, with date/integer/number
  // collapsing to their broader class when mixed (e.g., integer + number → number).
  const sortedTypes = Object.entries(typeCounts)
    .filter(([, c]) => c > 0)
    .sort((a, b) => b[1] - a[1]);
  let primaryType: PrimaryType = (sortedTypes[0]?.[0] as PrimaryType) || 'string';

  // Collapse integer + number to number
  if (typeCounts.integer > 0 && typeCounts.number > 0) {
    primaryType = 'number';
  }
  // If date + datetime mixed, prefer datetime
  if (typeCounts.date > 0 && typeCounts.datetime > 0) {
    primaryType = 'datetime';
  }
  // If 2+ types each cover >20% of values, mark as mixed
  const top = sortedTypes[0];
  const second = sortedTypes[1];
  if (top && second && second[1] / nonNull.length > 0.2 && top[1] / nonNull.length < 0.8) {
    // Allow integer/number siblings and date/datetime siblings to NOT count as mixed
    const collapsible =
      (top[0] === 'integer' && second[0] === 'number') ||
      (top[0] === 'number' && second[0] === 'integer') ||
      (top[0] === 'date' && second[0] === 'datetime') ||
      (top[0] === 'datetime' && second[0] === 'date');
    if (!collapsible) primaryType = 'mixed';
  }

  // Enum detection: low cardinality + high coverage
  const distinctCount = distinct.size;
  let enumValues: string[] | undefined;
  if (
    distinctCount > 0 &&
    distinctCount <= 30 &&
    distinctCount / nonNull.length <= 0.5 && // each value used ≥ 2× on average
    primaryType !== 'mixed' &&
    primaryType !== 'datetime' &&
    primaryType !== 'date' // dates are typically not enums even when low-cardinality in tiny samples
  ) {
    enumValues = Array.from(distinct).sort();
  }

  // Patterns: include any pattern that holds for ≥ 80% of non-null values
  const patterns: ValuePatterns = {};
  for (const [name, count] of Object.entries(patternCounts) as Array<[keyof ValuePatterns, number]>) {
    if (name === 'customRegex') continue;
    if (count / nonNull.length >= 0.8) patterns[name] = true;
  }

  const uniqueRate = distinctCount / nonNull.length;

  const profile: ValueProfile = {
    primaryType,
    nullRate,
    uniqueRate,
    sampleSize,
  };
  if (enumValues) profile.enumValues = enumValues;
  if (Object.keys(patterns).length > 0) profile.patterns = patterns;
  if (numericValues.length > 0) {
    profile.numericRange = {
      min: Math.min(...numericValues),
      max: Math.max(...numericValues),
    };
  }
  if (stringLengths.length > 0) {
    profile.lengthRange = {
      min: Math.min(...stringLengths),
      max: Math.max(...stringLengths),
    };
  }

  // Character-class normalization probe — captures fraction of string samples
  // carrying invisible-char / look-alike Unicode corruption. Only persisted
  // when nonzero to keep clean profiles compact.
  const { invisibleCharRate } = probeInvisibleCharRate(nonNull);
  if (invisibleCharRate > 0) {
    profile.invisibleCharRate = invisibleCharRate;
  }

  // Emission strategy — deterministic detection of how the cube generator
  // should parse text values into typed concepts. Structural format
  // matching on regex SHAPES (not value enumeration), so it works for any
  // tenant in any language. Novel formats fall through to 'unknown' and
  // the AI fallback layer (in detect-emission-strategy.ts) routes them to
  // AIClassificationCache for global per-format learning.
  const strategy = detectEmissionStrategyDeterministic(nonNull);
  if (strategy.kind !== 'canonical' || (strategy.sentinelValues && strategy.sentinelValues.length > 0)) {
    profile.emissionStrategy = strategy;
  }

  return profile;
}

// ─────────────────────────────────────────────────────────────────────────────
// Emission-strategy detection
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Deterministic emission-strategy detection. Inspects sample shapes (not
 * specific values) to determine the appropriate parsing approach for the
 * cube generator.
 *
 * Returns `kind: 'unknown'` when the format doesn't match any known
 * pattern — caller (detect-emission-strategy-ai.ts) can route to AI
 * fallback for novel formats. Always populates `sentinelValues` when
 * detected, regardless of inner kind.
 *
 * Class-shaped properties (CLAUDE.md):
 *   - Industry-agnostic: structural regex matching, no value enumeration
 *   - Concept-agnostic: returns a strategy for any sample set
 *   - Language-agnostic on date/number patterns (digits are universal);
 *     sentinel defaults are English/Western with AI fallback elsewhere
 *   - Forward-compat: new formats route to AI without code change
 */
export function detectEmissionStrategyDeterministic(
  samples: unknown[],
): EmissionStrategy {
  if (samples.length === 0) {
    return { kind: 'canonical', confidence: 1.0, detectedVia: 'deterministic' };
  }

  // Separate sentinels from real values — sentinels don't drive format
  // classification, they just mean the strategy needs CASE WHEN wrapping.
  const sentinels = new Set<string>();
  const realSamples: string[] = [];
  for (const v of samples) {
    if (isNullish(v)) continue;
    const s = typeof v === 'string' ? v.trim() : String(v);
    if (isSentinelDefault(s)) {
      sentinels.add(s);
    } else {
      realSamples.push(s);
    }
  }

  if (realSamples.length === 0) {
    // All real values are sentinels — column has no usable data
    return {
      kind: 'canonical',
      sentinelValues: Array.from(sentinels),
      confidence: 1.0,
      detectedVia: 'deterministic',
    };
  }

  // Count which format SHAPE each sample matches. The dominant shape wins
  // (≥80% threshold to be confident; below that we route to AI).
  const shapeCounts: Record<EmissionStrategyKind, number> = {
    canonical: 0,
    us_date_mdy: 0,
    us_date_mdy_dash: 0,
    eu_date_dmy: 0,
    eu_date_dmy_slash: 0,
    text_date_natural: 0,
    currency_prefixed: 0,
    percentage_string: 0,
    thousand_separated: 0,
    boolean_text: 0,
    unknown: 0,
  };

  // First pass: detect unambiguous EU-slash signal (any sample with first
  // field > 12 — US never has month > 12, so this proves EU locale).
  let hasUnambiguousEUSlash = false;
  for (const s of realSamples) {
    const m = RE_FMT_DATE_SLASH.exec(s);
    if (m && parseInt(m[1], 10) > 12) {
      hasUnambiguousEUSlash = true;
      break;
    }
  }

  for (const s of realSamples) {
    if (RE_DATE_ISO.test(s) || RE_DATETIME_ISO.test(s) || RE_INTEGER.test(s) || RE_NUMBER.test(s)) {
      shapeCounts.canonical++;
    } else if (RE_FMT_DATE_SLASH.test(s)) {
      // Slash dates are ambiguous between US M/D/YYYY and EU D/M/YYYY.
      // Resolve via structural signal: if ANY sample in the column has
      // first field > 12, ALL slash-shaped samples must be EU (consistent
      // format within a column). Otherwise default to US (more common
      // globally). AI fallback can override if context suggests EU
      // locale even without month-disambiguator evidence.
      if (hasUnambiguousEUSlash) {
        shapeCounts.eu_date_dmy_slash++;
      } else {
        shapeCounts.us_date_mdy++;
      }
    } else if (RE_FMT_DATE_DASH.test(s)) {
      shapeCounts.us_date_mdy_dash++;
    } else if (RE_FMT_DATE_DOT.test(s)) {
      shapeCounts.eu_date_dmy++;
    } else if (RE_FMT_DATE_NATURAL.test(s)) {
      shapeCounts.text_date_natural++;
    } else if (RE_FMT_PERCENTAGE_STRING.test(s)) {
      shapeCounts.percentage_string++;
    } else if (RE_FMT_CURRENCY_PREFIXED.test(s) || RE_FMT_CURRENCY_TRAILING.test(s)) {
      shapeCounts.currency_prefixed++;
    } else if (RE_FMT_THOUSAND_SEPARATED.test(s)) {
      shapeCounts.thousand_separated++;
    } else if (RE_FMT_BOOLEAN_TEXT.test(s)) {
      shapeCounts.boolean_text++;
    } else {
      shapeCounts.unknown++;
    }
  }

  // Find the dominant shape ≥ 80% of real samples
  let dominantKind: EmissionStrategyKind = 'unknown';
  let dominantCount = 0;
  for (const [kind, count] of Object.entries(shapeCounts) as Array<[EmissionStrategyKind, number]>) {
    if (count > dominantCount) {
      dominantKind = kind;
      dominantCount = count;
    }
  }

  const coverage = dominantCount / realSamples.length;
  const result: EmissionStrategy = {
    kind: coverage >= 0.8 ? dominantKind : 'unknown',
    confidence: coverage,
    detectedVia: 'deterministic',
  };
  if (sentinels.size > 0) {
    result.sentinelValues = Array.from(sentinels);
  }
  return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// Value-vs-profile match scoring (used by realignment engine)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Score how well a single value matches a column's profile.
 *
 * Returns a number in [0, 1]:
 *   1.0 = clear match (type + enum + patterns all align)
 *   0.5 = neutral (null in a column that tolerates nulls, or ambiguous)
 *   0.0 = clear mismatch (wrong type, not in enum, fails patterns)
 *
 * Used by the realignment engine to compare alignment quality across shift
 * candidates. The engine sums scores across all columns for each candidate
 * and picks the highest-scoring alignment.
 */
export function scoreValueAgainstProfile(value: unknown, profile: ValueProfile): number {
  // Null handling — neutral if column tolerates nulls, mismatch if it doesn't
  if (isNullish(value)) {
    if (profile.nullRate >= 0.05) return 0.7; // nulls are normal here
    return 0.3; // suspicious — column is usually populated
  }

  const { type: vType, patterns: vPatterns, asString } = classifyValue(value);

  let score = 0.5; // start neutral

  // Type match
  const profileType = profile.primaryType;
  const typeMatch =
    vType === profileType ||
    (vType === 'integer' && profileType === 'number') ||
    (vType === 'number' && profileType === 'integer') ||
    (vType === 'date' && profileType === 'datetime') ||
    (vType === 'datetime' && profileType === 'date') ||
    profileType === 'mixed';

  if (typeMatch) {
    score = 0.7;
  } else if (profileType === 'string' && vType === 'string') {
    score = 0.7;
  } else {
    // Clear type mismatch — but soften if profile is string (strings are forgiving)
    if (profileType === 'string') {
      score = 0.5;
    } else {
      score = 0.1;
    }
  }

  // Enum membership — if profile has an enum, value must be in it
  if (profile.enumValues && profile.enumValues.length > 0) {
    const inEnum = profile.enumValues.includes(asString);
    if (inEnum) {
      score = Math.max(score, 0.95);
    } else {
      // Out-of-enum is a strong signal, especially for narrow enums
      if (profile.enumValues.length <= 10) {
        score = Math.min(score, 0.1);
      } else {
        score = Math.min(score, 0.3);
      }
    }
  }

  // Pattern match — if profile has any pattern flags, check the value
  if (profile.patterns) {
    let patternMatchCount = 0;
    let patternCheckCount = 0;
    for (const [name, expected] of Object.entries(profile.patterns) as Array<[keyof ValuePatterns, boolean | string | undefined]>) {
      if (name === 'customRegex' || !expected) continue;
      patternCheckCount++;
      if (vPatterns.has(name)) patternMatchCount++;
    }
    if (patternCheckCount > 0) {
      const patternScore = patternMatchCount / patternCheckCount;
      // Pattern check is a hard signal — boost or penalize accordingly
      if (patternScore >= 0.8) {
        score = Math.max(score, 0.9);
      } else if (patternScore === 0) {
        score = Math.min(score, 0.2);
      }
    }
  }

  // Numeric range plausibility (loose — values within 100× the observed range are OK)
  if (profile.numericRange && (vType === 'number' || vType === 'integer')) {
    const n = Number(asString);
    if (!Number.isNaN(n)) {
      const { min, max } = profile.numericRange;
      const span = max - min || Math.abs(max) || 1;
      const padded = { lo: min - span * 100, hi: max + span * 100 };
      if (n < padded.lo || n > padded.hi) {
        score = Math.min(score, 0.4);
      }
    }
  }

  return Math.max(0, Math.min(1, score));
}

/**
 * Score a batch of samples against a single profile — how well does this
 * column of values fit the shape this concept is known to take?
 *
 * Used by AI #1's cross-concept fit-warning system: when mapping column C
 * to proposed concept P, also score C's samples against the profiles of
 * sibling concepts (same field, different entity — e.g., `employee.name`
 * when proposing `vendor.name`). If a sibling profile fits substantially
 * better than the proposed concept's profile, flag a fit conflict so a
 * human can confirm or correct the mapping.
 *
 * Returns the AVERAGE per-value score in [0, 1]. Null values use the
 * profile's null-tolerance (consistent with scoreValueAgainstProfile).
 */
export function scoreSamplesAgainstProfile(
  samples: unknown[],
  profile: ValueProfile
): number {
  if (samples.length === 0) return 0.5;
  let total = 0;
  for (const v of samples) total += scoreValueAgainstProfile(v, profile);
  return total / samples.length;
}

// ─────────────────────────────────────────────────────────────────────────────
// FK-candidate detection (Item B, 2026-06-03)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Canonical FK name-suffix regex. The captured groups are:
 *   1: the prefix (the inferred target entity name)
 *   2: the suffix (id|uid|fk|key|ref|code|email)
 *
 * Order-of-precedence matters in the alternation: longer/more-specific
 * suffixes first so e.g. `employee_email` matches as suffix=email (FK to
 * person/user entity) rather than splitting wrong.
 *
 * Class-shape per CLAUDE.md: this suffix set is a PLATFORM INVARIANT — the
 * set of canonical FK shapes we recognize is owned by the platform, not
 * user-defined. Hardcoded list is correct here. Industry/language-
 * agnostic in practice because tenants overwhelmingly use English column
 * suffixes; FK-suffix patterns in other languages (e.g. `_kennung`,
 * `_référence`) are ambiguous-target cases that defer to AI fallback (C2).
 */
const RE_FK_NAME_SUFFIX = /^(.+)_(id|uid|fk|key|ref|code|email)$/i;

/**
 * Detect whether a column is structurally an FK candidate.
 *
 * Compound rule (all three signals together → high confidence; subsets →
 * lower confidence with AI fallback path):
 *
 *   1. Name signal: column name matches RE_FK_NAME_SUFFIX
 *   2. Shape signal: value type + pattern (uuid/email/integer/short-code)
 *   3. Uniqueness signal: profile.uniqueRate >= 0.70 (FKs to high-cardinality
 *      entities; low uniqueness is enum-ish like a status code)
 *
 * Returns undefined for columns that don't carry any FK signal (free text,
 * numeric measures, low-cardinality enums without FK-shape names).
 *
 * Targets are inferred from the name prefix:
 *   - `_id`/`_uid` → suggestedTarget = `${prefix}.id`
 *   - `_email`     → suggestedTarget = `${prefix}.email`
 *   - `_code`/`_key`/`_ref`/`_fk` → suggestedTarget = `${prefix}.${suffix}`
 *
 * Examples:
 *   - column "person_email", uuid-free, email-shaped, uniqueRate=0.85
 *     → confidence ~0.85, signal 'email_shape', suggestedTarget 'person.email'
 *   - column "incident_id", UUID-shaped, uniqueRate=0.99
 *     → confidence ~0.95, signal 'uuid_shape', suggestedTarget 'incident.id'
 *   - column "code" (no prefix) with UUID values, uniqueRate=0.99
 *     → confidence ~0.60, signal 'uuid_shape', suggestedTarget undefined
 *       (caller routes to AI fallback for target resolution)
 *   - column "description" (long text, uniqueRate=0.4)
 *     → returns undefined (free text — not a FK)
 *   - column "status" (5 enum values, uniqueRate=0.05)
 *     → returns undefined (status code, not an FK)
 *   - column "id" (no prefix) — that's the PK of THIS row, not a FK
 *     → returns undefined (suffix-only match, no prefix to extract)
 */
export function detectFkShape(
  columnName: string,
  profile: ValueProfile,
): FkCandidate | undefined {
  if (!columnName) return undefined;

  const trimmed = columnName.trim();
  const suffixMatch = RE_FK_NAME_SUFFIX.exec(trimmed);

  // Step 1: extract name prefix + suffix (if any)
  const namePrefix = suffixMatch?.[1] ?? null;
  const nameSuffix = suffixMatch?.[2]?.toLowerCase() ?? null;
  const hasNameSignal = Boolean(namePrefix && nameSuffix);

  // Step 2: detect value shape signal
  const patterns = profile.patterns ?? {};
  const isUuidShaped = Boolean(patterns.isUuid);
  const isEmailShaped = Boolean(patterns.isEmail);
  const isInteger = profile.primaryType === 'integer';
  const lenMax = profile.lengthRange?.max ?? 0;
  // Short-code shape: short strings, NOT date/email/url shaped, NOT mixed
  const isShortCode =
    profile.primaryType === 'string' &&
    !patterns.isDate &&
    !patterns.isDatetime &&
    !patterns.isEmail &&
    !patterns.isUrl &&
    !patterns.isUuid &&
    lenMax > 0 &&
    lenMax <= 64;

  let shapeSignal:
    | 'uuid_shape'
    | 'email_shape'
    | 'integer_id_shape'
    | 'short_code_shape'
    | null = null;
  if (isUuidShaped) shapeSignal = 'uuid_shape';
  else if (isEmailShaped) shapeSignal = 'email_shape';
  else if (isInteger) shapeSignal = 'integer_id_shape';
  else if (isShortCode) shapeSignal = 'short_code_shape';

  // Step 3: uniqueness signal
  const uniqueness = profile.uniqueRate ?? 0;
  const hasUniquenessSignal = uniqueness >= 0.7;

  // Reject obvious non-FK cases early
  // - Mixed type → not an FK (FKs are single-typed)
  // - Free-text shape (long strings, low uniqueness) → not an FK
  if (profile.primaryType === 'mixed') return undefined;
  if (lenMax > 64 && uniqueness < 0.7) return undefined;
  // - Low uniqueness + small enum → status/category code, not an FK
  if (profile.enumValues && profile.enumValues.length > 0 && uniqueness < 0.1) {
    return undefined;
  }
  // - No signals at all → return undefined
  if (!hasNameSignal && !shapeSignal) return undefined;

  // Compose confidence + signal + target
  let confidence = 0;
  let signal: FkCandidate['signal'] = shapeSignal ?? 'name_suffix_only';
  const reasoningParts: string[] = [];

  if (hasNameSignal && shapeSignal && hasUniquenessSignal) {
    // All three signals: high confidence
    confidence = 0.95;
    signal = shapeSignal;
    reasoningParts.push(`name suffix '_${nameSuffix}'`, `${shapeSignal}`, `uniqueRate=${uniqueness.toFixed(2)}`);
  } else if (hasNameSignal && shapeSignal) {
    // Name + shape but uniqueness weak: still strong
    confidence = 0.85;
    signal = shapeSignal;
    reasoningParts.push(`name suffix '_${nameSuffix}'`, `${shapeSignal}`, `uniqueRate=${uniqueness.toFixed(2)} (below 0.7 — weak uniqueness)`);
  } else if (hasNameSignal && hasUniquenessSignal) {
    // Name + uniqueness but no clear shape (e.g. mixed integers/strings)
    confidence = 0.75;
    signal = 'name_suffix_only';
    reasoningParts.push(`name suffix '_${nameSuffix}'`, `uniqueRate=${uniqueness.toFixed(2)}`, 'no clear value shape');
  } else if (hasNameSignal) {
    // Name alone — needs AI to confirm target
    confidence = 0.65;
    signal = 'name_suffix_only';
    reasoningParts.push(`name suffix '_${nameSuffix}'`, 'no value shape or uniqueness signal');
  } else if (shapeSignal && hasUniquenessSignal) {
    // Shape + uniqueness but no name suffix (e.g. column called "ref" with UUIDs)
    confidence = 0.60;
    signal = shapeSignal;
    reasoningParts.push(`no FK suffix in name`, `${shapeSignal}`, `uniqueRate=${uniqueness.toFixed(2)}`);
  } else {
    // Shape only (e.g. UUID values in a "notes" column — false positive risk)
    confidence = 0.5;
    signal = shapeSignal as FkCandidate['signal'];
    reasoningParts.push(`shape signal only (${shapeSignal})`, `no name suffix or uniqueness — likely false positive`);
  }

  // Below the AI-fallback floor → don't return. Caller would only ignore.
  if (confidence < 0.6) return undefined;

  // Compute suggestedTarget if we have a name prefix
  let suggestedTarget: string | undefined;
  if (namePrefix && nameSuffix) {
    if (nameSuffix === 'id' || nameSuffix === 'uid') {
      suggestedTarget = `${namePrefix.toLowerCase()}.id`;
    } else if (nameSuffix === 'email') {
      suggestedTarget = `${namePrefix.toLowerCase()}.email`;
    } else {
      // code/key/ref/fk — point at the same field on the target entity
      suggestedTarget = `${namePrefix.toLowerCase()}.${nameSuffix}`;
    }
  }

  return {
    confidence,
    signal,
    suggestedTarget,
    reasoning: reasoningParts.join('; '),
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// FK detection threshold constants (Item B P2 backlog, 2026-06-03)
// ─────────────────────────────────────────────────────────────────────────────
//
// Previously these thresholds were bare numeric literals scattered across
// column-mapper.ts (C2 synthesis loop) and cube/generate/route.ts (C3 Stage
// A.5 widening). The "drift" between sites — C2 used 0.60 and 0.75, C3 used
// 0.70 — wasn't a bug per se (each threshold reflected a different concern:
// cost gate, AI accept, structural gate), but the LACK OF NAMED CONSTANTS
// meant a future PR raising one without raising the others would silently
// break the ordering invariant.
//
// This block names each threshold + documents its purpose + the ordering
// invariant they must satisfy. Test file at integrity/__tests__/fk-thresholds.test.ts
// pins the invariant — any future change that breaks the ordering fails CI.
//
// ORDERING INVARIANT (load-bearing — see fk-thresholds.test.ts):
//   FK_DETECTION_FLOOR (0.60) <= FK_SYNTHESIS_AI_FALLBACK (0.60)
//     <= FK_CUBE_WIDENING (0.70)
//     <= FK_SYNTHESIS_DIRECT (0.75)
//     <= FK_AI_ACCEPT (0.75)
//
// Class-shape per CLAUDE.md: named constants + ordering test catch the class
// of "threshold drift" bug structurally, without enumerating specific values.
// Future changes to any threshold MUST keep the invariant or the test fails.

/**
 * Minimum confidence detectFkShape returns. Below this, detectFkShape returns
 * undefined (the "false-positive guard floor"). Below this, no consumer ever
 * sees the signal — it's the platform-wide floor.
 */
export const FK_DETECTION_FLOOR = 0.60;

/**
 * Minimum confidence at which C2's synthesis loop in column-mapper.ts will
 * call aiInferRelationshipTarget to resolve an ambiguous suggestedTarget.
 * Below this, the synthesis loop skips entirely (no AI cost incurred).
 * Same as FK_DETECTION_FLOOR — intentionally aligned; documented separately
 * because it's a DIFFERENT concern (cost gate, not detection floor).
 */
export const FK_SYNTHESIS_AI_FALLBACK = 0.60;

/**
 * Minimum confidence at which C3's Stage A.5 widening in
 * apps/web/app/api/cube/generate/route.ts admits an FK candidate for
 * join inference. Below this, the candidate is ignored at cube emission
 * time even if it was synthesized + persisted by C2.
 *
 * This sits ABOVE the AI fallback floor (0.60) because Stage A.5 emits
 * a structural artifact (a cube join) — false positives create visible
 * errors. Higher gate = lower false-positive rate at the join emission
 * surface, at the cost of some near-the-floor candidates not benefitting.
 */
export const FK_CUBE_WIDENING = 0.70;

/**
 * Minimum confidence at which C2's synthesis loop will DIRECTLY synthesize
 * a suggestRelationship from the fkCandidate signal alone (no AI fallback
 * needed). Above this, detectFkShape's structural signal is strong enough
 * to skip the AI call entirely.
 *
 * Between FK_SYNTHESIS_AI_FALLBACK (0.60) and FK_SYNTHESIS_DIRECT (0.75)
 * is the AI-fallback band: structural signal is suggestive but not
 * conclusive, so we call aiInferRelationshipTarget.
 */
export const FK_SYNTHESIS_DIRECT = 0.75;

/**
 * Minimum AI confidence at which we accept an aiInferRelationshipTarget
 * result and synthesize a relationship from it. Below this, the AI's
 * answer is treated as uncertain and the synthesis is skipped.
 *
 * Aligned with FK_SYNTHESIS_DIRECT because both gates represent the same
 * underlying concept ("we're confident enough to write a UniversalRelationship
 * row"), just from different sources (structural detection vs AI inference).
 */
export const FK_AI_ACCEPT = 0.75;
