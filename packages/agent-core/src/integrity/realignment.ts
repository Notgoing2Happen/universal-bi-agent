/**
 * Row Realignment Engine — column-shift defense.
 *
 * Given a row and per-column value profiles (built by AI #1), detect rows
 * where values landed in the wrong columns due to source-data column shifts
 * (extra commas in CSV, mis-aligned Excel cells, swapped fields in API
 * payloads, etc.) and propose a realignment.
 *
 * Algorithm (deterministic, no AI call at runtime):
 *   1. Score the original row against profiles
 *   2. If alignment score ≥ threshold → row is clean, pass through
 *   3. Otherwise try shift candidates (−3..+3 positions)
 *   4. For each shift, build hypothetical realigned row and score it
 *   5. If the best shift score clearly dominates the original → auto-realign
 *   6. Otherwise → quarantine with diagnostic info
 *
 * Transport-agnostic: works on Record<string, unknown> rows, so the same
 * engine plugs into local-file sync, NangoDriver pass-through, agent
 * passthrough, and any other entry point.
 *
 * Design principle: AI builds the rules once (column profiles via AI #1);
 * deterministic code applies them at scale. Matches the AI #4 self-healing
 * pattern already in production.
 */
import { scoreValueAgainstProfile, type ValueProfile } from './value-profile';

// ─────────────────────────────────────────────────────────────────────────────
// Types
// ─────────────────────────────────────────────────────────────────────────────

export interface AlignmentCheckInput {
  /** The row to check (keys = source column names) */
  row: Record<string, unknown>;
  /** Source columns in their positional order (matches the source header order) */
  columnOrder: string[];
  /** Value profile per source column (keyed by source column name). Columns without profiles are skipped. */
  profiles: Record<string, ValueProfile | undefined>;
}

export interface AlignmentDiagnostic {
  column: string;
  value: unknown;
  score: number;
  reason: string;
}

/**
 * Describes how a realigned row was transformed from the original.
 *
 * Pivot-shift: the value at `pivot` and beyond moved by `shift` positions.
 * Swaps: pairs of columns whose values were transposed. Swaps catch same-type
 * column-shift bugs that pivot-shifts can't (two free-text columns swapped,
 * shared-enum columns transposed, etc.) by routing each value to the column
 * whose profile it actually fits — the cross-column referential-integrity
 * extension to the basic shift search.
 */
export interface AlignmentTransformation {
  /** Shift offset applied at the pivot (0 = no shift) */
  shift: number;
  /** 0-based column index from which the shift applies (0 = whole-row shift) */
  pivot: number;
  /** Column-name pairs whose values were swapped. Empty when no swaps applied. */
  swaps: Array<[string, string]>;
}

export type AlignmentVerdict =
  /** Row matches its profile — no action needed */
  | { kind: 'aligned'; score: number }
  /** Row had issues but a clear transformation fixes them — engine returns the realigned row */
  | {
      kind: 'realigned';
      /** Back-compat: equal to transformation.shift */
      shift: number;
      /** Back-compat: equal to transformation.pivot */
      pivot: number;
      /** Full description of what changed (shift + any swaps) */
      transformation: AlignmentTransformation;
      originalScore: number;
      newScore: number;
      realigned: Record<string, unknown>;
      diagnostics: AlignmentDiagnostic[];
    }
  /** Row has issues and no transformation clearly fixes them — quarantine */
  | {
      kind: 'quarantine';
      originalScore: number;
      diagnostics: AlignmentDiagnostic[];
      bestAttempt?: { shift: number; pivot: number; swaps: Array<[string, string]>; score: number };
    };

export interface RealignmentOptions {
  /** Minimum per-row alignment score to consider the row "clean" (default 0.65) */
  alignedThreshold?: number;
  /** Minimum improvement (new_score - original_score) to apply a shift (default 0.20) */
  improvementThreshold?: number;
  /** Minimum new_score required to apply a shift (default 0.70) */
  applyThreshold?: number;
  /** How many positions to try shifting (default 3 in each direction) */
  maxShift?: number;
  /** Enable cross-column swap search (default true). Disable to skip the O(K²) swap pass. */
  enableSwapSearch?: boolean;
  /** Max number of low-scoring columns to consider for pairwise swaps (default 6 — bounds the K² blowup) */
  maxSwapCandidates?: number;
}

const DEFAULT_OPTIONS: Required<RealignmentOptions> = {
  alignedThreshold: 0.65,
  improvementThreshold: 0.20,
  applyThreshold: 0.70,
  maxShift: 3,
  enableSwapSearch: true,
  maxSwapCandidates: 6,
};

// ─────────────────────────────────────────────────────────────────────────────
// Core scoring
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Score how well a row's values match their column profiles.
 *
 * Returns the average per-column score across columns that have a profile.
 * Columns without a profile contribute a neutral 0.5 (no signal).
 * Returns per-column diagnostics for any column that scored below 0.3.
 */
function scoreRow(
  row: Record<string, unknown>,
  columnOrder: string[],
  profiles: Record<string, ValueProfile | undefined>
): { score: number; problems: AlignmentDiagnostic[] } {
  if (columnOrder.length === 0) return { score: 1, problems: [] };

  let total = 0;
  let scored = 0;
  const problems: AlignmentDiagnostic[] = [];

  for (const col of columnOrder) {
    const profile = profiles[col];
    if (!profile) continue; // no signal for this column
    const value = row[col];
    const cellScore = scoreValueAgainstProfile(value, profile);
    total += cellScore;
    scored++;
    if (cellScore < 0.3) {
      problems.push({
        column: col,
        value,
        score: cellScore,
        reason: describeMismatch(value, profile),
      });
    }
  }

  if (scored === 0) return { score: 0.5, problems: [] };
  return { score: total / scored, problems };
}

function describeMismatch(value: unknown, profile: ValueProfile): string {
  if (value === null || value === undefined || value === '') {
    return `null/empty value in column with nullRate=${profile.nullRate.toFixed(2)}`;
  }
  const reasons: string[] = [];
  if (profile.enumValues && profile.enumValues.length > 0) {
    const inEnum = profile.enumValues.includes(String(value).trim());
    if (!inEnum) {
      reasons.push(`value "${String(value).slice(0, 60)}" not in enum [${profile.enumValues.slice(0, 5).join(', ')}${profile.enumValues.length > 5 ? ', …' : ''}]`);
    }
  }
  if (profile.patterns) {
    const expected = Object.keys(profile.patterns).filter((k) => k !== 'customRegex');
    if (expected.length > 0) reasons.push(`expected pattern: ${expected.join('|')}`);
  }
  if (reasons.length === 0) reasons.push(`type mismatch (profile primaryType=${profile.primaryType})`);
  return reasons.join('; ');
}

// ─────────────────────────────────────────────────────────────────────────────
// Shift generation
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build a hypothetical realigned row by shifting positional column assignments
 * from a pivot point onward.
 *
 * Most real-world column-shift bugs (stray comma in a CSV, dropped cell in
 * Excel) cause a PARTIAL shift — columns before the pivot stay aligned,
 * columns from the pivot onward shift by a constant offset. Whole-row
 * shifts (pivot=0) are also handled.
 *
 * shift=+1 from pivot=P: for columns at positions ≥ P, each receives the
 *   value that was at the NEXT position (i.e., values shift LEFT to recover
 *   from an EXTRA empty field that pushed everything right).
 * shift=−1 from pivot=P: for columns at positions ≥ P, each receives the
 *   value that was at the PREVIOUS position (i.e., values shift RIGHT to
 *   recover from a MISSING field that pulled everything left).
 *
 * Columns before the pivot keep their original values. Out-of-range source
 * positions become null.
 */
function buildShiftedRow(
  row: Record<string, unknown>,
  columnOrder: string[],
  shift: number,
  pivot: number = 0
): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  for (let i = 0; i < columnOrder.length; i++) {
    if (i < pivot) {
      // Before the pivot: keep original value
      result[columnOrder[i]] = row[columnOrder[i]];
      continue;
    }
    const sourceIdx = i + shift;
    const sourceCol = columnOrder[sourceIdx];
    result[columnOrder[i]] = sourceCol !== undefined ? row[sourceCol] : null;
  }
  return result;
}

/**
 * Apply a sequence of pairwise swaps to a row's column values.
 * Each swap = transpose the two named columns' values.
 */
function applySwaps(
  row: Record<string, unknown>,
  swaps: Array<[string, string]>
): Record<string, unknown> {
  if (swaps.length === 0) return row;
  const result = { ...row };
  for (const [a, b] of swaps) {
    const tmp = result[a];
    result[a] = result[b];
    result[b] = tmp;
  }
  return result;
}

/**
 * Cross-column referential-integrity search: find pairs of low-scoring
 * columns whose values would each fit BETTER if swapped.
 *
 * Catches the same-type column-shift class that pivot-shifts can't handle:
 *   - Two free-text columns transposed (Dr. Martin ↔ Project name)
 *   - Two shared-enum columns swapped (approval_status ↔ ship_status)
 *   - A code-like value in a name field and a name in a code field
 *
 * Algorithm:
 *   1. Find the K worst-scoring columns in the row (default K=6)
 *   2. For each pair (A, B) in that set, score the row with A and B swapped
 *   3. Return the best single-swap candidate; if its score beats the
 *      current best by `improvementThreshold`, apply it and recurse
 *      (greedy multi-swap up to maxIterations)
 *
 * Bounded: K=6 means at most 15 pair checks per iteration × 3 iterations = 45
 * score evaluations. Each score is O(N) deterministic comparisons. Still
 * microseconds per row at typical column counts.
 */
function searchSwaps(
  row: Record<string, unknown>,
  columnOrder: string[],
  profiles: Record<string, ValueProfile | undefined>,
  startScore: number,
  startProblems: AlignmentDiagnostic[],
  maxSwapCandidates: number,
  improvementThreshold: number
): { score: number; swaps: Array<[string, string]>; row: Record<string, unknown> } | null {
  // Greedy multi-swap: apply best single swap if it improves, then look for another
  const MAX_ITERATIONS = 3;
  let currentRow = row;
  let currentScore = startScore;
  let currentProblems = startProblems;
  const appliedSwaps: Array<[string, string]> = [];

  for (let iter = 0; iter < MAX_ITERATIONS; iter++) {
    // Pick K lowest-scoring columns as swap candidates
    const candidates = currentProblems
      .slice()
      .sort((a, b) => a.score - b.score)
      .slice(0, maxSwapCandidates)
      .map((p) => p.column);

    if (candidates.length < 2) break;

    let bestPairScore = currentScore;
    let bestPair: [string, string] | null = null;
    let bestRow: Record<string, unknown> | null = null;

    for (let i = 0; i < candidates.length; i++) {
      for (let j = i + 1; j < candidates.length; j++) {
        const a = candidates[i];
        const b = candidates[j];
        const swapped = applySwaps(currentRow, [[a, b]]);
        const { score } = scoreRow(swapped, columnOrder, profiles);
        if (score > bestPairScore) {
          bestPairScore = score;
          bestPair = [a, b];
          bestRow = swapped;
        }
      }
    }

    // Stop when no swap improves materially
    if (!bestPair || !bestRow || bestPairScore - currentScore < improvementThreshold / 4) break;

    appliedSwaps.push(bestPair);
    currentRow = bestRow;
    currentScore = bestPairScore;
    // Recompute problems for next iteration
    const next = scoreRow(currentRow, columnOrder, profiles);
    currentProblems = next.problems;
  }

  if (appliedSwaps.length === 0) return null;
  return { score: currentScore, swaps: appliedSwaps, row: currentRow };
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Check a row's alignment against column profiles and propose a fix if needed.
 *
 * Single-row entry point. For bulk processing, use {@link realignRows}.
 */
export function checkRowAlignment(
  input: AlignmentCheckInput,
  options: RealignmentOptions = {}
): AlignmentVerdict {
  const opts = { ...DEFAULT_OPTIONS, ...options };
  const { row, columnOrder, profiles } = input;

  // 1. Score the original row
  const original = scoreRow(row, columnOrder, profiles);

  // Aligned check uses BOTH a healthy mean score AND a critical-column floor.
  // Without the floor, a row with a few profile-broken cells gets drowned out
  // by many healthy cells and the mean creeps above the threshold — but those
  // broken cells are exactly the same-type-swap or single-cell-misplacement
  // signals we need the swap search to handle. A row is only "aligned" when:
  //   • mean score ≥ alignedThreshold (overall row health)
  //   • AND no column scores below 0.2 (no critically broken cell)
  // The 0.2 floor matches the threshold at which scoreValueAgainstProfile
  // returns a "clear mismatch" — typically enum violation or wrong type.
  const hasCriticallyBrokenColumn = original.problems.some((p) => p.score < 0.2);
  if (original.score >= opts.alignedThreshold && !hasCriticallyBrokenColumn) {
    return { kind: 'aligned', score: original.score };
  }

  // 2. Try shift candidates × pivot points. Most real-world column-shift
  // bugs are PARTIAL: a stray comma in a CSV shifts only the columns after
  // it. Whole-row shifts (pivot=0) are a special case and also tried.
  //
  // Pivot selection: use the FIRST problematic column as the most likely
  // pivot, plus a few neighbors and pivot=0 for completeness. Scanning all
  // N pivots is wasteful when we already know where the misalignment starts.
  const candidatePivots = new Set<number>([0]);
  if (original.problems.length > 0) {
    const firstBadCol = original.problems[0].column;
    const firstBadIdx = columnOrder.indexOf(firstBadCol);
    if (firstBadIdx >= 0) {
      for (let p = Math.max(0, firstBadIdx - 2); p <= Math.min(columnOrder.length - 1, firstBadIdx + 1); p++) {
        candidatePivots.add(p);
      }
    }
  }

  let bestShift = 0;
  let bestPivot = 0;
  let bestScore = original.score;
  let bestRow: Record<string, unknown> | null = null;
  let bestSwaps: Array<[string, string]> = [];

  for (const pivot of candidatePivots) {
    for (let s = 1; s <= opts.maxShift; s++) {
      for (const shift of [-s, s]) {
        const candidate = buildShiftedRow(row, columnOrder, shift, pivot);
        const { score } = scoreRow(candidate, columnOrder, profiles);
        if (score > bestScore) {
          bestScore = score;
          bestShift = shift;
          bestPivot = pivot;
          bestRow = candidate;
          bestSwaps = [];
        }
      }
    }
  }

  // 2b. Cross-column swap search (referential integrity extension).
  // Closes the same-type column-shift gap that pivot-shifts can't catch:
  // when two columns of compatible types had their values transposed,
  // shifting won't help — only swapping the values does. Runs against
  // the BEST candidate so far (post-shift if a shift won) so swaps can
  // stack on top of a partial shift fix.
  if (opts.enableSwapSearch) {
    const swapBaseRow = bestRow || row;
    const swapBaseScore = bestScore;
    const swapBaseProblems = scoreRow(swapBaseRow, columnOrder, profiles).problems;
    const swapResult = searchSwaps(
      swapBaseRow,
      columnOrder,
      profiles,
      swapBaseScore,
      swapBaseProblems,
      opts.maxSwapCandidates,
      opts.improvementThreshold
    );
    if (swapResult && swapResult.score > bestScore) {
      bestScore = swapResult.score;
      bestRow = swapResult.row;
      bestSwaps = swapResult.swaps;
    }
  }

  // 3. Decide whether to auto-realign or quarantine
  const improvement = bestScore - original.score;
  const hasTransformation = bestShift !== 0 || bestSwaps.length > 0;

  // A transformation that specifically fixes ALL critically-broken columns
  // is high-confidence even when the absolute improvement is small — the
  // alternative interpretation (false-positive realignment) doesn't fit
  // the evidence. Allow these through with a softer improvement floor.
  let criticalsFixed = false;
  if (hasTransformation && bestRow && hasCriticallyBrokenColumn) {
    const postProblems = scoreRow(bestRow, columnOrder, profiles).problems;
    const stillBroken = postProblems.some((p) => p.score < 0.2);
    criticalsFixed = !stillBroken;
  }
  const meetsImprovement =
    improvement >= opts.improvementThreshold || (criticalsFixed && improvement >= 0.05);

  if (hasTransformation && bestScore >= opts.applyThreshold && meetsImprovement) {
    const realigned = bestRow || buildShiftedRow(row, columnOrder, bestShift, bestPivot);
    return {
      kind: 'realigned',
      shift: bestShift,
      pivot: bestPivot,
      transformation: { shift: bestShift, pivot: bestPivot, swaps: bestSwaps },
      originalScore: original.score,
      newScore: bestScore,
      realigned,
      diagnostics: original.problems,
    };
  }

  return {
    kind: 'quarantine',
    originalScore: original.score,
    diagnostics: original.problems,
    bestAttempt: hasTransformation
      ? { shift: bestShift, pivot: bestPivot, swaps: bestSwaps, score: bestScore }
      : undefined,
  };
}

export interface RealignBatchSummary {
  total: number;
  aligned: number;
  realigned: number;
  quarantined: number;
  /**
   * Quarantined rows with POSITIVE misalignment evidence: a different column
   * arrangement (shift/swap) scored materially better than the original but
   * wasn't confident enough to auto-apply. This is the signal for a REAL
   * column-shift — distinct from rows that merely couldn't be verified (e.g.
   * incomplete value-profile enums penalizing valid out-of-sample values, where
   * NO rearrangement fits better and the data is almost certainly in the right
   * columns). Consumers should warn "possibly misaligned" on suspectedShift, NOT
   * on raw quarantined (which over-fires on numeric-heavy / sparse-profile data).
   */
  suspectedShift: number;
  /** Breakdown of how many realignments used each transformation kind */
  realignedByKind: { shiftOnly: number; swapOnly: number; shiftPlusSwap: number };
  realignmentExamples: Array<{
    rowIdx: number;
    shift: number;
    pivot: number;
    swaps: Array<[string, string]>;
    before: number;
    after: number;
  }>;
  quarantineExamples: Array<{ rowIdx: number; score: number; problems: AlignmentDiagnostic[] }>;
}

export interface RealignBatchOptions extends RealignmentOptions {
  /**
   * What to do with quarantined rows in the output:
   *   - 'drop': remove from output (default for sync — keeps DB clean)
   *   - 'keep': leave as-is in output (safe default for pass-through)
   *   - 'mark': add `__quarantine_reason` key (useful when caller wants to surface)
   */
  quarantineMode?: 'drop' | 'keep' | 'mark';
  /** Cap examples returned in summary (default 5) */
  exampleCap?: number;
}

/**
 * Apply realignment across a batch of rows.
 *
 * Returns the (possibly modified) row set plus a summary suitable for logging.
 * No throws — safe to call on any row set; rows without profiles are passed
 * through unchanged.
 */
export function realignRows(
  rows: Record<string, unknown>[],
  columnOrder: string[],
  profiles: Record<string, ValueProfile | undefined>,
  options: RealignBatchOptions = {}
): { rows: Record<string, unknown>[]; summary: RealignBatchSummary } {
  const mode = options.quarantineMode ?? 'keep';
  const cap = options.exampleCap ?? 5;

  // Skip work entirely if we have no profiles to compare against
  const profileCount = Object.values(profiles).filter((p) => p != null).length;
  if (profileCount === 0 || rows.length === 0) {
    return {
      rows,
      summary: {
        total: rows.length,
        aligned: rows.length,
        realigned: 0,
        quarantined: 0,
        suspectedShift: 0,
        realignedByKind: { shiftOnly: 0, swapOnly: 0, shiftPlusSwap: 0 },
        realignmentExamples: [],
        quarantineExamples: [],
      },
    };
  }

  // Effective improvement floor for counting a quarantined row as a suspected
  // shift (a materially-better arrangement existed). Matches the auto-apply
  // improvement gate so "suspected" means the same magnitude of evidence,
  // minus only the absolute-score confidence the auto-apply also requires.
  const improvementFloor = options.improvementThreshold ?? DEFAULT_OPTIONS.improvementThreshold;

  const out: Record<string, unknown>[] = [];
  const summary: RealignBatchSummary = {
    total: rows.length,
    aligned: 0,
    realigned: 0,
    quarantined: 0,
    suspectedShift: 0,
    realignedByKind: { shiftOnly: 0, swapOnly: 0, shiftPlusSwap: 0 },
    realignmentExamples: [],
    quarantineExamples: [],
  };

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const verdict = checkRowAlignment({ row, columnOrder, profiles }, options);

    if (verdict.kind === 'aligned') {
      summary.aligned++;
      out.push(row);
    } else if (verdict.kind === 'realigned') {
      summary.realigned++;
      const hasShift = verdict.transformation.shift !== 0;
      const hasSwap = verdict.transformation.swaps.length > 0;
      if (hasShift && hasSwap) summary.realignedByKind.shiftPlusSwap++;
      else if (hasSwap) summary.realignedByKind.swapOnly++;
      else summary.realignedByKind.shiftOnly++;
      if (summary.realignmentExamples.length < cap) {
        summary.realignmentExamples.push({
          rowIdx: i,
          shift: verdict.shift,
          pivot: verdict.pivot,
          swaps: verdict.transformation.swaps,
          before: verdict.originalScore,
          after: verdict.newScore,
        });
      }
      out.push(verdict.realigned);
    } else {
      // quarantine
      summary.quarantined++;
      // Count POSITIVE shift evidence: a candidate arrangement scored materially
      // better than the original. Absence of a better arrangement → the row
      // failed its profile but isn't shifted (incomplete enum / unverifiable
      // numeric) → NOT a suspected shift.
      if (
        verdict.kind === 'quarantine' &&
        verdict.bestAttempt &&
        verdict.bestAttempt.score - verdict.originalScore >= improvementFloor
      ) {
        summary.suspectedShift++;
      }
      if (summary.quarantineExamples.length < cap) {
        summary.quarantineExamples.push({
          rowIdx: i,
          score: verdict.originalScore,
          problems: verdict.diagnostics.slice(0, 5),
        });
      }
      if (mode === 'keep') {
        out.push(row);
      } else if (mode === 'mark') {
        out.push({
          ...row,
          __quarantine_reason: verdict.diagnostics
            .slice(0, 3)
            .map((d) => `${d.column}: ${d.reason}`)
            .join(' | '),
        });
      }
      // 'drop' → don't push
    }
  }

  return { rows: out, summary };
}
