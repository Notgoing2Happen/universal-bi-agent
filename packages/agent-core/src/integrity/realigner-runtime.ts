/**
 * Clean runtime API surface for the VENDORED realigner (see VENDORED.md).
 *
 * The agent CONSUMES pre-built ValueProfiles (shipped from the platform on the
 * querySpec) and runs the EXACT per-row realigner over rows streamed from DuckDB
 * to verify column integrity for the big-file self-verify serve path. The agent
 * never BUILDS profiles, so buildValueProfile / the zod ValueProfileSchema /
 * normalize-string are unused at runtime — they remain in the vendored copy only
 * to keep it byte-identical to the platform source (so a drift check is exact).
 */
export { realignRows, checkRowAlignment } from './realignment';
export type {
  RealignBatchSummary,
  RealignBatchOptions,
  RealignmentOptions,
  AlignmentVerdict,
} from './realignment';
export { scoreValueAgainstProfile } from './value-profile';
export type { ValueProfile } from './value-profile';
