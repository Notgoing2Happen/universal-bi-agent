/**
 * Neutral query-spec — contractVersion 2 (Phase 2 of the DuckDB local-passthrough
 * plan). The PLATFORM builds this from its already-parsed Cube.js plan; the AGENT
 * compiles it to engine SQL (DuckDB) and runs it locally, returning only small
 * result rows.
 *
 * It is a strict SUPERSET of the Phase-1 `aggregationSpec` — a plain associative
 * aggregation is the trivial case (no filters/dateTruncs). Resolved transforms
 * (filters, date_trunc) travel as DATA so the agent NEVER reaches back to the
 * platform DB.
 *
 * NOT carried here (by design, per the adversarial review): the AI #7 canonical
 * CASE-WHEN variant-merge. That collapse stays PLATFORM-side via the Phase-5 remap
 * (agent groups by the RAW key, platform re-collapses + re-reduces) — shipping the
 * canonical map as data is the drift-toward-silently-wrong vector we avoid.
 */

export type AggType = 'sum' | 'count' | 'min' | 'max' | 'avg';

export interface SpecAggregation {
  type: AggType;
  /** Source column, or '*' / '1' for COUNT(*). */
  column: string;
  alias: string;
  /** COUNT(DISTINCT col) when true (only meaningful for type 'count'). */
  distinct?: boolean;
}

export type FilterOp =
  | 'equals' | 'notEquals'
  | 'contains' | 'notContains' | 'startsWith' | 'endsWith'
  | 'gt' | 'lt' | 'gte' | 'lte'
  | 'set' | 'notSet';

export interface SpecFilter {
  column: string;
  operator: FilterOp;
  /** Single comparison value. Absent for set/notSet and multi-value filters. */
  value?: string | number;
  /** Multi-value (IN / any-of) for equals/notEquals/contains/notContains/
   *  startsWith/endsWith. Ordered ops (gt/lt/gte/lte) use a single value only. */
  values?: Array<string | number>;
}

export type Granularity = 'day' | 'week' | 'month' | 'quarter' | 'year';

/** A date_trunc applied to a group-by column (the column also appears in groupBy). */
export interface SpecDateTrunc {
  column: string;
  granularity: Granularity;
}

export interface SpecOrder {
  column: string; // a groupBy column or an aggregation alias
  dir: 'asc' | 'desc';
}

export interface QuerySpec {
  contractVersion: 2;
  groupBy: string[];
  aggregations: SpecAggregation[];
  filters?: SpecFilter[];
  dateTruncs?: SpecDateTrunc[];
  orderBy?: SpecOrder[];
  limit?: number;
  /** mtime:size fingerprint the platform proved this cube on — staleness guard. */
  expectedFileSig?: string;
}
