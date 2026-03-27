/**
 * Local Data Bridge
 *
 * Pure transform functions for the local data bridge pipeline.
 * Extracted from apps/web/lib/migration-pipeline.ts — no server dependencies.
 *
 * Flow: read source file → resolve columns → transform rows → write to destination
 */

/**
 * A single column mapping in the migration plan.
 */
export interface BridgePlanItem {
  conceptId: string;
  conceptName: string;
  sourceTable: string;
  sourceColumn: string;
  destColumn: string | null;
  action: 'migrate' | 'create';
}

/**
 * Optional import tag injected into every transformed row.
 */
export interface ImportTag {
  columnName: string;
  value: string;
}

/**
 * Resolve migration plan sourceColumn names to actual data keys.
 *
 * Schema discovery and live fetch may use different flattening logic,
 * producing different column names (e.g. "created" vs "created_at").
 *
 * Uses 4-level fuzzy matching:
 * 1. Exact match
 * 2. Case-insensitive match
 * 3. Suffix/prefix match (e.g. "created" matches "created_at")
 * 4. Normalized match (strip underscores, camelCase → lowercase)
 */
export function resolveColumnNames(
  planColumns: string[],
  actualKeys: string[],
): Map<string, string> {
  const resolved = new Map<string, string>();
  const usedKeys = new Set<string>();

  const normalize = (s: string) => s.toLowerCase().replace(/[_\-\s]/g, '');

  for (const planCol of planColumns) {
    // 1. Exact match
    if (actualKeys.includes(planCol)) {
      resolved.set(planCol, planCol);
      usedKeys.add(planCol);
      continue;
    }

    const planLower = planCol.toLowerCase();
    const planNorm = normalize(planCol);

    // 2. Case-insensitive exact match
    let match = actualKeys.find(k => !usedKeys.has(k) && k.toLowerCase() === planLower);
    if (match) {
      resolved.set(planCol, match);
      usedKeys.add(match);
      continue;
    }

    // 3. Suffix/prefix match
    const candidates = actualKeys
      .filter(k => !usedKeys.has(k))
      .filter(k => {
        const kLower = k.toLowerCase();
        return kLower.includes(planLower) || planLower.includes(kLower);
      })
      .sort((a, b) => {
        const aStarts = a.toLowerCase().startsWith(planLower) ? 0 : 1;
        const bStarts = b.toLowerCase().startsWith(planLower) ? 0 : 1;
        if (aStarts !== bStarts) return aStarts - bStarts;
        return a.length - b.length;
      });

    if (candidates.length > 0) {
      resolved.set(planCol, candidates[0]);
      usedKeys.add(candidates[0]);
      continue;
    }

    // 4. Normalized match (strip separators)
    match = actualKeys.find(k => !usedKeys.has(k) && normalize(k) === planNorm);
    if (match) {
      resolved.set(planCol, match);
      usedKeys.add(match);
      continue;
    }

    // No match — leave as-is (will produce undefined values)
    console.warn(`[Bridge] Could not resolve column "${planCol}" in actual data keys`);
  }

  return resolved;
}

/**
 * Transform rows by renaming columns according to the migration plan.
 */
export function transformRows(
  sourceData: Record<string, unknown>[],
  plan: BridgePlanItem[],
  importTag?: ImportTag,
  columnResolution?: Map<string, string>,
): Record<string, unknown>[] {
  return sourceData.map(row => {
    const transformed: Record<string, unknown> = {};
    for (const item of plan) {
      const actualKey = columnResolution?.get(item.sourceColumn) ?? item.sourceColumn;
      const sourceValue = row[actualKey];
      const destCol = item.destColumn || item.sourceColumn;
      transformed[destCol] = sourceValue;
    }
    if (importTag) {
      transformed[importTag.columnName] = importTag.value;
    }
    return transformed;
  });
}
