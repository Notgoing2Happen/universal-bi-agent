/**
 * Phase A4 — the SOUNDNESS CONTRACT for the streaming value-map filter (property 2 of
 * docs/streaming-detail-value-map-design.md): the AGENT's in-stream caseWhenSpec eval
 * (query-server.ts applyFilters) MUST equal the PLATFORM's applyFiltersInMemory caseWhenSpec
 * branch (nango-driver.js L5403-5457) byte-for-byte. If it doesn't, the agent could under-collect
 * → the streamed page under-returns. This test runs the REAL agent applyFilters (+ the real
 * rehydrate, exactly as the streaming branch does) against a PLATFORM ORACLE copied verbatim from
 * the platform, across a full matrix of (rawValue × ELSE-form × operator × filterValues) cases.
 *
 * The platform RE-FILTERS the page with the same evaluator, so the actual requirement is only
 * "agent never STRICTER than platform"; byte-exact parity (asserted here) is the simplest guarantee
 * and the strongest. Any drift between the two evaluators fails this test.
 *
 * Run: node packages/agent-core/__tests__/value-map-filter-parity.test.cjs   (after `npm run build`)
 */
const assert = require('assert');
const { applyFilters, rehydrateStreamingFilters } = require('../dist/query-server.js');

let pass = 0, checks = 0;
const ok = (n) => { pass++; console.log('  ✓ ' + n); };

// ── PLATFORM ORACLE: verbatim copy of applyFiltersInMemory's caseWhenSpec branch
//    (nango-driver.js L5403-5457). The oracle uses a Map (as the platform does). Returns
//    true if the row PASSES the filter (kept), false if dropped. ──
function platformCaseWhenPasses(row, f) {
  const spec = f.caseWhenSpec;            // { sourceCol, valueMap: Map, everyRowNonNull }
  const raw = row[spec.sourceCol];
  let caseResult;
  if (raw == null || raw === '') {
    caseResult = null;
  } else {
    const norm = String(raw).trim().toLowerCase();
    const matched = spec.valueMap.get(norm);
    if (matched !== undefined) caseResult = matched;
    else if (spec.everyRowNonNull) caseResult = String(raw);
    else caseResult = null;
  }
  if (f.operator === 'set') return caseResult !== null && caseResult !== undefined && caseResult !== '';
  if (f.operator === 'notSet') return caseResult === null || caseResult === undefined || caseResult === '';
  const v = caseResult == null ? null : String(caseResult);
  const fv = f.values || [];
  switch (f.operator) {
    case 'equals':        return v === null ? false : fv.some((x) => v === String(x));
    case 'notEquals':     return v === null ? true : !fv.some((x) => v === String(x));
    case 'contains':      return v === null ? false : fv.some((x) => v.toLowerCase().includes(String(x).toLowerCase()));
    case 'notContains':   return v === null ? true : !fv.some((x) => v.toLowerCase().includes(String(x).toLowerCase()));
    case 'startsWith':    return v === null ? false : fv.some((x) => v.toLowerCase().startsWith(String(x).toLowerCase()));
    case 'notStartsWith': return v === null ? true : !fv.some((x) => v.toLowerCase().startsWith(String(x).toLowerCase()));
    case 'endsWith':      return v === null ? false : fv.some((x) => v.toLowerCase().endsWith(String(x).toLowerCase()));
    case 'notEndsWith':   return v === null ? true : !fv.some((x) => v.toLowerCase().endsWith(String(x).toLowerCase()));
    default:              return true;
  }
}

// The canonical value-map (raw-lower-trimmed → canonical), as the platform builds it.
const VALUE_MAP = [
  ['sigma-aldrich [usa]', 'Sigma-Aldrich'],
  ['sigma', 'Sigma-Aldrich'],
  ['neb', 'New England Biolabs'],
];
const SRC = 'vendor';

// Matrix axes.
const RAW_VALUES = [
  'Sigma-Aldrich [USA]',   // matched (mixed case + spaces — exercises trim+lowercase)
  '  sigma  ',             // matched after trim+lowercase
  'NEB',                   // matched
  'Thermo Fisher',         // UNMATCHED (→ everyRowNonNull ? String(raw) : null)
  '',                      // blank → null
  null,                    // null → null
];
const ELSE_FORMS = [false, true];   // everyRowNonNull: ELSE NULL vs ELSE 'unmatched:'||src
const OPERATORS = ['equals', 'notEquals', 'contains', 'notContains', 'startsWith', 'notStartsWith', 'endsWith', 'notEndsWith', 'set', 'notSet'];
const FILTER_VALUE_SETS = [
  ['Sigma-Aldrich'],                  // canonical match
  ['New England Biolabs'],            // other canonical
  ['Thermo Fisher'],                  // the raw unmatched value (only reachable via everyRowNonNull)
  ['Sigma'],                          // substring of a canonical (contains/startsWith)
  ['Biolabs'],                        // substring/suffix
  [],                                 // empty (set/notSet)
];

console.log('value-map-filter-parity (agent applyFilters vs platform applyFiltersInMemory):');

let mismatches = 0;
const firstMismatches = [];
for (const everyRowNonNull of ELSE_FORMS) {
  for (const rawValue of RAW_VALUES) {
    for (const operator of OPERATORS) {
      for (const values of FILTER_VALUE_SETS) {
        checks++;
        const row = { [SRC]: rawValue };
        // Build the WIRE filter (valueMap as pairs, exactly as the platform serializes it).
        const wireFilter = {
          caseWhenSpec: { sourceCol: SRC, valueMap: VALUE_MAP.map((p) => p.slice()), everyRowNonNull },
          operator,
          values,
        };
        // AGENT: rehydrate (as the streaming branch does) then run the REAL applyFilters.
        const agentFilters = rehydrateStreamingFilters([wireFilter]);
        const agentKeeps = applyFilters([row], agentFilters).length === 1;
        // PLATFORM ORACLE: same filter with a Map valueMap.
        const oracleKeeps = platformCaseWhenPasses(row, {
          caseWhenSpec: { sourceCol: SRC, valueMap: new Map(VALUE_MAP), everyRowNonNull },
          operator, values,
        });
        if (agentKeeps !== oracleKeeps) {
          mismatches++;
          if (firstMismatches.length < 8) firstMismatches.push({ rawValue, everyRowNonNull, operator, values, agentKeeps, oracleKeeps });
        }
      }
    }
  }
}

if (mismatches > 0) {
  console.error(`\nPARITY MISMATCH: ${mismatches}/${checks} cases diverge. Samples:`);
  for (const m of firstMismatches) console.error('  ' + JSON.stringify(m));
  process.exit(1);
}
ok(`agent eval == platform oracle for ALL ${checks} (rawValue × ELSE-form × operator × filterValues) cases`);

// Mixed filter AND: a representable filter + a value-map filter — row kept iff BOTH pass.
{
  const rows = [
    { vendor: 'Sigma-Aldrich [USA]', region: 'US' },   // canonical Sigma + US → keep
    { vendor: 'sigma', region: 'EU' },                  // canonical Sigma but EU → drop
    { vendor: 'NEB', region: 'US' },                    // canonical NEB (≠ Sigma) but US → drop
  ];
  const filters = rehydrateStreamingFilters([
    { column: 'region', operator: 'equals', value: 'US' },
    { caseWhenSpec: { sourceCol: 'vendor', valueMap: VALUE_MAP.map((p) => p.slice()), everyRowNonNull: false }, operator: 'equals', values: ['Sigma-Aldrich'] },
  ]);
  const kept = applyFilters(rows, filters);
  assert.strictEqual(kept.length, 1, 'mixed AND keeps only Sigma+US');
  assert.strictEqual(kept[0].region, 'US');
  assert.strictEqual(kept[0].vendor, 'Sigma-Aldrich [USA]');
  ok('mixed representable + value-map filters AND together (both must pass)');
}

// Rehydrate is idempotent + non-mutating; an already-Map valueMap is preserved.
{
  const wire = [{ caseWhenSpec: { sourceCol: 'vendor', valueMap: [['sigma', 'Sigma-Aldrich']], everyRowNonNull: false }, operator: 'equals', values: ['Sigma-Aldrich'] }];
  const r1 = rehydrateStreamingFilters(wire);
  assert.ok(r1[0].caseWhenSpec.valueMap instanceof Map, 'array → Map');
  assert.ok(Array.isArray(wire[0].caseWhenSpec.valueMap), 'input not mutated (still array)');
  const r2 = rehydrateStreamingFilters(r1);
  assert.ok(r2[0].caseWhenSpec.valueMap instanceof Map, 'idempotent (Map stays Map)');
  ok('rehydrateStreamingFilters: array→Map, non-mutating, idempotent');
}

console.log('\n' + pass + ' checks passed (' + checks + ' parity cases).');
