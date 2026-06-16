/**
 * Verifies the self-verify blank-null divergence gate (compileCountersSql __gbn +
 * the genuineNullFormat decision selfVerifyStream applies).
 *
 * The agent's group/distinct keys COALESCE(col,'') a genuine null into the '' bucket;
 * production keys a genuine null as a SEPARATE 'Unknown' group and excludes it from
 * COUNT(DISTINCT). On CSV a blank is '' in production's JS parse → COALESCE→'' matches
 * (no divergence). ONLY .json/.parquet genuine nulls diverge — and Leg 1 can't catch it
 * (the JS oracle uses the same COALESCE). So __gbn counts at-risk null rows and
 * selfVerifyStream declines when (.json|.parquet) && __gbn > 0.
 *
 * Run: node __tests__/blank-null-gate.test.cjs   (after `npm run build`)
 */
const assert = require('assert');
const path = require('path');
const { compileCountersSql } = require('../dist/duckdb-engine.js');

let pass = 0;
const ok = (n) => { pass++; console.log('  ✓ ' + n); };
const norm = (s) => s.replace(/\s+/g, ' ').trim();

console.log('blank-null-gate:');

// 1. Grouped SUM: __gbn counts rows where the GROUP-key col is NULL.
{
  const spec = { groupBy: ['vendor'], aggregations: [{ type: 'sum', column: 'amount', alias: 'total' }] };
  const sql = norm(compileCountersSql(spec, '/data/f.csv'));
  assert.ok(/AS __gbn/.test(sql), 'has __gbn column');
  assert.ok(/COUNT\(\*\) FILTER \(WHERE TRUE AND \("vendor" IS NULL\)\) AS __gbn/.test(sql),
    'gbn counts vendor IS NULL: ' + sql);
  ok('grouped SUM: __gbn = rows where group key is NULL');
}

// 2. COUNT(DISTINCT col): __gbn counts rows where the distinct-source col is NULL.
{
  const spec = { groupBy: ['vendor'], aggregations: [{ type: 'count', distinct: true, column: 'reagent_id', alias: 'n' }] };
  const sql = norm(compileCountersSql(spec, '/data/f.json'));
  assert.ok(/"vendor" IS NULL OR "reagent_id" IS NULL/.test(sql), 'gbn spans group + distinct cols: ' + sql);
  ok('COUNT(DISTINCT): __gbn spans group key + distinct source col');
}

// 3. Distinct over '*' / '1' is NOT a blank-risk col (no source value).
{
  const spec = { groupBy: ['vendor'], aggregations: [{ type: 'count', distinct: true, column: '*', alias: 'n' }] };
  const sql = norm(compileCountersSql(spec, '/data/f.json'));
  assert.ok(/\("vendor" IS NULL\)\) AS __gbn/.test(sql), 'COUNT(DISTINCT *) adds no col: ' + sql);
  ok('COUNT(DISTINCT *) contributes no blank-risk column');
}

// 4. No group + no distinct (plain TOTAL SUM): __gbn = 0 literal.
{
  const spec = { groupBy: [], aggregations: [{ type: 'sum', column: 'amount', alias: 'total' }] };
  const sql = norm(compileCountersSql(spec, '/data/f.csv'));
  assert.ok(/0 AS __gbn/.test(sql), 'plain TOTAL → gbn literal 0: ' + sql);
  ok('plain TOTAL SUM: __gbn = 0 (no group/distinct keys)');
}

// 5. Plain TOTAL COUNT(DISTINCT) on JSON: distinct col IS the blank-risk col.
{
  const spec = { groupBy: [], aggregations: [{ type: 'count', distinct: true, column: 'sku', alias: 'n' }] };
  const sql = norm(compileCountersSql(spec, '/data/f.json'));
  assert.ok(/\("sku" IS NULL\)\) AS __gbn/.test(sql), 'distinct TOTAL counts sku IS NULL: ' + sql);
  ok('plain distinct TOTAL: __gbn = rows where distinct col is NULL');
}

// 6. Filters are respected (gbn only over FILTERED rows).
{
  const spec = {
    groupBy: ['vendor'],
    aggregations: [{ type: 'sum', column: 'amount', alias: 'total' }],
    filters: [{ column: 'region', operator: 'equals', value: 'US' }],
  };
  const sql = norm(compileCountersSql(spec, '/data/f.csv'));
  assert.ok(/__gbn/.test(sql) && /region/.test(sql), 'gbn includes filter predicate: ' + sql);
  assert.ok(!/WHERE TRUE AND \("vendor" IS NULL\)\) AS __gbn/.test(sql), 'filter pred not TRUE when a filter exists');
  ok('__gbn respects the filter predicate (FILTERED rows only)');
}

// 7. genuineNullFormat decision (the gate selfVerifyStream applies). Mirror its 1-liner.
{
  const genuineNullFormat = (fp) => {
    const ext = path.extname(fp).toLowerCase();
    return ext === '.json' || ext === '.parquet';
  };
  assert.strictEqual(genuineNullFormat('/d/f.json'), true, '.json is genuine-null');
  assert.strictEqual(genuineNullFormat('/d/f.parquet'), true, '.parquet is genuine-null');
  assert.strictEqual(genuineNullFormat('/d/f.csv'), false, '.csv is NOT genuine-null (blank=\'\')');
  assert.strictEqual(genuineNullFormat('/d/f.tsv'), false, '.tsv is NOT genuine-null');
  // blankDivergence = genuineNullFormat && gbn > 0
  const div = (fp, gbn) => genuineNullFormat(fp) && gbn > 0;
  assert.strictEqual(div('/d/f.csv', 9), false, 'CSV with nulls → NO divergence (blanks are \'\')');
  assert.strictEqual(div('/d/f.json', 0), false, 'JSON with NO nulls → NO divergence');
  assert.strictEqual(div('/d/f.json', 1), true, 'JSON with a null group/distinct key → DECLINE');
  assert.strictEqual(div('/d/f.parquet', 3), true, 'Parquet with nulls → DECLINE');
  ok('blankDivergence: only (.json|.parquet) && gbn>0 declines; CSV/clean-JSON serve');
}

console.log(`\n${pass} assertions passed.`);
