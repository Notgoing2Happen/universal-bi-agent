/**
 * DuckDB parity check — Phase 4 reconciliation, runnable locally.
 *
 * Compiles QuerySpecs to DuckDB SQL (duckdb-engine) and asserts the result
 * byte-matches the agent's JS reference (applyAggregations) — the same oracle the
 * platform shadow gate uses — with the platform's tolerance (exact for
 * count/min/max, 1e-9 relative for sum).
 *
 * SKIPS gracefully (exit 0) when no DuckDB binary is found, so it never breaks a
 * machine without DuckDB. Point it at a binary via AGENT_DUCKDB_PATH, or put
 * `duckdb` on PATH.
 *
 * Run:  node packages/agent-core/scripts/duckdb-parity-check.cjs
 * (build agent-core first: pnpm --filter @universal-bi/agent-core build)
 */
const fs = require('fs');
const os = require('os');
const path = require('path');

const dist = path.join(__dirname, '..', 'dist');
let mod;
try {
  mod = {
    applyAggregations: require(path.join(dist, 'query-server.js')).applyAggregations,
    compileSpecToSql: require(path.join(dist, 'duckdb-engine.js')).compileSpecToSql,
    runDuckdbJson: require(path.join(dist, 'duckdb-engine.js')).runDuckdbJson,
    findDuckdbBinary: require(path.join(dist, 'duckdb-engine.js')).findDuckdbBinary,
  };
} catch (e) {
  console.error('Build agent-core first (pnpm --filter @universal-bi/agent-core build). ' + e.message);
  process.exit(1);
}

const DUCK = mod.findDuckdbBinary();
if (!DUCK) {
  console.log('SKIP: no DuckDB binary found (set AGENT_DUCKDB_PATH or add duckdb to PATH). Parity check skipped.');
  process.exit(0);
}

let pass = 0, fail = 0;
const okEq = (name, cond, extra) => { if (cond) pass++; else { fail++; console.log('FAIL: ' + name + (extra ? '  ' + extra : '')); } };

function writeCsv(file, cols, rows) {
  const esc = (v) => { const s = v == null ? '' : String(v); return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; };
  fs.writeFileSync(file, [cols.join(','), ...rows.map(r => cols.map(c => esc(r[c])).join(','))].join('\n'));
}

// Mirror query-server.ts applyFilters (the /query handler filters before aggregating).
function jsApplyFilters(rows, filters) {
  if (!filters || !filters.length) return rows;
  return rows.filter(row => filters.every(f => {
    const val = row[f.column];
    switch (f.operator) {
      case 'equals': return val == f.value;
      case 'notEquals': return val != f.value;
      case 'contains': return String(val).toLowerCase().includes(String(f.value).toLowerCase());
      case 'gt': return Number(val) > Number(f.value);
      case 'lt': return Number(val) < Number(f.value);
      case 'gte': return Number(val) >= Number(f.value);
      case 'lte': return Number(val) <= Number(f.value);
      default: return true;
    }
  }));
}

const num = (v) => (v == null ? null : Number(v));
function sameNum(a, b, isSum) {
  a = num(a); b = num(b);
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  if (!isSum) return a === b;
  return Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b));
}
function compareResult(name, spec, jsRows, duckRows) {
  const gb = spec.groupBy || [];
  const sumAliases = new Set(spec.aggregations.filter(a => a.type === 'sum').map(a => a.alias));
  const keyOf = (r) => gb.map(g => String(r[g] ?? '')).join('|||');
  const jsMap = new Map(jsRows.map(r => [keyOf(r), r]));
  const dkMap = new Map(duckRows.map(r => [keyOf(r), r]));
  let same = jsMap.size === dkMap.size;
  let detail = `groups js=${jsMap.size} duck=${dkMap.size}`;
  for (const [k, jr] of jsMap) {
    const dr = dkMap.get(k);
    if (!dr) { same = false; detail = `missing duck group "${k}"`; break; }
    for (const a of spec.aggregations) {
      if (!sameNum(jr[a.alias], dr[a.alias], sumAliases.has(a.alias))) {
        same = false; detail = `group "${k}" ${a.alias}: js=${jr[a.alias]} duck=${dr[a.alias]}`; break;
      }
    }
    if (!same) break;
  }
  okEq(name, same, detail);
}

(async () => {
  const v = await mod.runDuckdbJson(DUCK, 'SELECT version() AS v');
  console.log('DuckDB:', v[0].v, '@', DUCK);

  const tmp = path.join(os.tmpdir(), 'duckparity-' + process.pid);
  fs.mkdirSync(tmp, { recursive: true });
  const cols = ['vendor', 'region', 'cost', 'qty'];
  const rows = [
    { vendor: 'Acme', region: 'NA', cost: '100.50', qty: '2' },
    { vendor: 'Acme', region: 'NA', cost: '99.50', qty: '3' },
    { vendor: 'Acme', region: 'EU', cost: '', qty: '5' },     // blank measure -> 0
    { vendor: 'Bolt', region: 'EU', cost: '40', qty: '' },
    { vendor: 'Bolt', region: 'EU', cost: '-10', qty: '1' },  // negative
    { vendor: '', region: 'NA', cost: '7', qty: '4' },        // empty group key
    { vendor: 'Acme', region: 'NA', cost: '0.25', qty: '0' },
  ];
  const csv = path.join(tmp, 'sales.csv');
  writeCsv(csv, cols, rows);

  const specs = {
    'SUM by vendor': { contractVersion: 2, groupBy: ['vendor'], aggregations: [{ type: 'sum', column: 'cost', alias: 'total_cost' }] },
    'COUNT+SUM by vendor,region': { contractVersion: 2, groupBy: ['vendor', 'region'], aggregations: [{ type: 'count', column: '*', alias: 'n' }, { type: 'sum', column: 'cost', alias: 'total_cost' }] },
    'MIN/MAX by region': { contractVersion: 2, groupBy: ['region'], aggregations: [{ type: 'min', column: 'cost', alias: 'min_cost' }, { type: 'max', column: 'cost', alias: 'max_cost' }] },
    'SUM blank measure': { contractVersion: 2, groupBy: ['vendor'], aggregations: [{ type: 'sum', column: 'qty', alias: 'total_qty' }] },
    'grand total (no groupBy)': { contractVersion: 2, groupBy: [], aggregations: [{ type: 'sum', column: 'cost', alias: 'total_cost' }] },
    'filtered SUM (region=EU)': { contractVersion: 2, groupBy: ['vendor'], aggregations: [{ type: 'sum', column: 'cost', alias: 'total_cost' }], filters: [{ column: 'region', operator: 'equals', value: 'EU' }] },
  };

  for (const [name, spec] of Object.entries(specs)) {
    const jsRef = mod.applyAggregations(jsApplyFilters(rows, spec.filters), spec);
    let duck;
    try { duck = await mod.runDuckdbJson(DUCK, mod.compileSpecToSql(spec, csv)); }
    catch (e) { okEq(name, false, 'duckdb error: ' + e.message); continue; }
    compareResult(name, spec, jsRef.rows, duck);
  }

  fs.rmSync(tmp, { recursive: true, force: true });
  console.log(`\nDUCKDB-PARITY: ${pass} passed, ${fail} failed (of ${pass + fail})`);
  process.exit(fail ? 1 : 0);
})().catch(e => { console.error('ERR', e); process.exit(1); });
