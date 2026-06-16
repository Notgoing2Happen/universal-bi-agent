/**
 * Phase 3 / B3 HARD GATE for the DETAIL-passthrough lane's coercion.
 *
 * The B3 serve rests on ONE invariant: the agent's DuckDB detail page, after coerceDetailRows(),
 * is BYTE-IDENTICAL (value AND type AND null-ness) to the production raw-detail baseline
 * (parseCsvFileBuffered) — for every real cell type. If it isn't, the platform's ordered
 * byte-exact shadow (_compareDetailRows) will never prove the shape and B3 silently never serves;
 * this test makes that parity an explicit, measured contract instead.
 *
 * Run (needs a duckdb binary — discovered from PATH / AGENT_DUCKDB_PATH / the agent's temp engines):
 *   node packages/agent-core/__tests__/detail-passthrough.soundness.test.cjs   (after `npm run build`)
 * If no duckdb binary is found, the test SKIPS (does not fail) — the parity is DuckDB-dependent.
 */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

// ── discover a duckdb binary (mirror findDuckdbBinary) ──
function findDuck() {
  if (process.env.AGENT_DUCKDB_PATH && fs.existsSync(process.env.AGENT_DUCKDB_PATH)) return process.env.AGENT_DUCKDB_PATH;
  const exe = process.platform === 'win32' ? 'duckdb.exe' : 'duckdb';
  for (const dir of (process.env.PATH || '').split(path.delimiter)) {
    if (dir && fs.existsSync(path.join(dir, exe))) return path.join(dir, exe);
  }
  // the agent's lazy-download / bundled engines live under temp eng-* dirs on this box
  try {
    const tmp = os.tmpdir();
    for (const d of fs.readdirSync(tmp)) {
      if (d.startsWith('eng-')) { const p = path.join(tmp, d, exe); if (fs.existsSync(p)) return p; }
    }
  } catch { /* */ }
  return null;
}

const duck = findDuck();
if (!duck) {
  console.log('detail-passthrough.soundness: SKIP (no duckdb binary found — parity is DuckDB-dependent)');
  process.exit(0);
}
process.env.AGENT_DUCKDB_PATH = duck;

const { parseCsvFileBuffered, coerceDetailRows } = require('../dist/parsers/stream-csv.js');
const { runPassthroughSql } = require('../dist/duckdb-engine.js');

let pass = 0;
const ok = (n) => { pass++; console.log('  ✓ ' + n); };

// Fixture exercising every real cell type + (last col) the KNOWN residual: quoted intentional spaces.
const BOM = '﻿';
const CSV =
  BOM + 'id,name,amount,active,note,code,when,padnum,padtext,qspace\n' +
  '1,Acme,100.50,true,"hello, world",007,2026-01-09,  100  ,  hi  ,plain\n' +
  '2,,200,false,"multi\nline",000,2025-12-31,3.0,plain,"  spaced  "\n' +
  '3,Béta,0,TRUE,,42,2026-12-01,-5,,xyz\n' +
  '4,Zeta,1234567890123456789,false,trail,x12,2024-06-01,1e3,"q""x""",end\n';

const tmp = path.join(os.tmpdir(), 'b3-soundness-' + process.pid + '.csv');
fs.writeFileSync(tmp, CSV);

(async () => {
  console.log('detail-passthrough.soundness (duckdb: ' + path.basename(path.dirname(duck)) + '):');

  const buffered = await parseCsvFileBuffered(tmp, {});
  const duckRaw = await runPassthroughSql('SELECT * FROM __agent_src__', tmp, {});
  assert.ok(Array.isArray(duckRaw), 'runPassthroughSql returned rows');
  const coerced = coerceDetailRows(duckRaw);

  // 0. Sanity: the fixture exercises the coercion cases.
  assert.strictEqual(buffered.length, 4, 'parsed 4 data rows');
  assert.strictEqual(buffered[0].amount, 100.5, 'baseline numeric coerced');
  assert.strictEqual(typeof buffered[0].amount, 'number', 'baseline numeric is number');
  assert.strictEqual(buffered[1].name, '', 'baseline blank is empty string');
  ok('fixture parsed; baseline exercises numeric/blank coercion');

  // 1. THE GATE: coerced DuckDB page == production baseline, cell-by-cell, EXCEPT the known
  //    quoted-intentional-whitespace residual (qspace col, row 1).
  let divergent = 0; const residual = [];
  for (let i = 0; i < Math.max(buffered.length, coerced.length); i++) {
    const b = buffered[i] || {}, c = coerced[i] || {};
    for (const k of new Set([...Object.keys(b), ...Object.keys(c)])) {
      const bv = b[k], cv = c[k];
      if (bv === cv && typeof bv === typeof cv) continue;
      // The ONE known, documented residual: quoted intentional whitespace.
      if (k === 'qspace' && typeof bv === 'string' && bv.trim() === String(cv)) { residual.push({ i, k, bv, cv }); continue; }
      divergent++;
      console.log(`   UNEXPECTED DIVERGENCE row${i} col=${k}: baseline=${JSON.stringify(bv)}(${typeof bv}) coerced=${JSON.stringify(cv)}(${typeof cv})`);
    }
  }
  assert.strictEqual(divergent, 0, 'no UNEXPECTED divergences (numeric/bool/blank/date/leadingzero/bigint/unicode/quoted-comma/padded-unquoted all byte-match)');
  ok('coerced DuckDB page byte-matches production baseline for all real cell types');

  // 2. The known residual exists and is exactly quoted-intentional-whitespace (→ safe decline, not a wrong serve).
  assert.ok(residual.length >= 1, 'the quoted-intentional-whitespace residual is present (documents the safe-decline case)');
  assert.strictEqual(residual[0].k, 'qspace', 'residual is the qspace (quoted spaces) column');
  ok('the ONLY residual is quoted intentional whitespace — caught by the platform shadow → safe decline');

  // 3. coerceDetailRows is null/undefined-safe (the DuckDB-miss path passes null through).
  assert.strictEqual(coerceDetailRows(null), null, 'null passes through');
  assert.strictEqual(coerceDetailRows(undefined), undefined, 'undefined passes through');
  ok('coerceDetailRows is null/undefined-safe (DuckDB-miss path)');

  fs.unlinkSync(tmp);
  console.log('\n' + pass + ' checks passed. B3 detail-passthrough coercion is byte-identical to production (residual = quoted whitespace only, safely declined).');
})().catch((e) => { try { fs.unlinkSync(tmp); } catch {} console.error('\nFAIL:', e.message); process.exit(1); });
