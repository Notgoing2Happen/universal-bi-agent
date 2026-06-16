/**
 * Phase 0 HARD GATE for the big-file streaming-detail lane.
 *
 * The streaming-detail design rests on ONE invariant: a streamed early-exit page
 * (skip `offset`, take `limit`, break) is BYTE-IDENTICAL — value AND type AND null-ness —
 * to the current whole-file path (parseCsvFileBuffered then rows.slice(offset, offset+limit)).
 * Both share buildParser/legacyCoerce, so they MUST agree. If they don't, legacyCoerce
 * sharing is broken and the whole streaming approach is unsound — STOP.
 *
 * Run: node __tests__/streaming-detail-soundness.test.cjs   (after `npm run build`)
 */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { streamCsvRows, parseCsvFileBuffered } = require('../dist/parsers/stream-csv.js');

let pass = 0;
const ok = (n) => { pass++; console.log('  ✓ ' + n); };

// Representative CSV: numeric (int/float), blank cells, boolean, multi-line quoted,
// unicode, leading zeros, big int, quoted comma. BOM prepended.
const BOM = '﻿';
const CSV =
  BOM + 'id,name,amount,active,note,code\n' +
  '1,Acme,100.50,true,"hello, world",007\n' +
  '2,,200,false,"multi\nline note",000\n' +
  '3,Béta,0,true,,42\n' +
  '4,Zeta Corp,1234567890123456789,false,plain,x12\n' +
  '5,"Quote ""inside""",-5.5,true,trail,\n';

const tmp = path.join(os.tmpdir(), 'streaming-soundness-' + process.pid + '.csv');
fs.writeFileSync(tmp, CSV);

// Streamed early-exit page: skip `offset` rows, collect next `limit`, break (tears down read).
async function streamedPage(file, offset, limit) {
  const out = [];
  let seen = 0;
  for await (const row of streamCsvRows(file, {})) {
    if (seen++ < offset) continue;
    out.push(row);
    if (out.length >= limit) break; // early exit — native backpressure tears down the file read
  }
  return out;
}

// Deep, type-strict comparison: value (===) AND typeof must match per cell, same key set.
function assertPagesIdentical(a, b, label) {
  assert.strictEqual(a.length, b.length, label + ': row count (' + a.length + ' vs ' + b.length + ')');
  for (let i = 0; i < a.length; i++) {
    const ka = Object.keys(a[i]).sort();
    const kb = Object.keys(b[i]).sort();
    assert.deepStrictEqual(ka, kb, label + ' row ' + i + ': key set');
    for (const k of ka) {
      const va = a[i][k], vb = b[i][k];
      assert.strictEqual(typeof va, typeof vb, label + ' row ' + i + ' col ' + k + ': TYPE (' + typeof va + ' vs ' + typeof vb + ')');
      assert.strictEqual(va, vb, label + ' row ' + i + ' col ' + k + ': VALUE (' + JSON.stringify(va) + ' vs ' + JSON.stringify(vb) + ')');
    }
  }
}

(async () => {
  console.log('streaming-detail-soundness:');

  const buffered = await parseCsvFileBuffered(tmp, {});

  // 0. Sanity: the buffered parse coerced as expected (so the test fixture exercises the real cases).
  {
    assert.strictEqual(buffered.length, 5, 'parsed 5 data rows (5 quoted-multiline counts as one)');
    assert.strictEqual(buffered[0].amount, 100.5, 'numeric float coerced to number');
    assert.strictEqual(typeof buffered[0].amount, 'number', 'amount is a number type');
    assert.strictEqual(buffered[0].active, true, 'boolean coerced');
    assert.strictEqual(buffered[1].name, '', 'blank cell is empty string (NOT null)');
    assert.strictEqual(buffered[1].note, 'multi\nline note', 'multi-line quoted field preserved');
    assert.strictEqual(buffered[0].code, 7, 'leading-zero 007 -> Number 7 (coercion is lossy but DETERMINISTIC)');
    assert.strictEqual(buffered[2].note, '', 'trailing blank note is empty string');
    ok('fixture exercises numeric/blank/boolean/multiline/leading-zero coercion');
  }

  // 1. Full pages identical (offset 0, limit beyond EOF).
  assertPagesIdentical(await streamedPage(tmp, 0, 100), buffered.slice(0, 100), 'full');
  ok('streamed full page == buffered (value + type + null-ness)');

  // 2. offset=0, limit=2 (early exit before EOF — the real big-file win).
  assertPagesIdentical(await streamedPage(tmp, 0, 2), buffered.slice(0, 2), 'offset0-limit2');
  ok('streamed early-exit page (offset 0, limit 2) == buffered slice');

  // 3. offset=1, limit=2 (pagination page 2).
  assertPagesIdentical(await streamedPage(tmp, 1, 2), buffered.slice(1, 3), 'offset1-limit2');
  ok('streamed page (offset 1, limit 2) == buffered slice');

  // 4. offset=3, limit=10 (tail, fewer than limit remain).
  assertPagesIdentical(await streamedPage(tmp, 3, 10), buffered.slice(3, 13), 'offset3-tail');
  ok('streamed tail page (offset 3) == buffered slice');

  // 5. limit=1 (single-row early exit, the most aggressive teardown).
  assertPagesIdentical(await streamedPage(tmp, 0, 1), buffered.slice(0, 1), 'limit1');
  ok('streamed single-row early exit == buffered slice');

  // 6. LOOP COMPOSITION (Phase 1): streamed (skip `offset` MATCHED, take `limit`, per-row
  //    predicate, break) == whole-file (predicate-filter the array, then slice(offset, offset+limit)).
  //    This is the exact composition the agent's streamingDetail branch performs; it must equal
  //    the current whole-file detail path (applyFilters then rows.slice). Uses a representative
  //    predicate (amount >= 100) standing in for applyFilters([row]).
  {
    const pred = (r) => typeof r.amount === 'number' && r.amount >= 100;
    async function streamedFilteredPage(file, offset, limit) {
      const out = []; let matched = 0;
      for await (const row of streamCsvRows(file, {})) {
        if (!pred(row)) continue;
        if (matched++ < offset) continue;
        out.push(row);
        if (out.length >= limit) break;
      }
      return out;
    }
    const wholeFile = (await parseCsvFileBuffered(tmp, {})).filter(pred);
    // offset 0
    assertPagesIdentical(await streamedFilteredPage(tmp, 0, 2), wholeFile.slice(0, 2), 'filtered-offset0');
    // offset 1 (skip 1 matched)
    assertPagesIdentical(await streamedFilteredPage(tmp, 1, 5), wholeFile.slice(1, 6), 'filtered-offset1');
    // limit beyond matched count
    assertPagesIdentical(await streamedFilteredPage(tmp, 0, 100), wholeFile.slice(0, 100), 'filtered-all');
    ok('streamed filter+offset+limit loop == whole-file filter-then-slice (Phase 1 composition)');
  }

  fs.unlinkSync(tmp);
  console.log('\n' + pass + ' checks passed. Phase 0 gate + Phase 1 loop composition: streaming detail is byte-identical to the whole-file path.');
})().catch((e) => { try { fs.unlinkSync(tmp); } catch {} console.error('\nFAIL:', e.message); process.exit(1); });
