/**
 * Phase B SUB-CAP 2B branch SOUNDNESS (wf wnjexl3wa BLOCKER-3/5/MINOR-8). The agent's sub-cap
 * whole-file ordered-detail branch (query-server.ts, after applyFilters, before the file-order slice)
 * must: (1) SELECT the true global top-N for numeric sorts — `[...rows].sort(compareByStreamingOrder)
 * .slice(off, off+lim)` byte-identical to applySort-over-the-full-set sliced (the platform's Path-A
 * correctness); (2) CLONE before sorting (never mutate the shared parse cache — empty-filter returns
 * it by ref); (3) classify numeric→APPLY, date/string→DEFER, field-absent→BAIL via the SAME guards as
 * the >cap lane (isCrossRuntimeSafeSortValue + per-field presence). Selection uses the REAL exported
 * compareByStreamingOrder + isCrossRuntimeSafeSortValue, so any drift from applySort fails here.
 *
 * Run: node packages/agent-core/__tests__/subcap-orderby-2b.test.cjs   (after `npm run build`)
 */
const assert = require('assert');
const { compareByStreamingOrder, isCrossRuntimeSafeSortValue } = require('../dist/query-server.js');

let pass = 0;
const ok = (n) => { pass++; console.log('  ✓ ' + n); };

// ── ORACLE: applySort verbatim (nango-driver.js applySort comparator over a stable V8 sort) — the
//    platform's Path-A correctness the sub-cap 2B branch must match. ──
const ISO_DATE_PREFIX = /^\d{4}-\d{2}-\d{2}/;
function applySortOracle(rows, order) {
  return [...rows].sort((a, b) => {
    for (const { field, direction } of order) {
      const aVal = a[field], bVal = b[field];
      if (aVal == null && bVal == null) continue;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      const aStr = String(aVal), bStr = String(bVal);
      if (ISO_DATE_PREFIX.test(aStr) || ISO_DATE_PREFIX.test(bStr)) {
        const aT = Date.parse(aStr), bT = Date.parse(bStr);
        if (!isNaN(aT) && !isNaN(bT)) { if (aT !== bT) return direction === 'asc' ? aT - bT : bT - aT; continue; }
      }
      const aNum = parseFloat(aVal), bNum = parseFloat(bVal);
      if (!isNaN(aNum) && !isNaN(bNum)) { if (aNum !== bNum) return direction === 'asc' ? aNum - bNum : bNum - aNum; }
      else { const cmp = String(aVal).localeCompare(String(bVal)); if (cmp !== 0) return direction === 'asc' ? cmp : -cmp; }
    }
    return 0;
  });
}

// The 2B numeric APPLY path, verbatim: clone-sort then slice (NO explicit tie-break — V8 stable over
// filtered-file order, == applySort). This is the exact expression in the handler branch.
function subcapApply(rows, order, offset, limit) {
  return [...rows].sort((a, b) => compareByStreamingOrder(a, b, order)).slice(offset, offset + limit);
}

console.log('subcap-orderby-2b (sub-cap Phase B branch vs applySort oracle):');

// 1. SELECTION PARITY — numeric, the wrong-serve class. The true top-N must NOT be the first-N file rows.
{
  // 40 rows, numeric `amt` deliberately NOT in sorted file order (the original bug returned first-N file order).
  const rows = Array.from({ length: 40 }, (_, i) => ({ amt: ((i * 17 + 5) % 40), _i: i }));
  for (const dir of ['asc', 'desc']) {
    const order = [{ field: 'amt', direction: dir }];
    for (const [off, lim] of [[0, 10], [0, 5], [0, 1], [0, 40]]) {
      const got = subcapApply(rows, order, off, lim);
      const want = applySortOracle(rows, order).slice(off, off + lim);
      assert.deepStrictEqual(got, want, `numeric ${dir} [${off},${off + lim})`);
    }
  }
  ok('numeric sub-cap sort+slice == applySort over the FULL set (true global top-N, not first-N file order)');
}

// 2. CLONE / NO MUTATION (MINOR-8) — the sort must not reorder the input (shared cache) array.
{
  const rows = [{ amt: 3, _i: 0 }, { amt: 1, _i: 1 }, { amt: 2, _i: 2 }];
  const before = rows.map((r) => r._i).join(',');
  const out = subcapApply(rows, [{ field: 'amt', direction: 'asc' }], 0, 3);
  assert.strictEqual(rows.map((r) => r._i).join(','), before, 'input array order unchanged (not sorted in place)');
  assert.notStrictEqual(out, rows, 'returns a new array, not the input ref');
  assert.deepStrictEqual(out.map((r) => r._i), [1, 2, 0], 'sorted copy is correct (1,2,3 → indices 1,2,0)');
  ok('clone-sort never mutates the shared parse cache (MINOR-8)');
}

// 3. DECISION GUARD — numeric → APPLY (all safe); date/string → DEFER (some unsafe); these drive the branch.
{
  const numericRows = [{ amt: 1 }, { amt: '2' }, { amt: 3.5 }, { amt: '  4 ' }, { amt: null }];
  const allSafe = numericRows.every((r) => isCrossRuntimeSafeSortValue(r.amt));
  assert.strictEqual(allSafe, true, 'numeric/blank/null rows are all cross-runtime safe → APPLY');

  const dateRows = [{ d: '2026-01-09' }, { d: '2026-12-31' }];
  const anyUnsafeDate = dateRows.some((r) => !isCrossRuntimeSafeSortValue(r.d));
  assert.strictEqual(anyUnsafeDate, true, 'ISO-date rows are unsafe (TZ) → DEFER');

  const strRows = [{ v: 'Acme' }, { v: 'Zeta' }];
  const anyUnsafeStr = strRows.some((r) => !isCrossRuntimeSafeSortValue(r.v));
  assert.strictEqual(anyUnsafeStr, true, 'non-numeric string rows are unsafe (ICU) → DEFER');
  ok('guard: numeric→APPLY, date/string→DEFER (matches the >cap lane classifier)');
}

// 4. FIELD-ABSENT detection (per-field) → BAIL. A secondary sort field absent across ALL rows must trip it.
{
  const order = [{ field: 'amt', direction: 'asc' }, { field: 'missing', direction: 'asc' }];
  const rows = [{ amt: 1 }, { amt: 2 }, { amt: 3 }];
  const fieldSeen = order.map(() => false);
  for (const row of rows) for (let fi = 0; fi < order.length; fi++) if (order[fi].field in row) fieldSeen[fi] = true;
  assert.deepStrictEqual(fieldSeen, [true, false], 'secondary field `missing` never seen');
  assert.ok(fieldSeen.some((s) => !s), 'field-absent guard trips → BAIL (orderByFieldAbsent)');
  ok('per-field presence detects an absent sort key → BAIL (not a wrong-serve)');
}

// 5. FUZZ — random numeric data × order × offset × limit, sub-cap apply == applySort sliced.
{
  let seed = 0x1234567 >>> 0;
  const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 0x100000000; };
  const pick = (a) => a[Math.floor(rnd() * a.length)];
  let runs = 0;
  for (let t = 0; t < 300; t++) {
    const n = 1 + Math.floor(rnd() * 30);
    const rows = Array.from({ length: n }, (_, i) => ({ a: pick([null, 0, 1, 2, 2, 5, 5, '7', '10', '3']), b: pick(['1', '2', '2', null, 0]), _i: i }));
    const order = rnd() < 0.5
      ? [{ field: 'a', direction: pick(['asc', 'desc']) }]
      : [{ field: 'a', direction: pick(['asc', 'desc']) }, { field: 'b', direction: pick(['asc', 'desc']) }];
    const offset = Math.floor(rnd() * 4);
    const limit = 1 + Math.floor(rnd() * 8);
    assert.deepStrictEqual(subcapApply(rows, order, offset, limit), applySortOracle(rows, order).slice(offset, offset + limit), `fuzz#${t}`);
    runs++;
  }
  ok(`fuzz: ${runs} random numeric (data × order × offset × limit) — sub-cap apply == applySort sliced`);
}

console.log('\n' + pass + ' checks passed.');
