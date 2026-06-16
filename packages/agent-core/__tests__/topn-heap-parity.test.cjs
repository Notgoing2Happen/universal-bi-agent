/**
 * Phase B SOUNDNESS CONTRACT — the streaming ORDER BY top-N bounded heap (BoundedTopK) must select
 * the SAME rows, in the same order, that the platform's applySort would (nango-driver.js applySort),
 * for the page [offset, offset+limit). If it diverges, a big-file "ORDER BY x LIMIT n" serves the
 * WRONG rows. The delicate case is a TIE at the K/K+1 boundary: applySort is a stable (V8) sort, so
 * it keeps the earlier-inserted tied row — the heap must do the same via its stream-index tie-break.
 *
 * This runs the REAL agent BoundedTopK against a verbatim applySort oracle across hand-built edge
 * cases AND a randomized fuzz sweep (random data × order × offset × limit). Any divergence fails.
 *
 * Run: node packages/agent-core/__tests__/topn-heap-parity.test.cjs   (after `npm run build`)
 */
const assert = require('assert');
const { BoundedTopK, compareByStreamingOrder } = require('../dist/query-server.js');

let pass = 0;
const ok = (n) => { pass++; console.log('  ✓ ' + n); };

// ── ORACLE: applySort verbatim (nango-driver.js applySort comparator) over a stable [V8] sort.
//    Returns rows sorted; the top-N page = .slice(offset, offset+limit). No explicit tie-break —
//    relies on V8 sort stability, exactly like applySort. ──
function applySortOracle(rows, order) {
  return [...rows].sort((a, b) => {
    for (const { field, direction } of order) {
      const aVal = a[field], bVal = b[field];
      if (aVal == null && bVal == null) continue;
      if (aVal == null) return 1;
      if (bVal == null) return -1;
      const aNum = parseFloat(aVal), bNum = parseFloat(bVal);
      if (!isNaN(aNum) && !isNaN(bNum)) {
        if (aNum !== bNum) return direction === 'asc' ? aNum - bNum : bNum - aNum;
      } else {
        const cmp = String(aVal).localeCompare(String(bVal));
        if (cmp !== 0) return direction === 'asc' ? cmp : -cmp;
      }
    }
    return 0;
  });
}

// Run BoundedTopK over rows (in array order = stream order) and slice the page.
function heapTopN(rows, order, offset, limit) {
  const h = new BoundedTopK(offset + limit, order);
  rows.forEach((r, i) => h.offer(r, i));
  return h.result().slice(offset, offset + limit);
}

function assertSamePage(rows, order, offset, limit, label) {
  const want = applySortOracle(rows, order).slice(offset, offset + limit);
  const got = heapTopN(rows, order, offset, limit);
  assert.strictEqual(got.length, want.length, `${label}: page length (${got.length} vs ${want.length})`);
  for (let i = 0; i < want.length; i++) {
    assert.deepStrictEqual(got[i], want[i], `${label}: row ${i} (got ${JSON.stringify(got[i])} want ${JSON.stringify(want[i])})`);
  }
}

console.log('topn-heap-parity (BoundedTopK vs applySort oracle):');

// 1. Numeric desc, distinct.
{
  const rows = [{ a: 5 }, { a: 1 }, { a: 9 }, { a: 3 }, { a: 7 }];
  assertSamePage(rows, [{ field: 'a', direction: 'desc' }], 0, 3, 'numeric-desc');
  ok('numeric DESC top-3');
}
// 2. Numeric asc, limit beyond row count.
{
  const rows = [{ a: 5 }, { a: 1 }, { a: 9 }];
  assertSamePage(rows, [{ field: 'a', direction: 'asc' }], 0, 100, 'numeric-asc-all');
  ok('numeric ASC, limit > rows → all');
}
// 3. String localeCompare.
{
  const rows = [{ v: 'banana' }, { v: 'apple' }, { v: 'cherry' }, { v: 'Apple' }];
  assertSamePage(rows, [{ field: 'v', direction: 'asc' }], 0, 2, 'string-asc');
  ok('string ASC (localeCompare) top-2');
}
// 4. Nulls sort last.
{
  const rows = [{ a: 3 }, { a: null }, { a: 1 }, { a: null }, { a: 2 }];
  assertSamePage(rows, [{ field: 'a', direction: 'asc' }], 0, 4, 'nulls-last');
  ok('nulls sort last');
}
// 5. Multi-field: numeric primary tie → string secondary.
{
  const rows = [
    { g: 1, n: 'z' }, { g: 1, n: 'a' }, { g: 2, n: 'm' }, { g: 1, n: 'k' },
  ];
  assertSamePage(rows, [{ field: 'g', direction: 'asc' }, { field: 'n', direction: 'asc' }], 0, 3, 'multi-field');
  ok('multi-field (primary tie → secondary)');
}
// 6. BOUNDARY TIE (the wrong-serve case): many equal keys straddling the K/K+1 boundary →
//    must keep the SAME (earlier-inserted) rows applySort keeps.
{
  const rows = [];
  for (let i = 0; i < 10; i++) rows.push({ a: 5, tag: 'i' + i });   // all tie on `a`
  rows.push({ a: 1, tag: 'low' });                                  // one clearly-first
  assertSamePage(rows, [{ field: 'a', direction: 'asc' }], 0, 4, 'boundary-tie');     // low + first 3 tied (by index)
  assertSamePage(rows, [{ field: 'a', direction: 'desc' }], 0, 4, 'boundary-tie-desc'); // first 4 tied (low is last)
  ok('boundary TIE keeps the same stable rows as applySort (the wrong-serve guard)');
}
// 7. Pagination offset > 0.
{
  const rows = Array.from({ length: 20 }, (_, i) => ({ a: i, tag: 't' + i }));
  assertSamePage(rows, [{ field: 'a', direction: 'desc' }], 5, 5, 'offset-page'); // rows ranked 6..10
  ok('offset > 0 (pagination page of a top-N)');
}
// 8. K = 1.
{
  const rows = [{ a: 3 }, { a: 9 }, { a: 1 }];
  assertSamePage(rows, [{ field: 'a', direction: 'desc' }], 0, 1, 'k1');
  ok('K = 1 (single top)');
}

// 9. FUZZ: random data × order × offset × limit. Deterministic LCG (no Math.random — keep it
//    reproducible). Mixes numeric, numeric-as-string, null, and repeated keys (forces ties).
{
  let seed = 0x9e3779b9 >>> 0;
  const rnd = () => { seed = (seed * 1664525 + 1013904223) >>> 0; return seed / 0x100000000; };
  const pick = (arr) => arr[Math.floor(rnd() * arr.length)];
  let runs = 0;
  for (let t = 0; t < 400; t++) {
    const n = 1 + Math.floor(rnd() * 40);
    const rows = Array.from({ length: n }, (_, i) => {
      const a = pick([null, 0, 1, 2, 2, 3, 5, 5, 5, '7', '10', '2']); // dupes + numeric-strings + null
      const b = pick(['x', 'y', 'y', 'z', null, 'Apple', 'apple']);
      return { a, b, _i: i }; // _i makes rows distinguishable so deepEqual checks identity, not just key
    });
    const order = rnd() < 0.5
      ? [{ field: 'a', direction: pick(['asc', 'desc']) }]
      : [{ field: 'a', direction: pick(['asc', 'desc']) }, { field: 'b', direction: pick(['asc', 'desc']) }];
    const offset = Math.floor(rnd() * 5);
    const limit = 1 + Math.floor(rnd() * 8);
    assertSamePage(rows, order, offset, limit, `fuzz#${t}`);
    runs++;
  }
  ok(`fuzz: ${runs} random (data × order × offset × limit) cases — heap == applySort`);
}

// 10. compareByStreamingOrder direct sanity (the shared comparator).
{
  assert.ok(compareByStreamingOrder({ a: 1 }, { a: 2 }, [{ field: 'a', direction: 'asc' }]) < 0, 'asc 1<2');
  assert.ok(compareByStreamingOrder({ a: 1 }, { a: 2 }, [{ field: 'a', direction: 'desc' }]) > 0, 'desc 1 after 2');
  assert.ok(compareByStreamingOrder({ a: null }, { a: 2 }, [{ field: 'a', direction: 'asc' }]) > 0, 'null last');
  assert.strictEqual(compareByStreamingOrder({ a: 5 }, { a: 5 }, [{ field: 'a', direction: 'asc' }]), 0, 'equal → 0 (caller breaks)');
  ok('compareByStreamingOrder: asc/desc/nulls-last/tie');
}

console.log('\n' + pass + ' checks passed.');
