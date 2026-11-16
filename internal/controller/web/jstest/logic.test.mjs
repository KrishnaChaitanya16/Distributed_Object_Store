// Unit tests for the UI's pure logic; they run with `node --test` and need no browser.
import { test } from 'node:test';
import assert from 'node:assert/strict';

import { fmtBytes, fmtPct, fmtAgo, hex32, shortId, clamp, plural, RING } from '../static/js/util.js';
import { parseMetadata } from '../static/js/api.js';
import { setWorkers, slotOf, workerColor, workerOnColor, naturalCompare } from '../static/js/colors.js';
import { shares, walkPath } from '../static/js/views/ring.js';
import { shade } from '../static/js/views/bloom.js';
import { chunkSummary } from '../static/js/views/objects.js';
import { analyzeIncident, analyzeCorruption } from '../static/js/views/healing.js';
import { categoryOf, CATEGORIES } from '../static/js/describe.js';

// ---- formatting ----
test('fmtBytes picks sensible units', () => {
  assert.equal(fmtBytes(0), '0 B');
  assert.equal(fmtBytes(1023), '1023 B');
  assert.equal(fmtBytes(1024), '1.00 KiB');
  assert.equal(fmtBytes(1048576), '1.00 MiB');
  assert.equal(fmtBytes(2.5 * 1048576), '2.50 MiB');
  assert.equal(fmtBytes(15 * 1048576), '15.0 MiB');
  assert.equal(fmtBytes(150 * 1048576), '150 MiB');
  assert.equal(fmtBytes(1024 ** 4 * 3), '3.00 TiB');
  assert.equal(fmtBytes(1024 ** 5), '1024 TiB', 'beyond TiB it stays in TiB');
});

test('fmtBytes copes with missing numbers', () => {
  assert.equal(fmtBytes(undefined), '-');
  assert.equal(fmtBytes(null), '-');
  assert.equal(fmtBytes(NaN), '-');
});

test('fmtPct, fmtAgo, hex32, shortId, clamp, plural', () => {
  assert.equal(fmtPct(0.256, 1), '25.6%');
  assert.equal(fmtPct(0, 2), '0.00%');
  assert.equal(fmtPct(undefined), '-');
  assert.equal(fmtAgo(-1), 'never');
  assert.equal(fmtAgo(null), 'never');
  assert.equal(fmtAgo(250), '250 ms ago');
  assert.equal(fmtAgo(1500), '1.5 s ago');
  assert.equal(fmtAgo(125000), '2 min ago');
  assert.equal(hex32(255), '0x000000ff');
  assert.equal(hex32(0xffffffff), '0xffffffff');
  assert.equal(hex32(2 ** 32), '0x00000000', 'wraps like an unsigned 32-bit value');
  assert.equal(shortId('abcdef0123456789', 8), 'abcdef01');
  assert.equal(shortId(''), '');
  assert.equal(shortId(undefined), '');
  assert.equal(clamp(5, 0, 3), 3);
  assert.equal(clamp(-1, 0, 3), 0);
  assert.equal(plural(1, 'chunk'), '1 chunk');
  assert.equal(plural(3, 'chunk'), '3 chunks');
  assert.equal(plural(1, 'copy', 'copies'), '1 copy');
  assert.equal(plural(0, 'copy', 'copies'), '0 copies');
  assert.equal(plural(1234, 'chunk'), '1,234 chunks');
});

// ---- metadata input ----
test('parseMetadata accepts key=value lists', () => {
  assert.deepEqual(parseMetadata(''), {});
  assert.deepEqual(parseMetadata('   '), {});
  assert.deepEqual(parseMetadata(undefined), {});
  assert.deepEqual(parseMetadata('owner=alice'), { owner: 'alice' });
  assert.deepEqual(parseMetadata(' owner = alice , tag=demo ,, '), { owner: 'alice', tag: 'demo' });
  assert.deepEqual(parseMetadata('url=http://x/?a=b'), { url: 'http://x/?a=b' }, 'only the first = splits');
  assert.deepEqual(parseMetadata('empty='), { empty: '' });
  assert.deepEqual(parseMetadata('k=1,k=2'), { k: '2' }, 'a repeated key keeps the last value');
});

test('parseMetadata rejects malformed input with a readable message', () => {
  for (const bad of ['nokey', '=value', 'a=b,c']) {
    assert.throws(() => parseMetadata(bad), /not key=value/, bad);
  }
});

// ---- worker colours ----
test('worker colours follow the name, not the order they appear', () => {
  setWorkers(['worker3', 'worker1']);
  const c1 = workerColor('worker1');
  const c3 = workerColor('worker3');
  setWorkers(['worker2', 'worker4']);
  assert.equal(workerColor('worker1'), 'var(--s1)');
  assert.equal(workerColor('worker2'), 'var(--s2)');
  assert.equal(workerColor('worker3'), 'var(--s3)');
  assert.equal(workerColor('worker4'), 'var(--s4)');
  assert.notEqual(c1, undefined);
  assert.notEqual(c3, undefined);
  assert.equal(workerOnColor('worker4'), 'var(--on-s4)', 'dark ink on the yellow slot');
});

test('a fifth worker gets the neutral colour and numbers sort naturally', () => {
  setWorkers(['worker5']);
  assert.equal(workerColor('worker5'), 'var(--s-other)');
  assert.equal(workerColor('never-seen'), 'var(--s-other)');
  assert.equal(slotOf('never-seen'), -1);
  assert.deepEqual(['worker10', 'worker2', 'worker1'].sort(naturalCompare), ['worker1', 'worker2', 'worker10']);
});

// ---- ring geometry ----
const vn = (worker, hash, index = 0) => ({ worker, hash, index });
const wk = (id, vnodes, inRing = true) => ({ id, in_ring: inRing, vnodes });

test('shares split the hash space by arc and add up to one', () => {
  const q = RING / 4;
  const out = shares([wk('a', [vn('a', q), vn('a', 3 * q)]), wk('b', [vn('b', 2 * q), vn('b', RING - 1)])]);
  assert.ok(Math.abs(out.a - 0.5) < 1e-9, `a owns ${out.a}`);
  assert.ok(Math.abs(out.b - 0.5) < 1e-3, `b owns ${out.b}`);
  assert.ok(Math.abs(out.a + out.b - 1) < 1e-3);
});

test('a worker off the ring owns nothing and its share goes to the others', () => {
  const q = RING / 4;
  const all = [wk('a', [vn('a', q)]), wk('b', [vn('b', 2 * q)]), wk('c', [vn('c', 3 * q)])];
  const before = shares(all);
  assert.ok(Math.abs(before.b - 0.25) < 1e-9);
  all[1].in_ring = false;
  const after = shares(all);
  assert.equal(after.b, undefined);
  assert.ok(Math.abs(after.a + after.c - 1) < 1e-9, 'the remaining workers cover the whole ring');
  assert.ok(after.c > before.c, 'c inherits the slice that b used to own');
});

test('shares of an empty ring is empty and a single vnode owns everything', () => {
  assert.deepEqual(shares([]), {});
  assert.deepEqual(shares([wk('a', [], false)]), {});
  assert.ok(Math.abs(shares([wk('a', [vn('a', 12345)])]).a - 1) < 1e-9);
});

test('walkPath goes key, down to the first lane, then arcs clockwise hopping lanes', () => {
  const lane = (w) => (w === 'a' ? 290 : 260);
  assert.equal(walkPath({ key_hash: 5, steps: [] }, lane), '');
  const d = walkPath({ key_hash: RING / 8, steps: [vn('a', RING / 4), vn('a', RING / 2), vn('b', (3 * RING) / 4)] }, lane);
  const cmds = d.match(/[MLA]/g);
  assert.equal(cmds[0], 'M');
  assert.equal(cmds.filter((c) => c === 'A').length, 3, 'one arc per step');
  assert.ok(d.includes('A290.00 290.00'), 'arcs along the primary lane');
  assert.ok(d.includes('A260.00 260.00'), 'then hops to the secondary lane');
  assert.ok(d.includes(' 0 0 1 '), 'short arcs use the small-arc clockwise flag');
});

test('walkPath flags arcs longer than half the ring as large', () => {
  const d = walkPath({ key_hash: 0, steps: [vn('a', Math.floor(0.75 * RING))] }, () => 290);
  assert.ok(d.includes(' 0 1 1 '), 'a 270 degree walk needs the large-arc flag');
});

test('walkPath wraps past the end of the ring', () => {
  const d = walkPath({ key_hash: RING - 10, steps: [vn('a', 5)] }, () => 290);
  assert.ok(d.includes(' 0 0 1 '), 'a 15 position wrap is a tiny arc, not a full lap');
});

// ---- Bloom shading ----
test('shade maps counts to six steps with zero kept empty', () => {
  assert.equal(shade(0, 10), 0);
  assert.equal(shade(1, 10), 1);
  assert.equal(shade(10, 10), 6);
  assert.equal(shade(5, 10), 3);
  assert.equal(shade(1, 1000000), 1, 'one set bit is always visible');
  assert.equal(shade(99, 10), 6, 'never beyond the top step');
  assert.equal(shade(3, 0), 6, 'a zero maximum does not divide by zero');
  assert.equal(shade(undefined, 10), 0);
});

// ---- object chunk summary ----
const chunk = (n) => ({ reachable_copies: n });
test('chunkSummary describes the safety of an object in words', () => {
  assert.deepEqual(chunkSummary({ status: 'COMMITTED', chunks: [chunk(2), chunk(2)] }), ['good', 'all 2 chunks have 2 copies']);
  assert.deepEqual(chunkSummary({ status: 'COMMITTED', chunks: [chunk(2)] }), ['good', 'all 1 chunk have 2 copies']);
  assert.deepEqual(chunkSummary({ status: 'COMMITTED', chunks: [chunk(2), chunk(1)] }), ['serious', '1 chunk down to 1 copy']);
  assert.deepEqual(chunkSummary({ status: 'COMMITTED', chunks: [chunk(2), chunk(1), chunk(0)] }), ['critical', '1 chunk unreachable']);
  assert.deepEqual(chunkSummary({ status: 'COMMITTED', chunks: [] }), ['muted', 'no chunks']);
  assert.deepEqual(chunkSummary({ status: 'STAGED', chunks: [] }), ['muted', 'upload in progress']);
  assert.deepEqual(chunkSummary({ status: 'FAILED', chunks: [] }), ['muted', 'failed']);
});

// ---- incident reconstruction ----
const ev = (seq, kind, fields = {}) => ({ seq, kind, time: `2026-11-16T10:00:${String(seq).padStart(2, '0')}Z`, fields });
const healthy = { degraded: 0, unavailable: 0, extra_copies: 0 };
const status = (inc) => Object.fromEntries(inc.steps.map((s) => [s.key, s.status]));

test('no failure means no incident', () => {
  assert.equal(analyzeIncident([], healthy), null);
  assert.equal(analyzeIncident([ev(1, 'upload.start')], healthy), null);
});

test('a fresh failure shows rebuilding in progress', () => {
  const inc = analyzeIncident([ev(1, 'worker.offline', { worker: 'worker2' })], { degraded: 3, unavailable: 0, extra_copies: 0 });
  assert.equal(inc.worker, 'worker2');
  assert.deepEqual(status(inc), { stop: 'done', offline: 'done', rebuild: 'active', healthy: 'active', return: 'pending', cleanup: 'pending' });
});

test('after the rebalancer finishes, replication is restored', () => {
  const inc = analyzeIncident([ev(1, 'worker.offline', { worker: 'worker2' }), ev(2, 'rebalance.copy'), ev(3, 'rebalance.copy')], healthy);
  assert.deepEqual(status(inc), { stop: 'done', offline: 'done', rebuild: 'done', healthy: 'done', return: 'pending', cleanup: 'pending' });
  assert.match(inc.steps[2].detail, /2 chunks/);
});

test('the returning worker triggers cleanup of stale copies', () => {
  const events = [ev(1, 'worker.offline', { worker: 'worker2' }), ev(2, 'rebalance.copy'), ev(3, 'worker.online', { worker: 'worker2' })];
  assert.equal(status(analyzeIncident(events, { degraded: 0, unavailable: 0, extra_copies: 4 })).cleanup, 'active');
  const done = analyzeIncident([...events, ev(4, 'gc.excess'), ev(5, 'gc.excess')], healthy);
  assert.equal(status(done).cleanup, 'done');
  assert.equal(status(done).return, 'done');
  assert.match(done.steps[5].detail, /2 stale copies/);
});

test('only the latest failure counts and another worker returning does not end it', () => {
  const events = [ev(1, 'worker.offline', { worker: 'worker1' }), ev(2, 'worker.online', { worker: 'worker1' }), ev(3, 'worker.offline', { worker: 'worker3' }), ev(4, 'worker.online', { worker: 'worker1' })];
  const inc = analyzeIncident(events, healthy);
  assert.equal(inc.worker, 'worker3');
  assert.equal(status(inc).return, 'pending', 'worker1 coming back is not worker3 coming back');
});

test('unavailable chunks keep the story at the rebuild step', () => {
  const inc = analyzeIncident([ev(1, 'worker.offline', { worker: 'worker2' })], { degraded: 0, unavailable: 2, extra_copies: 0 });
  assert.equal(status(inc).healthy, 'active');
});

test('a failure that affected no chunk says so', () => {
  const inc = analyzeIncident([ev(1, 'worker.offline', { worker: 'worker4' })], healthy);
  assert.equal(status(inc).rebuild, 'done');
  assert.match(inc.steps[2].detail, /nothing needed rebuilding/);
});

// ---- corruption reconstruction ----
test('no damage seen means no corruption story', () => {
  assert.equal(analyzeCorruption([], healthy), null);
  assert.equal(analyzeCorruption([ev(1, 'worker.offline', { worker: 'w' })], healthy), null);
});

test('a demo-damaged copy waits to be detected', () => {
  const s = analyzeCorruption([ev(1, 'demo.action', { action: 'corrupt', worker: 'worker1', chunk_id: 'c1' })], healthy);
  assert.equal(s.steps[0].status, 'done');
  assert.equal(s.steps[1].status, 'active');
  assert.equal(s.steps[2].status, 'pending');
});

test('scrubber detection leads to fallback reads and a restore', () => {
  const base = [ev(1, 'demo.action', { action: 'corrupt', worker: 'worker1', chunk_id: 'c1' }), ev(2, 'scrub.quarantine', { worker: 'worker1', chunk_id: 'c1' })];
  let s = analyzeCorruption(base, { degraded: 1, unavailable: 0, extra_copies: 0 });
  assert.equal(s.steps[1].status, 'done');
  assert.match(s.steps[1].detail, /CRC32C/);
  assert.equal(s.steps[2].status, 'active');
  s = analyzeCorruption([...base, ev(3, 'download.fetch', { ok: true, role: 'secondary', chunk_id: 'c1' }), ev(4, 'gc.restore', { chunk_id: 'c1' })], healthy);
  assert.equal(s.steps[2].status, 'done');
  assert.equal(s.steps[3].status, 'done');
  assert.equal(s.steps[4].status, 'done');
});

test('a download that catches corruption is detection by SHA-256 and read-repair restores it', () => {
  const events = [ev(1, 'download.corrupt', { worker: 'worker2', chunk_id: 'c9' }), ev(2, 'download.fetch', { ok: true, role: 'secondary', chunk_id: 'c9' }), ev(3, 'download.repair', { ok: true, chunk_id: 'c9' })];
  const s = analyzeCorruption(events, healthy);
  assert.match(s.steps[1].detail, /SHA-256/);
  assert.equal(s.steps[3].status, 'done');
});

test('events about other chunks do not complete the story and a failed repair does not count', () => {
  const events = [ev(1, 'scrub.quarantine', { worker: 'w', chunk_id: 'c1' }), ev(2, 'download.repair', { ok: true, chunk_id: 'OTHER' }), ev(3, 'download.repair', { ok: false, chunk_id: 'c1' })];
  const s = analyzeCorruption(events, { degraded: 1, unavailable: 0, extra_copies: 0 });
  assert.equal(s.steps[3].status, 'active');
  assert.equal(s.steps[4].status, 'pending');
});

// ---- event categories ----
test('every event kind belongs to a known category', () => {
  const known = new Set(CATEGORIES.map((c) => c.id));
  for (const kind of ['upload.start', 'upload.commit', 'object.delete', 'download.fetch', 'worker.offline', 'rebalance.copy', 'gc.pass', 'gc.excess', 'scrub.quarantine', 'demo.action', 'something.else']) {
    assert.ok(known.has(categoryOf(kind)), kind);
  }
  assert.equal(categoryOf('gc.restore'), 'reconcile');
  assert.equal(categoryOf('object.delete'), 'upload');
});
