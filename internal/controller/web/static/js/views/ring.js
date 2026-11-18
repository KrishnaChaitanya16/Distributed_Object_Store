// Hash ring: every worker owns a lane; each virtual node sits at its real hash position.
import { h, svg, clear, hex32, fmtPct, tip, RING, shortId, toast } from '../util.js';
import { api } from '../api.js';
import { store } from '../store.js';
import { naturalCompare, workerColor } from '../colors.js';
import { workerChip, hashChip } from '../ui.js';
import { icon } from '../icons.js';

const VIEW = 760;
const PAD_X = 40;
const CX = VIEW / 2;
const CY = VIEW / 2;
const OUTER = 296;
const LANE_GAP = 30;
const KEY_R = 326;

const angle = (hash) => (hash / RING) * 2 * Math.PI - Math.PI / 2;
const pt = (hash, r) => [CX + r * Math.cos(angle(hash)), CY + r * Math.sin(angle(hash))];
const f2 = (n) => n.toFixed(2);

// shares returns the fraction of the hash space each worker is primary for, from the vnodes currently on the ring.
export function shares(workers) {
  const pts = [];
  for (const w of workers) if (w.in_ring) for (const v of w.vnodes) pts.push(v);
  pts.sort((a, b) => a.hash - b.hash);
  const out = {};
  for (let i = 0; i < pts.length; i++) {
    const prev = i === 0 ? pts[pts.length - 1].hash - RING : pts[i - 1].hash;
    out[pts[i].worker] = (out[pts[i].worker] || 0) + (pts[i].hash - prev) / RING;
  }
  return out;
}

// walkPath draws the clockwise search: from the key down to the first lane, then arc after arc, hopping lanes where the next virtual node belongs to another worker.
export function walkPath(walk, laneRadius) {
  if (!walk.steps.length) return '';
  const first = walk.steps[0];
  let r = laneRadius(first.worker);
  let [x, y] = pt(walk.key_hash, KEY_R);
  let d = `M${f2(x)} ${f2(y)}`;
  [x, y] = pt(walk.key_hash, r);
  d += ` L${f2(x)} ${f2(y)}`;
  let from = walk.key_hash;
  for (const s of walk.steps) {
    const nr = laneRadius(s.worker);
    if (nr !== r) {
      [x, y] = pt(from, nr);
      d += ` L${f2(x)} ${f2(y)}`;
      r = nr;
    }
    const sweepFrom = from;
    const span = (s.hash - sweepFrom + RING) % RING;
    [x, y] = pt(s.hash, r);
    d += ` A${f2(r)} ${f2(r)} 0 ${span > RING / 2 ? 1 : 0} 1 ${f2(x)} ${f2(y)}`;
    from = s.hash;
  }
  return d;
}

export function mount(root) {
  let selected = null;
  let walk = null;
  let lastSig = '';
  const ringBox = h('div');
  const legend = h('div', { class: 'legend' });
  const shareBox = h('div');
  const walkBox = h('div');
  const input = h('input', { type: 'text', placeholder: 'type anything, e.g. hello', 'aria-label': 'Text or chunk id to place on the ring' });
  const samples = h('div', { class: 'legend' });
  const goBtn = h('button', { class: 'btn primary', type: 'button' }, 'Place it');

  const left = h('div', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', null, 'The consistent-hash ring'), h('p', null, 'position = first 32 bits of SHA-256, drawn clockwise from the top')),
    ringBox, legend);
  const right = h('div', { style: { display: 'grid', gap: '14px' } },
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'Where would this go?'), h('p', null, 'the same code that places real chunks')),
      h('div', { class: 'locate' }, input, goBtn), samples, walkBox),
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'Share of the ring'), h('p', null, 'how much of the hash space each worker is primary for')),
      shareBox),
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'How to read it')),
      h('ol', { class: 'steps-list' },
        h('li', null, h('b', null, 'Each worker has 50 virtual nodes'), ' (the dots), spread by hashing "worker:0", "worker:1", ... Many small slices even out the load.'),
        h('li', null, h('b', null, 'A chunk lands where its hash is'), ' (a tick on the outer edge). Walk clockwise to the first dot: that worker is the ', h('b', null, 'primary'), '.'),
        h('li', null, 'Keep walking to the first dot of a ', h('b', null, 'different worker'), ': that is the ', h('b', null, 'secondary'), ', so two copies never share a machine.'),
        h('li', null, 'A hollow dot is a worker that is offline: it has left the ring, so new chunks skip it. Its old chunks are rebuilt elsewhere by the rebalancer.'))));
  root.append(h('div', { class: 'ring-wrap' }, left, right));

  async function locate(query, label) {
    try {
      const res = await api.locate(query);
      walk = res.walk;
      selected = { id: res.chunk_id, label: label || res.chunk_id };
      lastSig = '';
      draw(store);
    } catch (e) {
      toast(`Could not place it: ${e.message}`, 'error');
    }
  }

  goBtn.addEventListener('click', () => locate(`text=${encodeURIComponent(input.value)}`, `"${input.value}"`));
  input.addEventListener('keydown', (e) => { if (e.key === 'Enter') goBtn.click(); });

  function laneIndex(workers) {
    const ids = workers.map((w) => w.id).sort(naturalCompare);
    return (id) => OUTER - ids.indexOf(id) * LANE_GAP;
  }

  function drawWalkCard() {
    clear(walkBox);
    if (!walk) {
      walkBox.append(h('p', { class: 'hint' }, 'Type some text and press "Place it", or click any chunk tick on the ring. The text is hashed with SHA-256 exactly like a chunk would be.'));
      return;
    }
    if (!walk.steps.length) {
      walkBox.append(h('p', { class: 'note' }, 'The ring is empty: no worker is online, so nothing can be placed.'));
      return;
    }
    walkBox.append(
      h('p', { class: 'note' }, 'Key ', hashChip(selected.id, 12), ' hashes to position ', h('b', { class: 'mono' }, hex32(walk.key_hash)), ` (${fmtPct(walk.key_hash / RING, 1)} round the ring).`),
      h('ol', { class: 'walk-list' }, walk.steps.map((s, i) => h('li', null,
        `Next dot clockwise at ${hex32(s.hash)}: `, workerChip(s.worker, { mark: `#${s.index}` }),
        i === 0 ? ' - first dot, so this worker is the primary.' : i === walk.steps.length - 1 ? ' - a different worker, so this is the secondary.' : ' - same worker as the primary, keep walking.'))),
      h('p', { class: 'note', style: { marginTop: '8px' } }, 'Result: primary ', workerChip(walk.primary), walk.secondary ? [' and secondary ', workerChip(walk.secondary)] : ' (no second worker is available)', '.'));
  }

  function draw(s) {
    const st = s.state;
    if (!st) return;
    const workers = [...st.workers].sort((a, b) => naturalCompare(a.id, b.id));
    const chunkTicks = [];
    const seen = new Set();
    for (const o of s.objects) for (const c of o.chunks) if (!seen.has(c.chunk_id)) { seen.add(c.chunk_id); chunkTicks.push({ id: c.chunk_id, pos: c.ring_pos, name: `${o.name} #${c.index + 1}` }); }
    const sig = JSON.stringify([workers.map((w) => [w.id, w.in_ring, w.health]), chunkTicks.map((c) => c.id), selected && selected.id, walk && walk.primary]);
    if (sig === lastSig) return;
    lastSig = sig;

    const radius = laneIndex(workers);
    const fig = svg('svg', { class: 'ring-svg', viewBox: `${-PAD_X} 0 ${VIEW + 2 * PAD_X} ${VIEW}`, role: 'img',
      'aria-label': `Hash ring with ${workers.length} workers, ${workers.filter((w) => w.in_ring).length} on the ring, each with ${st.vnodes_per_worker} virtual nodes. A text summary follows the figure.` });
    for (const w of workers) fig.append(svg('circle', { class: 'lane', cx: CX, cy: CY, r: radius(w.id) }));
    const innerR = OUTER - workers.length * LANE_GAP + 6;
    for (let q = 0; q < 4; q++) {
      const a = (q / 4) * RING;
      const [x1, y1] = pt(a, innerR);
      const [x2, y2] = pt(a, OUTER + 14);
      fig.append(svg('line', { class: 'axis', x1: f2(x1), y1: f2(y1), x2: f2(x2), y2: f2(y2) }));
    }
    const label = (q, text, dx, dy, anchor) => { const [x, y] = pt((q / 4) * RING, OUTER + 28); fig.append(svg('text', { x: f2(x + dx), y: f2(y + dy), 'text-anchor': anchor }, text)); };
    label(0, '0', 0, -2, 'middle');
    label(1, '2³⁰ (25%)', 6, 4, 'start');
    label(2, '2³¹ (50%)', 0, 14, 'middle');
    label(3, '(75%)', -6, 4, 'end');

    for (const c of chunkTicks) {
      const [x1, y1] = pt(c.pos, OUTER + 10);
      const [x2, y2] = pt(c.pos, OUTER + 22);
      const tick = svg('line', { class: `chunk-tick${selected && selected.id === c.id ? ' sel' : ''}`, x1: f2(x1), y1: f2(y1), x2: f2(x2), y2: f2(y2), tabindex: 0, role: 'button', 'aria-label': `Chunk ${c.name}, click to place it` });
      tip(tick, () => [c.name, [['chunk id', shortId(c.id, 12)], ['ring position', hex32(c.pos)], ['action', 'click to trace placement']]]);
      tick.addEventListener('click', () => locate(`chunk=${c.id}`, c.name));
      tick.addEventListener('keydown', (e) => { if (e.key === 'Enter') locate(`chunk=${c.id}`, c.name); });
      fig.append(tick);
    }

    const owned = shares(workers);
    for (const w of workers) {
      for (const v of w.vnodes) {
        const [x, y] = pt(v.hash, radius(w.id));
        const color = workerColor(w.id);
        const dot = svg('circle', { class: `vn${w.in_ring ? '' : ' ghost'}`, cx: f2(x), cy: f2(y), r: 5.2, fill: w.in_ring ? color : undefined, stroke: w.in_ring ? undefined : color });
        tip(dot, () => [`${w.id} · virtual node #${v.index}`, [['position', hex32(v.hash)], ['on the ring', w.in_ring ? 'yes' : 'no (worker offline)'], ['worker owns', fmtPct(owned[w.id] || 0, 1)]]]);
        fig.append(dot);
      }
      const [lx, ly] = pt(0, radius(w.id));
      fig.append(svg('rect', { x: f2(lx + 6), y: f2(ly - 10), width: 70, height: 20, rx: 5, fill: 'var(--surface-1)', opacity: 0.92 }));
      fig.append(svg('text', { x: f2(lx + 11), y: f2(ly + 5) }, w.id));
    }

    if (walk && walk.steps.length) {
      fig.append(svg('path', { class: 'walk', d: walkPath(walk, radius) }));
      const [kx, ky] = pt(walk.key_hash, KEY_R);
      fig.append(svg('path', { class: 'walk-key', d: `M${f2(kx)} ${f2(ky - 8)} l7 8 l-7 8 l-7 -8z` }));
      const mark = (step, text, anchor) => {
        const [x, y] = pt(step.hash, radius(step.worker));
        fig.append(svg('circle', { class: 'halo', cx: f2(x), cy: f2(y), r: 9.5 }));
        const [tx, ty] = pt(step.hash, OUTER + 50);
        const w = text.length * 8 + 10;
        fig.append(svg('rect', { x: f2(anchor === 'end' ? tx - w + 4 : tx - 4), y: f2(ty - 14), width: w, height: 20, rx: 5, fill: 'var(--surface-1)', stroke: 'var(--border-strong)' }));
        fig.append(svg('text', { x: f2(tx), y: f2(ty), 'text-anchor': anchor, style: 'font-weight:650;fill:var(--text)' }, text));
      };
      mark(walk.steps[0], 'primary', 'end');
      if (walk.secondary) mark(walk.steps[walk.steps.length - 1], 'secondary', 'start');
    }
    fig.append(svg('text', { class: 'center-title', x: CX, y: CY - 8, 'text-anchor': 'middle' }, 'hash space'));
    fig.append(svg('text', { x: CX, y: CY + 12, 'text-anchor': 'middle' }, '0 to 2³²'));
    fig.append(svg('text', { x: CX, y: CY + 30, 'text-anchor': 'middle' }, selected ? `placing ${shortId(selected.id)}` : 'click a tick to place a chunk'));
    ringBox.replaceChildren(fig);

    clear(legend);
    for (const w of workers) legend.append(workerChip(w.id, { dead: !w.in_ring, mark: w.in_ring ? `${st.vnodes_per_worker} nodes` : 'off the ring' }));
    clear(shareBox);
    const total = Object.values(owned).reduce((a, b) => a + b, 0) || 1;
    for (const w of workers) {
      const v = (owned[w.id] || 0) / total;
      shareBox.append(h('div', { class: 'share' }, workerChip(w.id, { dead: !w.in_ring }),
        h('div', { class: 'track', role: 'img', 'aria-label': `${w.id} owns ${fmtPct(v, 1)}` }, h('span', { style: { width: `${v * 100}%` } })),
        h('span', { class: 'num' }, w.in_ring ? fmtPct(v, 1) : '0%')));
    }
    shareBox.append(h('p', { class: 'hint', style: { marginTop: '6px' } }, 'An even split is 25% each with four workers. When a worker leaves, its slices pass to the next dots clockwise.'));

    clear(samples);
    for (const c of chunkTicks.slice(0, 6)) samples.append(h('button', { class: 'btn small', type: 'button', onclick: () => locate(`chunk=${c.id}`, c.name) }, icon('hash', 'glyph', 13), c.name));
    drawWalkCard();
  }

  return { update: draw };
}
