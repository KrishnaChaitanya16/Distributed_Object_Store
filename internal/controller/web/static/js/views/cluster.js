// Workers: who is alive, how fresh their heartbeat is, and what is really on their disks.
import { h, svg, reconcile, fmtAgo, fmtInt, shortId } from '../util.js';
import { naturalCompare, workerColor } from '../colors.js';
import { healthPill } from '../ui.js';
import { icon } from '../icons.js';

const W = 760;
const H = 300;

function leaseTone(ageMs, leaseMs) {
  if (ageMs < 0) return 'var(--warning)';
  const f = ageMs / leaseMs;
  return f < 0.5 ? 'var(--good)' : f < 0.85 ? 'var(--warning)' : 'var(--critical)';
}

export function topology(workers) {
  const fig = svg('svg', { class: 'topo', viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': `Controller connected to ${workers.length} workers; ${workers.filter((w) => w.health === 'ONLINE').length} are alive.` });
  const cx = W / 2;
  const cy = 62;
  const n = workers.length || 1;
  const pos = workers.map((w, i) => [(W / (n + 1)) * (i + 1), 212, w]);
  for (const [x, y, w] of pos) {
    const alive = w.health === 'ONLINE';
    const dead = w.health === 'OFFLINE';
    const d = `M${x} ${y - 30} L${cx} ${cy + 26}`;
    fig.append(svg('path', { class: `link ${alive ? 'alive' : dead ? 'dead' : ''}`, d, 'stroke-dasharray': dead ? '2 7' : undefined, 'stroke-linecap': 'round' }));
    if (alive) {
      for (let k = 0; k < 2; k++) {
        // animateMotion adds the path to the shape's own position, so the dot must start at the origin
        const dot = svg('circle', { class: 'pulse', r: 3.4, cx: 0, cy: 0 });
        dot.append(svg('animateMotion', { dur: '2s', begin: `${k}s`, repeatCount: 'indefinite', path: d }));
        fig.append(dot);
      }
    }
    if (dead) {
      const mx = (x + cx) / 2;
      const my = (y - 30 + cy + 26) / 2;
      fig.append(svg('circle', { cx: mx, cy: my, r: 10, fill: 'var(--surface-1)', stroke: 'var(--critical)', 'stroke-width': 2 }));
      fig.append(svg('path', { d: `M${mx - 4} ${my - 4} l8 8 M${mx + 4} ${my - 4} l-8 8`, stroke: 'var(--critical)', 'stroke-width': 2, 'stroke-linecap': 'round' }));
    }
  }
  fig.append(svg('rect', { x: cx - 120, y: cy - 28, width: 240, height: 56, rx: 12, class: 'node', stroke: 'var(--text-2)', 'stroke-width': 2 }));
  fig.append(svg('text', { class: 'node-label', x: cx, y: cy - 4, 'text-anchor': 'middle' }, 'Controller'));
  fig.append(svg('text', { x: cx, y: cy + 14, 'text-anchor': 'middle' }, 'metadata, hash ring, Bloom filter, reconciler'));
  for (const [x, y, w] of pos) {
    const alive = w.health === 'ONLINE';
    const color = workerColor(w.id);
    fig.append(svg('circle', { class: 'node', cx: x, cy: y, r: 30, stroke: color, opacity: alive ? 1 : 0.65 }));
    fig.append(svg('path', { d: alive ? `M${x - 8} ${y} l5 5 l11 -11` : w.health === 'OFFLINE' ? `M${x - 7} ${y - 7} l14 14 M${x + 7} ${y - 7} l-14 14` : `M${x - 4} ${y - 5} a4 4 0 118 0c0 3-4 3-4 7 M${x} ${y + 11} v.1`,
      fill: 'none', stroke: alive ? 'var(--good)' : w.health === 'OFFLINE' ? 'var(--critical)' : 'var(--warning)', 'stroke-width': 3, 'stroke-linecap': 'round', 'stroke-linejoin': 'round' }));
    fig.append(svg('text', { class: 'node-label', x, y: y + 50, 'text-anchor': 'middle' }, w.id));
    fig.append(svg('text', { x, y: y + 66, 'text-anchor': 'middle' }, alive ? 'alive' : w.health === 'OFFLINE' ? 'DEAD (no heartbeat)' : 'unknown'));
  }
  return fig;
}

function buildCard(w) {
  const head = h('div', { class: 'wcard-head' });
  const hb = h('div', { class: 'hb' });
  const info = h('dl', { class: 'kv' });
  const scrub = h('div', { class: 'scrub' });
  const chips = h('div', { class: 'chips', role: 'list', 'aria-label': `Chunks on ${w.id}` });
  const el = h('article', { class: 'wcard', dataset: { id: w.id } }, head, hb, info, scrub, h('h4', { style: { marginTop: '10px', fontSize: '13px' } }, 'On disk'), chips);
  Object.assign(el, { head, hb, info, scrub, chips });
  return el;
}

function updateCard(el, w, ctx) {
  const dead = w.health !== 'ONLINE';
  el.classList.toggle('dead', dead);
  el.style.setProperty('--wc', workerColor(w.id));
  const lease = ctx.leaseMs;
  const age = w.heartbeat_age_ms;
  const pct = age < 0 ? 100 : Math.min(100, (age / lease) * 100);
  el.head.replaceChildren(h('h3', null, h('i'), w.id), healthPill(w.health));
  el.hb.replaceChildren(
    h('div', { class: 'hb-row' },
      h('span', null, h('span', { class: `hb-dot${w.health === 'ONLINE' && age >= 0 && age < 2600 ? ' beat' : ''}`, style: { background: leaseTone(age, lease) } }), w.health === 'ONLINE' ? 'Last heartbeat' : 'Silent for'),
      h('b', { class: 'num' }, fmtAgo(age))),
    h('div', { class: 'hb-bar', role: 'img', 'aria-label': `${Math.round(pct)}% of the ${lease / 1000} second lease used` }, h('span', { style: { width: `${pct}%`, background: leaseTone(age, lease) } })),
    h('div', { class: 'hint' }, `Workers beat every ${ctx.heartbeatS} s. No beat for ${lease / 1000} s and the controller declares them OFFLINE.`));
  el.info.replaceChildren(
    h('dt', null, 'Address'), h('dd', { class: 'mono' }, w.addr),
    h('dt', null, 'On the ring'), h('dd', null, w.in_ring ? `yes, ${ctx.vnodes} virtual nodes` : 'no, removed from the ring'),
    h('dt', null, 'Chunks held'), h('dd', null, w.inventory_ok ? fmtInt(w.chunk_count) : dead ? 'unreachable' : 'unknown'));
  const s = w.scrubber;
  if (s) {
    el.scrub.replaceChildren(icon('shield', 'glyph', 14), ` Scrubber: ${fmtInt(s.passes)} passes (every ${s.interval_seconds}s), last pass checked ${fmtInt(s.last_pass_scanned)} chunks in ${fmtInt(s.last_pass_duration_ms)} ms. `,
      h('b', null, `${fmtInt(s.corrupt_quarantined)} corrupt ${s.corrupt_quarantined === 1 ? 'copy' : 'copies'} quarantined.`));
  } else {
    el.scrub.replaceChildren(dead ? 'Scrubber status unavailable while the worker is down.' : 'No scrubber report.');
  }
  const ids = w.inventory_ok ? w.chunks : [];
  reconcile(el.chips, ids, (id) => id,
    (id) => h('span', { class: 'cchip', role: 'listitem', dataset: { chunk: id } }, shortId(id, 7)),
    (chip, id) => {
      const listed = ctx.listed.get(id);
      const extra = !listed || !listed.has(w.id);
      chip.classList.toggle('extra', extra);
      chip.title = extra ? `${id}\nNot listed as a replica on this worker: a stale or orphaned copy` : `${id}\nListed replica`;
    });
  if (!ids.length) {
    if (!el.chips.querySelector('.hint')) el.chips.append(h('span', { class: 'hint', dataset: { empty: '1' } }, dead ? 'Disk cannot be read while the worker is down.' : 'No chunks stored.'));
  } else {
    el.chips.querySelector('[data-empty]')?.remove();
  }
  for (const chip of el.chips.querySelectorAll('.cchip')) {
    if (!chip.title) chip.title = chip.dataset.chunk;
  }
}

export function mount(root) {
  const topoBox = h('div');
  const grid = h('div', { class: 'wgrid' });
  let topoSig = '';
  root.append(
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'Cluster'), h('p', null, 'green pulses are heartbeats travelling to the controller')), topoBox),
    grid,
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'Reading the worker cards')),
      h('ul', { class: 'steps-list' },
        h('li', null, h('b', null, 'Heartbeat meter:'), ' each worker reports in every 2 seconds. The bar fills as the silence grows; at 6 seconds the lease expires and the worker is declared offline.'),
        h('li', null, h('b', null, 'On disk:'), ' the real contents of each worker, fetched from the worker itself, not from the controller\'s records. Hover a chunk to see the same chunk on the other workers.'),
        h('li', null, h('b', null, 'Orange outline:'), ' a copy the controller does not list for that worker. After a worker returns from the dead it still has old copies; the reconciler removes them.'))));

  grid.addEventListener('pointerover', (e) => {
    const chip = e.target.closest('.cchip');
    for (const c of grid.querySelectorAll('.cchip.hl')) c.classList.remove('hl');
    if (!chip) return;
    for (const c of grid.querySelectorAll(`.cchip[data-chunk="${CSS.escape(chip.dataset.chunk)}"]`)) c.classList.add('hl');
  });
  grid.addEventListener('pointerleave', () => { for (const c of grid.querySelectorAll('.cchip.hl')) c.classList.remove('hl'); });

  return {
    update(s) {
      const st = s.state;
      if (!st) return;
      const workers = [...st.workers].sort((a, b) => naturalCompare(a.id, b.id));
      const sig = workers.map((w) => `${w.id}:${w.health}`).join(',');
      if (sig !== topoSig) {
        topoSig = sig;
        topoBox.replaceChildren(topology(workers));
      }
      const listed = new Map();
      for (const o of s.objects) for (const c of o.chunks) {
        if (!listed.has(c.chunk_id)) listed.set(c.chunk_id, new Set());
        for (const r of c.replicas) listed.get(c.chunk_id).add(r.worker);
      }
      const ctx = { leaseMs: st.config.lease_timeout_s * 1000, heartbeatS: st.config.heartbeat_interval_s, vnodes: st.vnodes_per_worker, listed };
      reconcile(grid, workers, (w) => w.id, buildCard, (el, w) => updateCard(el, w, ctx));
    },
  };
}
