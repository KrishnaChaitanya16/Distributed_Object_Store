// Bloom filter: the real bit array the controller consults before looking a chunk up in the database.
import { h, clear, fmtInt, fmtPct, fmtBytes, showTip, moveTip, hideTip, toast, shortId } from '../util.js';
import { api } from '../api.js';
import { pill, hashChip } from '../ui.js';

const STEPS = 6;

// shade maps a cell's set-bit count to one of STEPS sequential shades; 0 means empty.
export function shade(count, max) {
  if (!count) return 0;
  return Math.max(1, Math.min(STEPS, Math.ceil((count / Math.max(1, max)) * STEPS)));
}

export function mount(root) {
  let probe = null;
  let cellsCache = [];
  let perCell = 1;
  const tiles = h('div', { class: 'stats' });
  const counters = h('div', { class: 'stats' });
  const grid = h('div', { class: 'heat', role: 'img', 'aria-label': 'Bloom filter bit array shown as a heat map. Darker cells have more bits set.' });
  const scale = h('div', { class: 'scale' });
  const input = h('input', { type: 'text', placeholder: 'type anything, e.g. hello', 'aria-label': 'Text or chunk id to look up in the Bloom filter', style: { flex: '1 1 200px', padding: '7px 10px', border: '1px solid var(--border-strong)', borderRadius: '8px', background: 'var(--surface-1)', color: 'var(--text)', font: 'inherit', minWidth: 0 } });
  const goBtn = h('button', { class: 'btn primary', type: 'button' }, 'Look it up');
  const samples = h('div', { class: 'legend' });
  const result = h('div');

  root.append(
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'The filter right now'), h('p', null, 'a compact "have I seen this chunk?" memory, kept in the controller')),
      tiles, h('div', { style: { height: '10px' } }), counters),
    h('div', { class: 'ring-wrap' },
      h('div', { class: 'card' },
        h('div', { class: 'card-head' }, h('h2', null, 'The bit array'), h('p', null, 'every cell folds many bits; dark = many set')),
        grid, scale),
      h('div', { style: { display: 'grid', gap: '14px' } },
        h('div', { class: 'card' },
          h('div', { class: 'card-head' }, h('h2', null, 'Ask the filter'), h('p', null, 'the same test an upload performs')),
          h('div', { class: 'locate' }, input, goBtn), samples, result),
        h('div', { class: 'card' },
          h('div', { class: 'card-head' }, h('h2', null, 'How it works')),
          h('ol', { class: 'steps-list' },
            h('li', null, 'A chunk id is run through ', h('b', null, 'k hash functions'), '; each picks one bit position out of millions.'),
            h('li', null, h('b', null, 'Adding'), ' a chunk sets those k bits to 1.'),
            h('li', null, h('b', null, 'Asking'), ': if ', h('b', null, 'any'), ' of the k bits is 0, the chunk was never added: ', h('b', null, 'definitely new'), ' (no false negatives, ever).'),
            h('li', null, 'If ', h('b', null, 'all'), ' k bits are 1 it is ', h('b', null, 'possibly stored'), ': other chunks may have set those bits, so the controller confirms in its database. A wrong "maybe" is a ', h('b', null, 'false positive'), '.'),
            h('li', null, 'A filter cannot forget, so a chunk that was deleted still says "maybe". That is harmless: the database has the final word.')))
      )));

  grid.addEventListener('pointermove', (e) => {
    const cell = e.target.closest('.cell');
    if (!cell) { hideTip(); return; }
    const i = Number(cell.dataset.i);
    showTip(e, `Cell ${i + 1}`, [['bits covered', `${fmtInt(i * perCell)} to ${fmtInt((i + 1) * perCell - 1)}`], ['bits set', fmtInt(cellsCache[i] || 0)]]);
    moveTip(e);
  });
  grid.addEventListener('pointerleave', hideTip);

  async function run(query) {
    try {
      probe = await api.probe(query);
      renderProbe();
    } catch (e) {
      toast(`Lookup failed: ${e.message}`, 'error');
    }
  }
  goBtn.addEventListener('click', () => run(`text=${encodeURIComponent(input.value)}`));
  input.addEventListener('keydown', (e) => { if (e.key === 'Enter') goBtn.click(); });

  function stat(k, v, title) {
    return h('div', { class: 'stat', title }, h('div', { class: 'k' }, k), h('div', { class: 'v num' }, v));
  }

  function renderProbe() {
    clear(result);
    for (const c of grid.children) c.classList.remove('probe');
    if (!probe) return;
    const p = probe.probe;
    const allSet = p.set.every(Boolean);
    for (const i of p.cells) grid.children[i]?.classList.add('probe');
    const verdict = !allSet ? pill('good', 'Definitely NOT stored: at least one bit is 0')
      : probe.in_store ? pill('muted', 'Possibly stored, and the database confirms it')
        : pill('serious', 'Possibly stored, but the database says no: a false positive');
    result.append(
      h('div', { class: 'verdict' }, verdict),
      h('p', { class: 'note' }, 'Chunk id ', hashChip(p.chunk_id, 12), ` maps to ${p.positions.length} bit positions (outlined above):`),
      h('div', { class: 'bitrow' }, p.positions.map((pos, i) => h('span', { class: `bit ${p.set[i] ? 'set' : 'unset'}`, title: `bit ${fmtInt(pos)} is ${p.set[i] ? 'set to 1' : '0'}` },
        h('b', null, p.set[i] ? '1' : '0'), fmtInt(pos)))));
  }

  return {
    update(s) {
      const st = s.state;
      if (!st) return;
      const b = st.bloom;
      const target = 0.01;
      clear(tiles);
      tiles.append(
        stat('Size', `${fmtBytes(Math.round(b.bits / 8))}`, `${fmtInt(b.bits)} bits`),
        stat('Hash functions (k)', String(b.hash_funcs), 'Each chunk id sets or tests this many bits'),
        stat('Chunk ids remembered', fmtInt(b.items), 'Estimated from the number of set bits'),
        stat('Bits set', fmtPct(b.fill_ratio, 3), `${fmtInt(b.set_bits)} of ${fmtInt(b.bits)}`),
        stat('False-positive rate now', fmtPct(b.estimated_false_positive, 4), `Designed for ${fmtPct(target, 0)} when full`));
      const c = b.counters;
      clear(counters);
      counters.append(
        stat('Questions asked', fmtInt(c.queries), 'One per chunk uploaded'),
        stat('"Definitely new"', fmtInt(c.definitely_new), 'At least one bit was 0'),
        stat('"Possibly stored"', fmtInt(c.maybe), 'All bits were 1'),
        stat('...really stored (duplicates)', fmtInt(c.true_positive), 'The database confirmed it'),
        stat('...false positives', fmtInt(c.false_positive), 'The database had never seen it'));

      cellsCache = b.cells;
      perCell = b.bits_per_cell;
      const max = Math.max(1, ...b.cells);
      const side = Math.round(Math.sqrt(b.cells.length)) || 1;
      grid.style.gridTemplateColumns = `repeat(${side}, minmax(0, 1fr))`;
      if (grid.children.length !== b.cells.length) {
        clear(grid);
        for (let i = 0; i < b.cells.length; i++) grid.append(h('div', { class: 'cell', dataset: { i, s: '0' } }));
        renderProbe();
      }
      b.cells.forEach((v, i) => { grid.children[i].dataset.s = String(shade(v, max)); });
      clear(scale);
      scale.append('fewer bits set ');
      for (let i = 0; i <= STEPS; i++) scale.append(h('i', { style: { background: `var(--seq-${i})`, border: i === 0 ? '1px solid var(--border)' : 'none' } }));
      scale.append(` more (a full cell holds ${fmtInt(max)} here)`);

      const picks = [];
      const seen = new Set();
      for (const o of s.objects) for (const ch of o.chunks) {
        if (picks.length >= 4 || seen.has(ch.chunk_id)) continue;
        seen.add(ch.chunk_id);
        picks.push({ id: ch.chunk_id, label: `${o.name} #${ch.index + 1} (${shortId(ch.chunk_id)})` });
      }
      const sampleSig = picks.map((p) => p.id).join(',');
      if (sampleSig !== samples.dataset.sig) {
        samples.dataset.sig = sampleSig;
        clear(samples);
        for (const p of picks) samples.append(h('button', { class: 'btn small', type: 'button', onclick: () => run(`chunk=${p.id}`) }, p.label));
        samples.append(h('button', { class: 'btn small', type: 'button', onclick: () => run(`text=${encodeURIComponent(`never-stored-${Date.now()}`)}`) }, 'a chunk never stored'));
      }
    },
  };
}
