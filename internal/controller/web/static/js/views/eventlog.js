// The live event rail: every upload step, failure and repair as it happens, in plain English.
import { h, clear, fmtClock } from '../util.js';
import { icon } from '../icons.js';
import { describe, CATEGORIES, categoryOf } from '../describe.js';
import { store, onEvent, connectionState } from '../store.js';

const MAX_ROWS = 300;
const QUIET = new Set(['gc.pass']);

export function mount(root) {
  const on = new Set(CATEGORIES.map((c) => c.id));
  let paused = false;
  let held = [];
  let hideQuiet = true;

  const list = h('div', { class: 'events', role: 'log', 'aria-live': 'off', tabindex: '0', 'aria-label': 'Event list' });
  const foot = h('div', { class: 'rail-foot' });
  const pauseBtn = h('button', { class: 'btn small', type: 'button', onclick: () => { paused = !paused; if (!paused) flush(); renderHead(); } });
  const clearBtn = h('button', { class: 'btn small ghost', type: 'button', onclick: () => { clear(list); held = []; renderFoot(); } }, 'Clear');
  const quietBtn = h('button', { class: 'btn small ghost', type: 'button', onclick: () => { hideQuiet = !hideQuiet; applyFilters(); renderHead(); } });
  const filterBox = h('div', { class: 'filters', role: 'group', 'aria-label': 'Event categories' });
  const head = h('div', { class: 'rail-head' },
    h('div', { class: 'rail-title' }, h('h2', null, 'Live events'), h('div', { style: { display: 'flex', gap: '6px' } }, quietBtn, pauseBtn, clearBtn)),
    filterBox);
  root.append(head, list, foot);

  for (const cat of CATEGORIES) {
    filterBox.append(h('button', { type: 'button', 'aria-pressed': 'true', dataset: { cat: cat.id },
      onclick: (e) => {
        const b = e.currentTarget;
        const next = b.getAttribute('aria-pressed') !== 'true';
        b.setAttribute('aria-pressed', String(next));
        if (next) on.add(cat.id); else on.delete(cat.id);
        applyFilters();
      } }, icon(cat.icon, 'glyph', 13), cat.label));
  }

  function renderHead() {
    pauseBtn.textContent = paused ? `Resume${held.length ? ` (${held.length})` : ''}` : 'Pause';
    quietBtn.textContent = hideQuiet ? 'Show routine' : 'Hide routine';
    quietBtn.title = 'Routine events are the reconciler passes that run every 30 seconds.';
  }

  function renderFoot() {
    const n = list.children.length;
    const conn = connectionState();
    foot.replaceChildren(
      h('span', null, `${n} shown`),
      h('span', null, conn === 'live' ? 'streaming live' : conn === 'offline' ? 'controller unreachable' : 'reconnecting'));
  }

  function visible(el) {
    return on.has(el.dataset.cat) && !(hideQuiet && el.dataset.quiet === '1');
  }

  function applyFilters() {
    for (const el of list.children) el.hidden = !visible(el);
  }

  function row(ev, fresh) {
    const d = describe(ev);
    const el = h('div', { class: `ev${fresh ? ' fresh' : ''}`, dataset: { cat: categoryOf(ev.kind), kind: ev.kind, quiet: QUIET.has(ev.kind) ? '1' : '0' } },
      h('span', { 'data-tone': d.tone }, icon(d.icon, 'glyph ev-ico', 20)),
      h('div', null,
        h('div', { class: 'ev-top' }, h('span', { class: 'ev-title' }, d.title), h('span', { class: 'ev-time' }, fmtClock(ev.time))),
        h('div', { class: 'ev-detail' }, ...d.detail)));
    el.hidden = !visible(el);
    return el;
  }

  function add(ev, fresh) {
    list.prepend(row(ev, fresh));
    while (list.children.length > MAX_ROWS) list.lastChild.remove();
  }

  function flush() {
    for (const ev of held) add(ev, true);
    held = [];
  }

  for (const ev of store.events.slice(-120)) add(ev, false);
  onEvent((ev) => {
    if (paused) { held.push(ev); renderHead(); return; }
    add(ev, true);
    renderFoot();
  });
  renderHead();
  renderFoot();

  return { update: renderFoot };
}
