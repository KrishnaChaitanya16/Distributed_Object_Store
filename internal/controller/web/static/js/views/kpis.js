// Headline numbers across the top. Every tile is a number plus the words that explain it.
import { h, fmtInt, plural, reconcile } from '../util.js';
import { icon, TONE_ICON } from '../icons.js';

function tile(id, label, value, sub, tone) {
  return { id, label, value, sub, tone };
}

function build(item) {
  const el = h('div', { class: 'tile', dataset: { id: item.id } },
    h('div', { class: 'tile-label' }),
    h('div', { class: 'tile-value' }),
    h('div', { class: 'tile-sub' }));
  return el;
}

function update(el, item) {
  const [label, value, sub] = el.children;
  const ico = item.tone ? icon(TONE_ICON[item.tone], 'glyph', 14) : null;
  if (ico) ico.setAttribute('data-tone', item.tone);
  label.replaceChildren(ico || '', item.label);
  value.replaceChildren(...item.value);
  sub.textContent = item.sub;
  sub.title = item.sub;
}

export function mount(root) {
  return {
    update(store) {
      const st = store.state;
      if (!st) return;
      const c = st.cluster;
      const total = c.workers_total;
      const online = c.workers_online;
      const workerTone = online === total && total > 0 ? 'good' : online === 0 ? 'critical' : 'serious';
      const ch = c.chunks;
      const durable = ch.healthy + ch.degraded + ch.unavailable;
      const atRisk = ch.degraded + ch.unavailable;
      const replTone = durable === 0 ? 'muted' : atRisk === 0 ? 'good' : ch.unavailable > 0 ? 'critical' : 'serious';
      const unscanned = c.workers_unscanned || 0;
      const objs = c.objects || {};
      const bloom = st.bloom;
      const items = [
        tile('workers', 'Workers online', [String(online), h('small', null, ` / ${total}`)], total - online > 0 ? `${total - online} offline` : 'all alive', workerTone),
        tile('objects', 'Objects', [fmtInt(objs.COMMITTED || 0)], `${objs.STAGED || 0} uploading, ${objs.FAILED || 0} failed`),
        tile('chunks', 'Chunks', [fmtInt(ch.total)], `${ch.by_state.DURABLE || 0} durable, ${ch.by_state.PENDING || 0} pending`),
        tile('repl', 'Chunks with 2 copies', [fmtInt(ch.healthy), h('small', null, ` / ${durable}`)], atRisk === 0 ? (unscanned ? 'so far (some disks not scanned)' : 'replication factor met') : `${ch.degraded} with 1 copy, ${ch.unavailable} unreachable`, replTone),
        tile('extra', 'Stale extra copies', [fmtInt(ch.extra_copies)], unscanned ? `not sure yet: ${plural(unscanned, 'worker')} not scanned` : ch.extra_copies ? 'reconciler will remove them' : 'none', unscanned ? 'warning' : ch.extra_copies ? 'warning' : 'good'),
        tile('bloom', 'Bloom filter entries', [fmtInt(bloom.items)], `${bloom.hash_funcs} bits set per chunk id`),
      ];
      reconcile(root, items, (i) => i.id, build, update);
    },
  };
}
