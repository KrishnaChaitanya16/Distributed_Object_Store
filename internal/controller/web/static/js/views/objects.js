// Objects and chunks: the metadata the controller keeps, and where every copy really is.
import { h, reconcile, fmtBytes, fmtDateTime, hex32, toast, plural } from '../util.js';
import { api, newTrace, parseMetadata } from '../api.js';
import { refreshNow } from '../store.js';
import { objectStatusPill, chunkStatePill, workerChip, hashChip, pill } from '../ui.js';
import { icon } from '../icons.js';

// chunkSummary describes how safe an object's chunks are right now, in words.
export function chunkSummary(o) {
  if (o.status !== 'COMMITTED') return ['muted', o.status === 'STAGED' ? 'upload in progress' : o.status.toLowerCase()];
  const chunks = o.chunks;
  if (!chunks.length) return ['muted', 'no chunks'];
  const full = chunks.filter((c) => c.reachable_copies >= 2).length;
  const one = chunks.filter((c) => c.reachable_copies === 1).length;
  const none = chunks.length - full - one;
  if (full === chunks.length) return ['good', `all ${plural(chunks.length, 'chunk')} have 2 copies`];
  if (none > 0) return ['critical', `${plural(none, 'chunk')} unreachable`];
  return ['serious', `${plural(one, 'chunk')} down to 1 copy`];
}

function replicaChip(r) {
  const mark = r.physical === true ? 'on disk' : r.physical === false ? 'MISSING' : 'unreachable';
  return workerChip(r.worker, { dead: !r.online, mark, title: r.physical === true ? `${r.worker} holds this chunk` : r.physical === false ? `${r.worker} is listed but does not have the chunk` : `${r.worker} is offline, its disk cannot be checked` });
}

function chunkTable(o) {
  const rows = o.chunks.map((c) => h('tr', null,
    h('td', { class: 'num' }, String(c.index + 1)),
    h('td', null, hashChip(c.chunk_id, 10)),
    h('td', { class: 'num' }, fmtBytes(c.size)),
    h('td', { class: 'mono' }, hex32(c.ring_pos)),
    h('td', null, chunkStatePill(c.state)),
    h('td', { class: 'num' }, String(c.ref_count)),
    h('td', null, h('div', { class: 'replicas' }, c.replicas.map(replicaChip), c.extra_copies.map((w) => workerChip(w, { mark: 'extra copy', title: `${w} holds a copy the controller does not list; the reconciler will delete it` }))))));
  return h('div', { class: 'tbl-wrap' }, h('table', { class: 'tbl' },
    h('thead', null, h('tr', null, ['#', 'Chunk id (SHA-256)', 'Size', 'Ring position', 'State', 'Used by', 'Copies (listed replicas)'].map((t) => h('th', null, t)))),
    h('tbody', null, rows)));
}

function details(o) {
  const meta = Object.entries(o.custom_metadata || {});
  return h('div', null,
    h('dl', { class: 'kv' },
      h('dt', null, 'Object id'), h('dd', null, hashChip(o.object_id, 16)),
      h('dt', null, 'Content type'), h('dd', null, o.content_type || 'application/octet-stream'),
      h('dt', null, 'Created'), h('dd', null, fmtDateTime(o.created_at)),
      h('dt', null, 'Metadata version'), h('dd', null, String(o.version)),
      h('dt', null, 'Chunk list hash'), h('dd', null, o.checksum ? hashChip(o.checksum, 16) : '-'),
      h('dt', null, 'Custom metadata'), h('dd', null, meta.length ? h('span', { class: 'replicas' }, meta.map(([k, v]) => pill('muted', `${k} = ${v}`))) : h('span', { class: 'muted' }, 'none'))),
    h('h4', { style: { margin: '12px 0 6px', fontSize: '13px' } }, 'Chunks'),
    o.chunks.length ? chunkTable(o) : h('p', { class: 'hint' }, 'This object has no chunks.'));
}

function buildCard(o, expanded) {
  const head = h('button', { class: 'obj-head', type: 'button', 'aria-expanded': 'false' });
  const detail = h('div');
  const metaInput = h('input', { type: 'text', placeholder: 'key=value', 'aria-label': 'Add custom metadata as key=value', style: { padding: '5px 8px', border: '1px solid var(--border-strong)', borderRadius: '7px', background: 'var(--surface-1)', color: 'var(--text)', font: 'inherit' } });
  const dlBtn = h('button', { class: 'btn small', type: 'button' }, icon('download', 'glyph', 14), 'Download');
  const delBtn = h('button', { class: 'btn small danger', type: 'button' }, icon('trash', 'glyph', 14), 'Delete');
  const metaBtn = h('button', { class: 'btn small', type: 'button' }, icon('plus', 'glyph', 14), 'Add metadata');
  const actions = h('div', { class: 'obj-actions' }, dlBtn, delBtn, metaInput, metaBtn);
  const body = h('div', { class: 'obj-body' }, actions, detail);
  const el = h('article', { class: 'obj', dataset: { id: o.object_id } }, head, body);
  Object.assign(el, { head, body, detail, dlBtn, delBtn, metaBtn, metaInput, sig: '', current: o });

  head.addEventListener('click', () => {
    const open = head.getAttribute('aria-expanded') !== 'true';
    head.setAttribute('aria-expanded', String(open));
    body.hidden = !open;
    if (open) expanded.add(o.object_id); else expanded.delete(o.object_id);
    updateCard(el, el.current, expanded);
  });
  dlBtn.addEventListener('click', async () => {
    dlBtn.disabled = true;
    const t0 = performance.now();
    try {
      const blob = await api.download(el.current.object_id, newTrace());
      const url = URL.createObjectURL(blob);
      const a = h('a', { href: url, download: el.current.name });
      document.body.append(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
      toast(`Downloaded ${el.current.name} (${fmtBytes(blob.size)}) in ${Math.round(performance.now() - t0)} ms. Every chunk was SHA-256 verified; see the event log.`, 'ok', 6000);
    } catch (e) {
      toast(`Download failed: ${e.message}`, 'error', 7000);
    } finally {
      dlBtn.disabled = false;
    }
  });
  delBtn.addEventListener('click', async () => {
    if (!window.confirm(`Delete ${el.current.name}? Chunks that no other object uses are removed from the workers.`)) return;
    try {
      await api.deleteObject(el.current.object_id, newTrace());
      toast(`Deleted ${el.current.name}`, 'ok');
      refreshNow();
    } catch (e) {
      toast(`Delete failed: ${e.message}`, 'error', 7000);
    }
  });
  metaBtn.addEventListener('click', async () => {
    let patch;
    try {
      patch = parseMetadata(metaInput.value);
      if (!Object.keys(patch).length) throw new Error('type key=value first');
    } catch (e) {
      toast(`Metadata: ${e.message}`, 'error');
      return;
    }
    try {
      const { etag } = await api.metadata(el.current.object_id);
      await api.patchMetadata(el.current.object_id, patch, etag);
      metaInput.value = '';
      toast('Metadata updated without re-uploading the object', 'ok');
      refreshNow();
    } catch (e) {
      toast(`Metadata update failed: ${e.message}`, 'error', 7000);
    }
  });
  return el;
}

function updateCard(el, o, expanded) {
  el.current = o;
  const open = expanded.has(o.object_id);
  el.head.setAttribute('aria-expanded', String(open));
  el.body.hidden = !open;
  const [tone, summary] = chunkSummary(o);
  el.head.replaceChildren(
    h('span', { class: 'obj-name' }, icon('layers', 'glyph', 16), h('span', { style: { overflow: 'hidden', textOverflow: 'ellipsis' } }, o.name)),
    objectStatusPill(o.status),
    h('span', { class: 'num sub' }, fmtBytes(o.size_bytes)),
    h('span', { class: 'sub' }, plural(o.chunk_count, 'chunk')),
    h('span', { 'data-tone': tone, class: 'sub' }, pill(tone, summary)));
  el.delBtn.disabled = o.status !== 'COMMITTED';
  el.dlBtn.disabled = o.status !== 'COMMITTED';
  el.metaBtn.disabled = o.status !== 'COMMITTED';
  if (!open) return;
  const sig = JSON.stringify(o);
  if (sig !== el.sig) {
    el.sig = sig;
    el.detail.replaceChildren(details(o));
  }
}

export function mount(root) {
  const expanded = new Set();
  const list = h('div');
  const empty = h('div', { class: 'empty' }, 'No objects yet. Store a file on the first tab and it appears here.');
  root.append(
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'Objects and their chunks'), h('p', null, 'click an object to see where each chunk lives')),
      list, empty),
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'The states you will see')),
      h('div', { class: 'grid2' },
        h('div', null, h('h4', { style: { marginBottom: '6px' } }, 'Object'),
          h('ul', { class: 'steps-list' },
            h('li', null, objectStatusPill('STAGED'), ' the upload is running; readers cannot see it.'),
            h('li', null, objectStatusPill('COMMITTED'), ' every chunk is stored twice; downloads work.'),
            h('li', null, objectStatusPill('FAILED'), ' the upload broke and was rolled back.'),
            h('li', null, objectStatusPill('DELETED'), ' removed; its unused chunks are being freed.'))),
        h('div', null, h('h4', { style: { marginBottom: '6px' } }, 'Chunk'),
          h('ul', { class: 'steps-list' },
            h('li', null, chunkStatePill('PENDING'), ' registered, not yet on both workers.'),
            h('li', null, chunkStatePill('DURABLE'), ' on both workers and recorded.'),
            h('li', null, chunkStatePill('PENDING_DELETE'), ' nothing uses it any more; being removed.'))))));

  return {
    update(s) {
      const objs = s.objects;
      empty.hidden = objs.length > 0;
      reconcile(list, objs, (o) => o.object_id, (o) => buildCard(o, expanded), (el, o) => updateCard(el, o, expanded));
    },
  };
}
