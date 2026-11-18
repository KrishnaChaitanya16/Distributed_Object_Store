// Turns raw controller events into plain-English steps for the event log and the flow narration.
import { h, fmtBytes, shortId, hex32 } from './util.js';
import { workerChip, hashChip } from './ui.js';

export const CATEGORIES = [
  { id: 'upload', label: 'Upload', icon: 'upload' },
  { id: 'download', label: 'Download', icon: 'download' },
  { id: 'worker', label: 'Workers', icon: 'server' },
  { id: 'rebalance', label: 'Rebalance', icon: 'layers' },
  { id: 'reconcile', label: 'Reconcile', icon: 'broom' },
  { id: 'scrub', label: 'Scrubber', icon: 'shield' },
  { id: 'demo', label: 'Demo', icon: 'flask' },
];

export function categoryOf(kind) {
  const p = kind.split('.')[0];
  if (p === 'gc') return 'reconcile';
  if (p === 'object') return 'upload';
  return CATEGORIES.some((c) => c.id === p) ? p : 'upload';
}

const VERDICT = {
  definitely_new: ['Definitely not stored yet', 'tone-good'],
  true_positive: ['Possibly stored, and the store confirmed it (a duplicate)', 'tone-good'],
  false_positive: ['Said "maybe" but the store had never seen it (false positive)', 'tone-serious'],
};

const w = (id) => (id ? workerChip(id) : '');
const c = (id) => (id ? hashChip(id) : '');
const f = (ev) => ev.fields || {};

// describe returns {tone, icon, title, detail:[nodes]} for one event.
export function describe(ev) {
  const x = f(ev);
  switch (ev.kind) {
    case 'upload.start':
      return { tone: 'good', icon: 'upload', title: 'Upload received', detail: [`${x.name || 'file'} (${fmtBytes(x.size)})`, ' - manifest saved as STAGED'] };
    case 'upload.chunk':
      return { tone: 'muted', icon: 'hash', title: `Chunk ${x.index + 1} cut and hashed`, detail: [fmtBytes(x.size), ' at offset ', fmtBytes(x.offset), ' - SHA-256 ', c(x.chunk_id)] };
    case 'upload.bloom': {
      const [text] = VERDICT[x.verdict] || [x.verdict];
      return { tone: x.verdict === 'false_positive' ? 'serious' : 'muted', icon: 'filter', title: `Bloom filter, chunk ${x.index + 1}`, detail: [text] };
    }
    case 'upload.place':
      return { tone: 'muted', icon: 'ring', title: `Ring placement, chunk ${x.index + 1}`, detail: ['position ', hex32(x.ring_pos), ' - primary ', w(x.primary), ' secondary ', w(x.secondary)] };
    case 'upload.push':
      return x.ok
        ? { tone: 'good', icon: 'server', title: `Chunk ${x.index + 1} stored on the ${x.role}`, detail: [w(x.worker), ` in ${x.ms} ms`] }
        : { tone: 'critical', icon: 'cross', title: `Chunk ${x.index + 1}: write to the ${x.role} failed`, detail: [w(x.worker), ' - ', x.error || 'unknown error'] };
    case 'upload.dedup':
      return { tone: 'good', icon: 'layers', title: `Chunk ${x.index + 1} is a duplicate`, detail: ['Identical content is already stored, so nothing was sent to the workers. ', c(x.chunk_id)] };
    case 'upload.durable':
      return { tone: 'good', icon: 'check', title: `Chunk ${x.index + 1} is DURABLE`, detail: ['Both copies written and recorded. ', c(x.chunk_id)] };
    case 'upload.commit':
      return { tone: 'good', icon: 'check', title: 'Object COMMITTED', detail: [`${x.chunks} chunk${x.chunks === 1 ? '' : 's'}, ${fmtBytes(x.bytes)} in ${x.ms} ms - now readable`] };
    case 'upload.error':
      return { tone: 'critical', icon: 'cross', title: 'Upload error', detail: [x.error || 'a chunk could not be stored'] };
    case 'upload.fail':
      return { tone: 'critical', icon: 'cross', title: 'Upload rolled back', detail: ['Object marked FAILED; the garbage collector will reclaim its chunks.'] };
    case 'object.delete':
      return { tone: 'muted', icon: 'trash', title: 'Object deleted', detail: [`${x.name || shortId(x.object_id)}: ${x.chunks_deleted} of ${x.chunks} chunks no longer used by anything and removed from the workers`] };
    case 'download.start':
      return { tone: 'good', icon: 'download', title: 'Download requested', detail: [`${x.name || shortId(x.object_id)} (${fmtBytes(x.bytes)}, ${x.chunks} chunk${x.chunks === 1 ? '' : 's'})`] };
    case 'download.fetch':
      if (x.ok) return { tone: 'good', icon: 'check', title: `Chunk fetched from the ${x.role}`, detail: [w(x.worker), ' - SHA-256 verified in ', `${x.ms} ms `, c(x.chunk_id)] };
      return { tone: x.cancelled ? 'muted' : 'serious', icon: x.cancelled ? 'dot' : 'warn', title: x.cancelled ? `Fetch from the ${x.role} cancelled` : `Fetch from the ${x.role} failed`, detail: [w(x.worker), ' - ', x.cancelled ? 'the other replica answered first' : (x.error || 'error')] };
    case 'download.corrupt':
      return { tone: 'critical', icon: 'cross', title: 'Corrupt copy detected', detail: [w(x.worker), ' sent bytes whose SHA-256 is ', hashChip(x.got), ' instead of ', c(x.chunk_id), ' - falling back to the other copy'] };
    case 'download.hedge':
      return { tone: 'warning', icon: 'bolt', title: 'Hedged read', detail: ['Primary ', w(x.primary), ` did not answer within ${x.threshold_ms} ms, so the secondary `, w(x.secondary), ' was asked too'] };
    case 'download.repair':
      return x.ok
        ? { tone: 'good', icon: 'wrench', title: 'Read-repair done', detail: ['The good copy on ', w(x.from), ' overwrote the corrupt one on ', w(x.to)] }
        : { tone: 'critical', icon: 'cross', title: 'Read-repair failed', detail: [x.error || 'could not copy the good replica back'] };
    case 'download.done':
      return { tone: 'good', icon: 'check', title: 'Download complete', detail: [`${x.chunks} chunk${x.chunks === 1 ? '' : 's'}, ${fmtBytes(x.bytes)}, every chunk verified`] };
    case 'worker.register':
      return { tone: 'good', icon: 'server', title: 'Worker registered', detail: [w(x.worker), ` at ${x.addr}`] };
    case 'worker.offline':
      return { tone: 'critical', icon: 'cross', title: 'Worker went OFFLINE', detail: [w(x.worker), ' stopped sending heartbeats, so its lease expired'] };
    case 'worker.online':
      return { tone: 'good', icon: 'check', title: 'Worker is back ONLINE', detail: [w(x.worker), ' is sending heartbeats again'] };
    case 'rebalance.copy':
      return { tone: 'good', icon: 'layers', title: 'Replica rebuilt', detail: ['Copied ', c(x.chunk_id), ' from ', w(x.from), ' to ', w(x.to), ' to replace the lost copy on ', w(x.replaced)] };
    case 'gc.restore':
      return { tone: 'good', icon: 'wrench', title: 'Missing copy restored', detail: [c(x.chunk_id), x.from ? [' copied ', w(x.from), ' to '] : ' recorded on ', w(x.to)] };
    case 'gc.excess':
      return { tone: 'good', icon: 'broom', title: 'Extra copy removed', detail: ['A stale copy of ', c(x.chunk_id), ' on ', w(x.worker), ' was not in the replica list'] };
    case 'gc.chunk_deleted':
      return { tone: 'muted', icon: 'trash', title: 'Unused chunk deleted', detail: [c(x.chunk_id), ` removed from ${x.copies} worker${x.copies === 1 ? '' : 's'}: no object references it`] };
    case 'gc.stale_upload':
      return { tone: 'warning', icon: 'clock', title: 'Abandoned upload expired', detail: [`${(x.object_ids || []).length} upload(s) idle too long were marked FAILED`] };
    case 'gc.adopt':
      return { tone: 'warning', icon: 'wrench', title: 'Chunk records rebuilt', detail: [`${(x.chunk_ids || []).length} record(s) recreated from what the workers hold`] };
    case 'gc.pass':
      return { tone: 'muted', icon: 'broom', title: 'Reconciler pass', detail: [`scanned ${x.workers_scanned} worker(s) in ${x.ms} ms: restored ${x.replicas_restored}, removed ${x.excess_deleted} extra, deleted ${x.chunks_deleted} unused`, x.lost_chunks ? ` - ${x.lost_chunks} chunk(s) LOST` : '', x.errors ? ` - ${x.errors} error(s), will retry` : ''] };
    case 'scrub.quarantine':
      return { tone: 'critical', icon: 'shield', title: 'Scrubber quarantined a corrupt copy', detail: [w(x.worker), ' CRC32C mismatch on ', c(x.chunk_id), ' - moved aside, no longer served'] };
    case 'demo.action':
      return { tone: 'warning', icon: 'flask', title: 'Demo control', detail: [ev.msg.replace(/^demo control used: /, '')] };
    default:
      return { tone: 'muted', icon: 'dot', title: ev.kind, detail: [ev.msg || ''] };
  }
}

// narration gives the plain-English caption shown above the pipeline for the pipeline step an event belongs to.
export function narration(ev) {
  const x = f(ev);
  switch (ev.kind) {
    case 'upload.start':
      return [1, [h('b', null, 'The controller received the file.'), ` It saved an object record in the metadata store (bbolt) with status STAGED: a half-finished upload is invisible to readers, so it can never be mistaken for real data.`]];
    case 'upload.chunk':
      return [2, [h('b', null, `Chunk ${x.index + 1} cut and hashed.`), ` The file is sliced into pieces of at most 1 MiB. Each piece is named by its SHA-256 hash (${shortId(x.chunk_id)}...), so identical content always has the same name.`]];
    case 'upload.bloom': {
      const why = x.verdict === 'definitely_new'
        ? 'At least one of its bits is 0, so this chunk has never been seen: no need to ask the database.'
        : x.verdict === 'true_positive'
          ? 'All its bits were set, and the metadata store confirmed it already holds this chunk.'
          : 'All its bits were set by other chunks, but the metadata store says it is new: a false positive, which only costs a lookup.';
      return [3, [h('b', null, 'Bloom filter consulted.'), ` ${why}`]];
    }
    case 'upload.place':
      return [4, [h('b', null, 'The hash ring chose the workers.'), ` The chunk hash lands at position ${hex32(x.ring_pos)} on the ring. The next virtual node clockwise belongs to ${x.primary} (primary); continuing to the next node of a different worker gives ${x.secondary} (secondary).`]];
    case 'upload.push':
      return [5, [h('b', null, x.ok ? `Chunk ${x.index + 1} written to ${x.worker}.` : `Write to ${x.worker} failed.`), x.ok ? ` The worker stores it with a CRC32C checksum and replies only after fsync, so it survives a crash.` : ` ${x.error || ''}`]];
    case 'upload.dedup':
      return [5, [h('b', null, 'Duplicate chunk skipped.'), ' The same bytes are already stored, so the object just records a reference: deduplication saves space and network.']];
    case 'upload.durable':
      return [6, [h('b', null, `Chunk ${x.index + 1} is DURABLE.`), ' Both copies are written and the chunk record flipped from PENDING to DURABLE in the metadata store.']];
    case 'upload.commit':
      return [7, [h('b', null, 'Object COMMITTED.'), ` One atomic update flipped the object from STAGED to COMMITTED and counted its chunk references. It is now visible and downloadable (${x.chunks} chunk${x.chunks === 1 ? '' : 's'}, ${x.ms} ms).`]];
    case 'upload.fail':
      return [0, [h('b', null, 'Upload rolled back.'), ' The object was marked FAILED. The reconciler will delete any chunks nothing else uses.']];
    default:
      return null;
  }
}
