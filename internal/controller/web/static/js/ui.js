// Shared building blocks: status pills, state badges, worker chips and hash chips.
import { h, shortId, copyText, toast } from './util.js';
import { icon, TONE_ICON } from './icons.js';
import { workerColor } from './colors.js';

export function pill(tone, label, title) {
  return h('span', { class: 'pill', 'data-tone': tone, title }, icon(TONE_ICON[tone] || 'dot', 'glyph'), label);
}

export const HEALTH = {
  ONLINE: ['good', 'Online'],
  OFFLINE: ['critical', 'Offline'],
  UNKNOWN: ['warning', 'Unknown'],
};

export function healthPill(status) {
  const [tone, label] = HEALTH[status] || HEALTH.UNKNOWN;
  return pill(tone, label, `Worker status: ${status}`);
}

export const OBJECT_STATUS = {
  COMMITTED: ['good', 'Committed', 'The object is fully stored and readable.'],
  STAGED: ['warning', 'Staged', 'The upload is still in progress; the object is not visible to readers yet.'],
  FAILED: ['critical', 'Failed', 'The upload failed and was rolled back; the garbage collector will clean it up.'],
  DELETED: ['muted', 'Deleted', 'Deleted; its chunks are being released.'],
};

export function objectStatusPill(status) {
  const [tone, label, title] = OBJECT_STATUS[status] || ['muted', status, ''];
  return pill(tone, label, title);
}

export const CHUNK_STATE = {
  DURABLE: ['good', 'Durable', 'Written to both workers and recorded.'],
  PENDING: ['warning', 'Pending', 'Registered but not yet on both workers.'],
  PENDING_DELETE: ['serious', 'Pending delete', 'No object uses this chunk; it is being removed from the workers.'],
  NO_RECORD: ['critical', 'No record', 'The object refers to this chunk but the metadata record is missing.'],
};

export function chunkStatePill(state) {
  const [tone, label, title] = CHUNK_STATE[state] || ['muted', state, ''];
  return pill(tone, label, title);
}

export function workerChip(id, opts = {}) {
  const { dead = false, mark = '', title } = opts;
  return h('span', { class: `wchip${dead ? ' dead' : ''}`, style: { '--wc': workerColor(id) }, title: title || (dead ? `${id} is offline` : id) },
    h('i'), id, mark ? h('span', { class: 'mark' }, mark) : null);
}

export function hashChip(id, n = 8) {
  return h('span', { class: 'hash', title: `SHA-256 ${id} (click to copy)`, role: 'button', tabindex: '0',
    onclick: () => copyText(id, toast),
    onkeydown: (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); copyText(id, toast); } } }, shortId(id, n));
}

export function card(title, subtitle, ...body) {
  return h('div', { class: 'card' },
    title ? h('div', { class: 'card-head' }, h('h2', null, title), subtitle ? h('p', null, subtitle) : null) : null,
    ...body);
}
