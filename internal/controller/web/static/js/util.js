// Small DOM and formatting helpers. Text is always inserted as text, never as HTML.
const SVG_NS = 'http://www.w3.org/2000/svg';

function setAttrs(el, attrs) {
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === 'class') el.setAttribute('class', v);
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else if (k === 'style' && typeof v === 'object') {
      for (const [sk, sv] of Object.entries(v)) {
        if (sk.startsWith('--')) el.style.setProperty(sk, sv);
        else el.style[sk] = sv;
      }
    }
    else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
    else if (v === true) el.setAttribute(k, '');
    else el.setAttribute(k, String(v));
  }
  return el;
}

function appendKids(el, kids) {
  for (const k of kids.flat(Infinity)) {
    if (k === null || k === undefined || k === false) continue;
    el.append(k.nodeType ? k : document.createTextNode(String(k)));
  }
  return el;
}

export function h(tag, attrs, ...kids) {
  return appendKids(setAttrs(document.createElement(tag), attrs), kids);
}

export function svg(tag, attrs, ...kids) {
  return appendKids(setAttrs(document.createElementNS(SVG_NS, tag), attrs), kids);
}

export function clear(el) {
  el.replaceChildren();
  return el;
}

// reconcile keeps one element per key (create builds it, update fills it, for new and old alike) so hover, focus and animations survive refreshes.
export function reconcile(container, items, keyOf, create, update) {
  const existing = container._items || new Map();
  const next = new Map();
  let prev = null;
  for (const item of items) {
    const key = keyOf(item);
    let el = existing.get(key);
    if (el) {
      existing.delete(key);
      if (update) update(el, item);
    } else {
      el = create(item);
      if (update) update(el, item);
    }
    next.set(key, el);
    const anchor = prev ? prev.nextSibling : container.firstChild;
    if (anchor !== el) container.insertBefore(el, anchor);
    prev = el;
  }
  for (const el of existing.values()) el.remove();
  container._items = next;
  return container;
}

export function fmtBytes(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return `${v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2)} ${units[i]}`;
}

export function fmtInt(n) {
  return Number(n || 0).toLocaleString('en-US');
}

export function fmtPct(x, digits = 1) {
  if (x === null || x === undefined || Number.isNaN(x)) return '-';
  return `${(x * 100).toFixed(digits)}%`;
}

export function fmtClock(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

export function fmtAgo(ms) {
  if (ms === null || ms === undefined || ms < 0) return 'never';
  if (ms < 1000) return `${ms} ms ago`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(1)} s ago`;
  return `${Math.floor(s / 60)} min ago`;
}

export function fmtDateTime(iso) {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? '' : d.toLocaleString();
}

export const shortId = (id, n = 8) => (id ? String(id).slice(0, n) : '');

export function hex32(n) {
  return `0x${(n >>> 0).toString(16).padStart(8, '0')}`;
}

export const clamp = (x, lo, hi) => Math.min(hi, Math.max(lo, x));

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// RING is the number of positions on the hash ring (the hash space is 0 up to 2^32).
export const RING = 2 ** 32;

export function plural(n, one, many) {
  return `${fmtInt(n)} ${n === 1 ? one : many || `${one}s`}`;
}

export async function copyText(text, toastFn) {
  try {
    await navigator.clipboard.writeText(text);
    if (toastFn) toastFn('Copied to clipboard');
  } catch {
    if (toastFn) toastFn('Copy is not available in this browser', 'error');
  }
}

// ---- tooltip ----
let tipEl = null;
function ensureTip() {
  if (!tipEl) tipEl = document.getElementById('tooltip');
  return tipEl;
}

export function showTip(evt, title, rows) {
  const el = ensureTip();
  if (!el) return;
  clear(el);
  if (title) el.append(h('strong', null, title));
  for (const [k, v] of rows || []) el.append(h('div', { class: 'tt-row' }, h('span', null, k), h('b', null, v)));
  el.hidden = false;
  moveTip(evt);
}

export function moveTip(evt) {
  const el = ensureTip();
  if (!el || el.hidden) return;
  const pad = 14;
  const w = el.offsetWidth;
  const hgt = el.offsetHeight;
  let x = evt.clientX + pad;
  let y = evt.clientY + pad;
  if (x + w > window.innerWidth - 8) x = evt.clientX - w - pad;
  if (y + hgt > window.innerHeight - 8) y = evt.clientY - hgt - pad;
  el.style.left = `${Math.max(8, x)}px`;
  el.style.top = `${Math.max(8, y)}px`;
}

export function hideTip() {
  const el = ensureTip();
  if (el) el.hidden = true;
}

// tip attaches hover and keyboard-focus tooltips; build returns [title, rows] when asked.
export function tip(el, build) {
  const show = (e) => { const [t, r] = build(); showTip(e, t, r); };
  el.addEventListener('pointerenter', show);
  el.addEventListener('pointermove', moveTip);
  el.addEventListener('pointerleave', hideTip);
  el.addEventListener('focus', () => {
    const r = el.getBoundingClientRect();
    show({ clientX: r.left + r.width / 2, clientY: r.top + r.height });
  });
  el.addEventListener('blur', hideTip);
  return el;
}

// ---- toasts ----
export function toast(message, kind = 'info', ms = 4200) {
  const box = document.getElementById('toasts');
  if (!box) return;
  const t = h('div', { class: `toast ${kind}` }, message);
  box.append(t);
  setTimeout(() => t.remove(), ms);
}
