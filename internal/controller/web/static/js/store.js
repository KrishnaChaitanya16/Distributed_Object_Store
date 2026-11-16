// Central data store: polls the controller for state and keeps a live event stream open.
import { api } from './api.js';
import { setWorkers } from './colors.js';

const MAX_EVENTS = 1000;

export const store = {
  state: null,
  objects: [],
  events: [],
  lastSeq: 0,
  stream: 'connecting',
  lastOk: 0,
};

const subscribers = new Set();
const eventSubscribers = new Set();
let scheduled = false;

export function subscribe(fn) {
  subscribers.add(fn);
  return () => subscribers.delete(fn);
}

export function onEvent(fn) {
  eventSubscribers.add(fn);
  return () => eventSubscribers.delete(fn);
}

function notify() {
  if (scheduled) return;
  scheduled = true;
  requestAnimationFrame(() => {
    scheduled = false;
    for (const fn of subscribers) {
      try { fn(store); } catch (e) { console.error('view update failed', e); }
    }
  });
}

export function connectionState() {
  const fresh = Date.now() - store.lastOk < 6000;
  if (!fresh) return 'offline';
  return store.stream === 'live' ? 'live' : 'connecting';
}

async function pollState() {
  try {
    store.state = await api.state();
    store.lastOk = Date.now();
    setWorkers(store.state.workers.map((w) => w.id));
  } catch (e) {
    console.warn('state poll failed', e);
  }
  notify();
}

async function pollObjects() {
  try {
    store.objects = (await api.objects()).objects;
  } catch (e) {
    console.warn('objects poll failed', e);
  }
  notify();
}

export function refreshNow() {
  pollState();
  pollObjects();
}

function accept(ev) {
  if (ev.seq <= store.lastSeq) return false;
  store.lastSeq = ev.seq;
  store.events.push(ev);
  if (store.events.length > MAX_EVENTS) store.events.splice(0, store.events.length - MAX_EVENTS);
  return true;
}

let source = null;

function openStream() {
  if (source) source.close();
  store.stream = 'connecting';
  source = new EventSource(`/ui/api/events/stream?since=${store.lastSeq}`);
  source.onopen = () => { store.stream = 'live'; notify(); };
  source.onmessage = (msg) => {
    let ev;
    try { ev = JSON.parse(msg.data); } catch { return; }
    if (!accept(ev)) return;
    for (const fn of eventSubscribers) {
      try { fn(ev); } catch (e) { console.error('event handler failed', e); }
    }
    notify();
  };
  source.onerror = () => {
    store.stream = 'connecting';
    source.close();
    notify();
    setTimeout(openStream, 1500);
  };
}

export async function start() {
  await Promise.all([pollState(), pollObjects()]);
  try {
    const hist = await api.events(0, 400);
    for (const ev of hist.events) accept(ev);
    store.lastSeq = Math.max(store.lastSeq, hist.last_seq);
  } catch (e) {
    console.warn('event history unavailable', e);
  }
  notify();
  openStream();
  setInterval(pollState, 1500);
  setInterval(pollObjects, 2500);
  document.addEventListener('visibilitychange', () => { if (!document.hidden) refreshNow(); });
}
