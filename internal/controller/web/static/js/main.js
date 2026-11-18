// Bootstraps the dashboard: theme, tabs, views and the data store.
import { store, subscribe, start, connectionState } from './store.js';
import * as kpis from './views/kpis.js';
import * as eventlog from './views/eventlog.js';
import * as flow from './views/flow.js';
import * as ring from './views/ring.js';
import * as cluster from './views/cluster.js';
import * as objects from './views/objects.js';
import * as bloom from './views/bloom.js';
import * as healing from './views/healing.js';

const TABS = ['flow', 'ring', 'cluster', 'objects', 'bloom', 'healing'];

function safeStorage(op, key, value) {
  try {
    return op === 'get' ? localStorage.getItem(key) : localStorage.setItem(key, value);
  } catch {
    return null;
  }
}

function applyTheme(theme) {
  if (theme === 'light' || theme === 'dark') document.documentElement.dataset.theme = theme;
  else delete document.documentElement.dataset.theme;
}

function currentTheme() {
  return document.documentElement.dataset.theme
    || (window.matchMedia && matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
}

function setupTheme() {
  applyTheme(safeStorage('get', 'minidfs-theme'));
  document.getElementById('theme-toggle').addEventListener('click', () => {
    const next = currentTheme() === 'dark' ? 'light' : 'dark';
    applyTheme(next);
    safeStorage('set', 'minidfs-theme', next);
    refreshViews();
  });
}

const views = {};

function activeTab() {
  const t = location.hash.replace('#', '');
  return TABS.includes(t) ? t : 'flow';
}

function showTab(name, focus) {
  for (const t of TABS) {
    const btn = document.getElementById(`t-${t}`);
    const panel = document.getElementById(`tab-${t}`);
    const sel = t === name;
    btn.setAttribute('aria-selected', String(sel));
    btn.tabIndex = sel ? 0 : -1;
    panel.hidden = !sel;
  }
  if (focus) document.getElementById(`t-${name}`).focus();
  refreshViews();
}

function setupTabs() {
  const tabs = document.querySelector('.tabs');
  tabs.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-tab]');
    if (btn) location.hash = btn.dataset.tab;
  });
  tabs.addEventListener('keydown', (e) => {
    const i = TABS.indexOf(activeTab());
    let next = null;
    if (e.key === 'ArrowRight') next = TABS[(i + 1) % TABS.length];
    else if (e.key === 'ArrowLeft') next = TABS[(i + TABS.length - 1) % TABS.length];
    else if (e.key === 'Home') next = TABS[0];
    else if (e.key === 'End') next = TABS[TABS.length - 1];
    if (next) { e.preventDefault(); location.hash = next; document.getElementById(`t-${next}`).focus(); }
  });
  window.addEventListener('hashchange', () => showTab(activeTab(), false));
}

function renderConn() {
  const state = connectionState();
  const el = document.getElementById('conn');
  el.dataset.state = state;
  el.querySelector('.conn-text').textContent = state === 'live' ? 'Live' : state === 'offline' ? 'Controller unreachable' : 'Connecting';
  const demo = store.state && store.state.demo && store.state.demo.enabled;
  document.getElementById('demo-badge').hidden = !demo;
}

function refreshViews() {
  renderConn();
  const tab = activeTab();
  views.kpis.update(store);
  views.events.update(store);
  if (!store.state) return;
  if (views[tab]) views[tab].update(store);
}

function init() {
  setupTheme();
  setupTabs();
  views.kpis = kpis.mount(document.getElementById('kpis'));
  views.events = eventlog.mount(document.getElementById('rail'));
  views.flow = flow.mount(document.getElementById('tab-flow'));
  views.ring = ring.mount(document.getElementById('tab-ring'));
  views.cluster = cluster.mount(document.getElementById('tab-cluster'));
  views.objects = objects.mount(document.getElementById('tab-objects'));
  views.bloom = bloom.mount(document.getElementById('tab-bloom'));
  views.healing = healing.mount(document.getElementById('tab-healing'));
  showTab(activeTab(), false);
  subscribe(refreshViews);
  setInterval(renderConn, 1000);
}

start().catch((e) => console.error('startup failed', e)).finally(() => {
  // views mount after the first data arrives so they can draw immediately
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
});
