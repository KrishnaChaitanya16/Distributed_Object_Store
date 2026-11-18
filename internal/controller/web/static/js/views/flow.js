// Upload and flow: drop a file, then watch it travel through the controller, the chunker, the Bloom filter, the hash ring and the workers.
import { h, fmtBytes, hex32, toast, plural, clear } from '../util.js';
import { icon } from '../icons.js';
import { uploadFile, newTrace, parseMetadata } from '../api.js';
import { onEvent, refreshNow } from '../store.js';
import { narration } from '../describe.js';
import { workerChip, hashChip, pill, chunkStatePill, objectStatusPill } from '../ui.js';

const STAGES = [
  ['Client', 'Your browser sends the file as multipart form-data.'],
  ['Controller', 'Receives it and records a STAGED object in the metadata store.'],
  ['Chunk + SHA-256', 'Cuts 1 MiB chunks. The hash of a chunk is its name.'],
  ['Bloom filter', 'Quick "seen this chunk before?" check.'],
  ['Hash ring', 'Maps the chunk hash to a primary and a secondary worker.'],
  ['Workers', 'Both workers store the chunk with a CRC32C checksum.'],
  ['Chunk state', 'PENDING becomes DURABLE once both copies exist.'],
  ['Commit', 'The object flips from STAGED to COMMITTED atomically.'],
];

const KIND_STAGE = { 'upload.start': 1, 'upload.chunk': 2, 'upload.bloom': 3, 'upload.place': 4, 'upload.push': 5, 'upload.dedup': 5, 'upload.durable': 6, 'upload.commit': 7 };
const SPEEDS = { narrated: 600, live: 0 };
const MAX_ROWS = 40;
const TEST_SIZES = [['1', '1 MiB (1 chunk)'], ['2.5', '2.5 MiB (3 chunks)'], ['5', '5 MiB (5 chunks)'], ['10', '10 MiB (10 chunks)']];

function newRun(id, x) {
  return { id, name: x.name || 'upload', size: x.size, status: 'STAGED', chunks: new Map(), commit: null, failed: false, maxStage: 1 };
}

function chunkOf(run, index) {
  if (!run.chunks.has(index)) run.chunks.set(index, { index, pushed: {}, state: null });
  return run.chunks.get(index);
}

function makeRandomFile(mib, name) {
  const bytes = Math.round(mib * 1048576);
  const buf = new Uint8Array(bytes);
  for (let o = 0; o < bytes; o += 65536) crypto.getRandomValues(buf.subarray(o, Math.min(o + 65536, bytes)));
  return new File([buf], name, { type: 'application/octet-stream' });
}

export function mount(root) {
  const runs = new Map();
  const order = [];
  let current = null;
  let activeStage = -1;
  let lastEvent = null;
  let speed = 'narrated';
  const pending = [];
  let draining = false;
  let counter = 0;
  let lastTest = null;
  const items = [];
  let pumping = false;

  // ---- upload card ----
  const fileInput = h('input', { type: 'file', multiple: true, 'aria-label': 'Choose files to upload' });
  const drop = h('div', { class: 'drop', role: 'button', tabindex: '0', 'aria-label': 'Drop files here or press Enter to choose files' },
    icon('upload', '', 38),
    h('strong', null, 'Drop files here to store them'),
    h('span', { class: 'hint' }, 'or click to choose. Any file works: it is cut into 1 MiB chunks, every chunk is stored on two different workers.'),
    fileInput);
  const metaInput = h('input', { type: 'text', placeholder: 'owner=alice, project=demo', 'aria-label': 'Custom metadata as key=value pairs' });
  const sizeSel = h('select', { 'aria-label': 'Size of the generated test file' }, TEST_SIZES.map(([v, l]) => h('option', { value: v }, l)));
  sizeSel.value = '2.5';
  const againBtn = h('button', { class: 'btn small', type: 'button', disabled: true, title: 'Uploads the same bytes again to show deduplication' }, 'Upload the same file again');
  const genBtn = h('button', { class: 'btn small', type: 'button', title: 'Creates random bytes in your browser, no file needed' }, 'Generate and upload a test file');
  const queueBox = h('div', { class: 'queue', 'aria-live': 'polite' });
  const uploadCard = h('div', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', null, 'Store a file'), h('p', null, 'Everything below reacts to real events from the controller.')),
    drop,
    h('div', { class: 'upload-opts' },
      h('label', null, 'Custom metadata (optional)', metaInput),
      h('label', null, 'Test file', sizeSel), genBtn, againBtn),
    queueBox);

  // ---- pipeline card ----
  const stageEls = STAGES.map(([name, desc], i) => {
    const stat = h('div', { class: 'stage-stat' });
    const el = h('div', { class: 'stage-card', dataset: { stage: i } },
      h('span', { class: 'stage-n' }, String(i + 1)), h('h4', null, name), h('p', null, desc), stat);
    el.stat = stat;
    return el;
  });
  const capN = h('span', { class: 'cap-n' }, '?');
  const capT = h('div', { class: 'cap-t' }, 'Nothing is happening yet. Drop a file above, or press "Generate and upload a test file", and each step lights up here with an explanation.');
  const speedBox = h('div', { class: 'speed', role: 'group', 'aria-label': 'Playback speed' },
    h('button', { type: 'button', 'aria-pressed': 'true', dataset: { s: 'narrated' }, title: 'Show each step for about half a second so it can be explained' }, 'Narrated'),
    h('button', { type: 'button', 'aria-pressed': 'false', dataset: { s: 'live' }, title: 'Show steps as fast as they really happen' }, 'Live'));
  speedBox.addEventListener('click', (e) => {
    const b = e.target.closest('[data-s]');
    if (!b) return;
    speed = b.dataset.s;
    for (const x of speedBox.children) x.setAttribute('aria-pressed', String(x === b));
  });
  const pipeCard = h('div', { class: 'card' },
    h('div', { class: 'card-head' }, h('h2', null, 'What happens to your file'), speedBox),
    h('div', { class: 'pipe', role: 'list' }, stageEls.map((e) => { e.setAttribute('role', 'listitem'); return e; })),
    h('div', { class: 'caption', 'aria-live': 'polite' }, capN, capT));

  // ---- run card ----
  const runHead = h('div', { class: 'card-head' });
  const runBody = h('div');
  const runCard = h('div', { class: 'card' }, runHead, runBody);

  root.append(uploadCard, pipeCard, runCard);

  // ---- uploads ----
  function renderQueue() {
    clear(queueBox);
    for (const it of items.slice(-4)) {
      const bar = h('span', { style: { width: `${Math.round(it.progress * 100)}%` } });
      let status;
      if (it.state === 'done') status = h('span', null, pill('good', 'Stored'), ' ', hashChip(it.objectId));
      else if (it.state === 'error') status = pill('critical', it.error);
      else if (it.state === 'uploading') status = pill('warning', `Sending ${Math.round(it.progress * 100)}%`);
      else status = pill('muted', 'Waiting');
      queueBox.append(h('div', { class: 'queue-item' },
        h('span', null, h('b', null, it.file.name), ' ', h('span', { class: 'sub' }, fmtBytes(it.file.size))), status,
        it.state === 'uploading' ? h('div', { class: 'bar' }, bar) : null));
    }
  }

  async function pump() {
    if (pumping) return;
    pumping = true;
    while (items.some((i) => i.state === 'waiting')) {
      const it = items.find((i) => i.state === 'waiting');
      it.state = 'uploading';
      renderQueue();
      renderPipeline();
      try {
        const { status, body } = await uploadFile(it.file, { trace: newTrace(), metadata: it.metadata, onProgress: (p) => { it.progress = p; renderQueue(); } });
        if (status === 201) {
          it.state = 'done';
          it.objectId = body.object_id;
        } else {
          it.state = 'error';
          it.error = body.error || `HTTP ${status}`;
          toast(`Upload of ${it.file.name} failed: ${it.error}`, 'error', 7000);
        }
      } catch (e) {
        it.state = 'error';
        it.error = e.message;
        toast(`Upload of ${it.file.name} failed: ${e.message}`, 'error', 7000);
      }
      renderQueue();
      renderPipeline();
      refreshNow();
    }
    pumping = false;
  }

  function enqueue(files) {
    let metadata;
    try {
      metadata = parseMetadata(metaInput.value);
    } catch (e) {
      toast(`Custom metadata: ${e.message}`, 'error', 6000);
      return;
    }
    for (const file of files) items.push({ file, state: 'waiting', progress: 0, metadata });
    renderQueue();
    pump();
  }

  drop.addEventListener('click', () => fileInput.click());
  drop.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); fileInput.click(); } });
  fileInput.addEventListener('change', () => { if (fileInput.files.length) enqueue([...fileInput.files]); fileInput.value = ''; });
  for (const t of ['dragenter', 'dragover']) drop.addEventListener(t, (e) => { e.preventDefault(); drop.classList.add('over'); });
  for (const t of ['dragleave', 'drop']) drop.addEventListener(t, (e) => { e.preventDefault(); drop.classList.remove('over'); });
  drop.addEventListener('drop', (e) => { const files = [...(e.dataTransfer?.files || [])]; if (files.length) enqueue(files); });
  window.addEventListener('dragover', (e) => e.preventDefault());
  window.addEventListener('drop', (e) => e.preventDefault());
  genBtn.addEventListener('click', () => {
    counter += 1;
    lastTest = makeRandomFile(Number(sizeSel.value), `test-${counter}-${sizeSel.value}MiB.bin`);
    againBtn.disabled = false;
    enqueue([lastTest]);
  });
  againBtn.addEventListener('click', () => {
    if (!lastTest) return;
    counter += 1;
    enqueue([new File([lastTest], lastTest.name.replace(/\.bin$/, `-again${counter}.bin`), { type: lastTest.type })]);
  });

  // ---- events ----
  onEvent((ev) => {
    if (!ev.kind.startsWith('upload.')) return;
    pending.push(ev);
    drain();
  });

  function drain() {
    if (draining) return;
    draining = true;
    const next = () => {
      for (;;) {
        const ev = pending.shift();
        if (!ev) { draining = false; return; }
        apply(ev);
        const delay = pending.length > 30 ? 0 : SPEEDS[speed];
        if (delay > 0) { setTimeout(next, delay); return; }
      }
    };
    next();
  }

  function runFor(id, x) {
    if (id && runs.has(id)) return runs.get(id);
    const run = newRun(id, x || {});
    if (id) runs.set(id, run);
    order.push(run);
    if (order.length > 6) order.shift();
    return run;
  }

  function apply(ev) {
    const x = ev.fields || {};
    const run = ev.kind === 'upload.start' ? runFor(x.object_id, x) : runFor(x.object_id, {});
    if (ev.kind === 'upload.start') revealPipeline();
    current = run;
    lastEvent = ev;
    const stage = KIND_STAGE[ev.kind];
    if (stage !== undefined) {
      activeStage = stage;
      run.maxStage = Math.max(run.maxStage, stage);
    }
    const c = typeof x.index === 'number' ? chunkOf(run, x.index) : null;
    switch (ev.kind) {
      case 'upload.chunk': Object.assign(c, { chunk_id: x.chunk_id, size: x.size, offset: x.offset }); break;
      case 'upload.bloom': c.bloom = x.verdict; break;
      case 'upload.place': Object.assign(c, { ring_pos: x.ring_pos, primary: x.primary, secondary: x.secondary, state: 'PENDING' }); break;
      case 'upload.push': c.pushed[x.role] = x.ok ? 'ok' : 'bad'; break;
      case 'upload.dedup': c.dup = true; c.state = 'DURABLE'; break;
      case 'upload.durable': c.state = 'DURABLE'; break;
      case 'upload.commit': run.status = 'COMMITTED'; run.commit = x; activeStage = -1; break;
      case 'upload.fail': run.status = 'FAILED'; run.failed = true; break;
      case 'upload.error': run.failed = true; break;
      default: break;
    }
    renderAll();
  }

  // a new run scrolls the pipeline into view so the narration is visible without hunting for it
  function revealPipeline() {
    if (root.hidden) return;
    const r = pipeCard.getBoundingClientRect();
    if (r.top < 0 || r.bottom > window.innerHeight) pipeCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  // ---- rendering ----
  function renderPipeline() {
    const uploading = items.some((i) => i.state === 'uploading');
    const run = current;
    stageEls.forEach((el, i) => {
      const done = run && (run.status === 'COMMITTED' || run.maxStage > i) && !(i === 0 && !run);
      el.classList.toggle('done', Boolean(done) && !run?.failed);
      el.classList.toggle('active', (i === activeStage && !(run && run.status !== 'STAGED')) || (i === 0 && uploading && (!run || run.status !== 'STAGED')));
      el.classList.toggle('failed', Boolean(run && run.failed && i === Math.max(1, activeStage)));
      el.stat.textContent = '';
    });
    if (!run) return;
    const chunks = [...run.chunks.values()];
    const dups = chunks.filter((c) => c.dup).length;
    stageEls[1].stat.textContent = run.id ? run.id.slice(0, 8) : '';
    stageEls[2].stat.textContent = chunks.length ? plural(chunks.filter((c) => c.chunk_id).length, 'chunk') : '';
    stageEls[3].stat.textContent = chunks.some((c) => c.bloom) ? `${chunks.filter((c) => c.bloom === 'definitely_new').length} new / ${dups} dup` : '';
    const placed = chunks.filter((c) => c.primary).slice(-1)[0];
    stageEls[4].stat.textContent = placed ? `${placed.primary} + ${placed.secondary}` : '';
    stageEls[5].stat.textContent = chunks.some((c) => c.pushed.primary || c.pushed.secondary) ? plural(chunks.reduce((n, c) => n + Object.values(c.pushed).filter((v) => v === 'ok').length, 0), 'write') : '';
    stageEls[6].stat.textContent = chunks.length ? `${chunks.filter((c) => c.state === 'DURABLE').length} / ${chunks.length} durable` : '';
    stageEls[7].stat.textContent = run.status === 'COMMITTED' ? `done in ${run.commit.ms} ms` : run.status === 'FAILED' ? 'rolled back' : '';
    stageEls[0].stat.textContent = run.size !== undefined ? fmtBytes(run.size) : '';
  }

  function renderCaption() {
    if (!lastEvent) return;
    const nar = narration(lastEvent);
    if (!nar) return;
    capN.textContent = nar[0] === 0 ? '!' : String(nar[0] + 1);
    clear(capT);
    capT.append(...nar[1]);
  }

  function stepDot(state, label) {
    return h('span', { class: `step ${state === 'ok' ? 'on' : state === 'bad' ? 'bad' : state === 'dup' ? 'dup' : ''}`, title: label }, state === 'ok' || state === 'dup' ? '✓' : state === 'bad' ? '×' : '');
  }

  function renderRun() {
    clear(runHead);
    clear(runBody);
    const run = current;
    if (!run) {
      runHead.append(h('h2', null, 'Chunks of the latest upload'), h('p', null, 'appears when a file is stored'));
      runBody.append(h('div', { class: 'empty' }, 'No upload seen yet. Drop a file above.'));
      return;
    }
    const picker = h('div', { style: { display: 'flex', gap: '6px', flexWrap: 'wrap' } },
      order.map((r) => h('button', { class: 'btn small', type: 'button', 'aria-pressed': String(r === run), style: r === run ? { borderColor: 'var(--s1)' } : {},
        onclick: () => { current = r; renderAll(); } }, r.name)));
    runHead.append(h('div', null, h('h2', null, run.name), h('p', null, `${fmtBytes(run.size)} - `, objectStatusPill(run.status), run.id ? [' ', hashChip(run.id)] : '')), order.length > 1 ? picker : '');
    const chunks = [...run.chunks.values()].sort((a, b) => a.index - b.index);
    const shown = chunks.slice(0, MAX_ROWS);
    const rows = shown.map((c) => h('tr', null,
      h('td', { class: 'num' }, String(c.index + 1)),
      h('td', null, c.chunk_id ? hashChip(c.chunk_id, 10) : '...'),
      h('td', { class: 'num' }, c.size !== undefined ? fmtBytes(c.size) : ''),
      h('td', null, c.bloom ? pill(c.bloom === 'definitely_new' ? 'good' : c.bloom === 'true_positive' ? 'muted' : 'serious', c.bloom === 'definitely_new' ? 'new' : c.bloom === 'true_positive' ? 'seen' : 'false positive') : ''),
      h('td', { class: 'mono' }, c.ring_pos !== undefined ? hex32(c.ring_pos) : ''),
      h('td', null, c.primary ? [workerChip(c.primary), ' ', workerChip(c.secondary)] : ''),
      h('td', null, h('span', { class: 'steps' },
        stepDot(c.chunk_id ? 'ok' : '', 'hashed'), stepDot(c.bloom ? 'ok' : '', 'bloom checked'), stepDot(c.primary ? 'ok' : '', 'placed on the ring'),
        stepDot(c.dup ? 'dup' : c.pushed.primary || '', c.dup ? 'duplicate: nothing sent' : 'primary written'),
        stepDot(c.dup ? 'dup' : c.pushed.secondary || '', c.dup ? 'duplicate: nothing sent' : 'secondary written'),
        stepDot(c.state === 'DURABLE' ? 'ok' : '', 'durable'))),
      h('td', null, c.dup ? pill('muted', 'Duplicate', 'Identical chunk already stored') : c.state ? chunkStatePill(c.state) : '')));
    runBody.append(h('div', { class: 'tbl-wrap' }, h('table', { class: 'tbl' },
      h('thead', null, h('tr', null, ['#', 'SHA-256 (chunk id)', 'Size', 'Bloom filter', 'Ring position', 'Primary / secondary', 'Hashed, checked, placed, 2 writes, durable', 'Chunk state'].map((t) => h('th', null, t)))),
      h('tbody', null, rows))));
    if (chunks.length > MAX_ROWS) runBody.append(h('p', { class: 'hint' }, `Showing the first ${MAX_ROWS} of ${chunks.length} chunks.`));
  }

  function renderAll() {
    renderPipeline();
    renderCaption();
    renderRun();
  }

  renderAll();
  return { update() { renderPipeline(); } };
}
