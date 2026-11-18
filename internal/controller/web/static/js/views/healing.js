// Failure and healing: what the system does when a worker dies or a copy is damaged, reconstructed from real events.
import { h, clear, fmtClock, toast, shortId, plural, tip } from '../util.js';
import { api } from '../api.js';
import { refreshNow } from '../store.js';
import { naturalCompare } from '../colors.js';
import { icon, TONE_ICON } from '../icons.js';

const lastOf = (events, pred) => {
  for (let i = events.length - 1; i >= 0; i--) if (pred(events[i])) return events[i];
  return null;
};

// analyzeIncident rebuilds the story of the most recent worker failure from the event log and the current cluster state.
export function analyzeIncident(events, chunks) {
  const off = lastOf(events, (e) => e.kind === 'worker.offline');
  if (!off) return null;
  const w = off.fields.worker;
  const after = events.filter((e) => e.seq > off.seq);
  const copies = after.filter((e) => e.kind === 'rebalance.copy');
  const back = after.find((e) => e.kind === 'worker.online' && e.fields.worker === w);
  const exc = after.filter((e) => e.kind === 'gc.excess');
  const risk = chunks.degraded + chunks.unavailable;
  const step = (key, title, status, detail, time) => ({ key, title, status, detail, time });
  const steps = [
    step('stop', 'Heartbeats stop', 'done', `${w} stopped answering. The controller notices only when its 6 second lease runs out.`, null),
    step('offline', 'Declared OFFLINE', 'done', `${w} is marked offline and removed from the hash ring, so new chunks skip it.`, off.time),
    step('rebuild', 'Replicas rebuilt',
      copies.length ? 'done' : risk > 0 ? 'active' : 'done',
      copies.length ? `The rebalancer copied ${plural(copies.length, 'chunk')} from surviving workers to new ones.` : risk > 0 ? 'Waiting for the rebalancer to copy the chunks that lost a replica.' : 'No chunk had a copy on that worker, so nothing needed rebuilding.',
      copies.length ? copies[copies.length - 1].time : null),
    step('healthy', 'Every chunk has 2 copies again', risk === 0 ? 'done' : 'active', risk === 0 ? 'The replication factor is restored without the dead worker.' : `${plural(risk, 'chunk')} still short of 2 copies.`, null),
    step('return', 'Worker comes back', back ? 'done' : 'pending', back ? `${w} sends heartbeats again.` : `Waiting for ${w} to return (docker start ${w}).`, back ? back.time : null),
    step('cleanup', 'Stale copies removed', !back ? 'pending' : chunks.extra_copies === 0 ? 'done' : 'active',
      !back ? 'After it returns, the reconciler compares its disk with the records.' : chunks.extra_copies === 0 ? `The reconciler removed ${plural(exc.length, 'stale copy', 'stale copies')}, so each chunk is back to exactly 2 copies.` : `${plural(chunks.extra_copies, 'stale copy', 'stale copies')} still on disk, waiting for the reconciler.`,
      exc.length ? exc[exc.length - 1].time : null),
  ];
  return { worker: w, steps };
}

// analyzeCorruption rebuilds the story of the most recent damaged copy.
export function analyzeCorruption(events, chunks) {
  const anchor = lastOf(events, (e) => e.kind === 'scrub.quarantine' || e.kind === 'download.corrupt' || (e.kind === 'demo.action' && e.fields.action === 'corrupt'));
  if (!anchor) return null;
  const chunk = anchor.fields.chunk_id;
  const worker = anchor.fields.worker;
  const since = events.filter((e) => e.seq >= anchor.seq);
  const sameChunk = (e) => e.fields.chunk_id === chunk;
  const detected = since.find((e) => (e.kind === 'scrub.quarantine' || e.kind === 'download.corrupt') && sameChunk(e));
  const served = detected && since.find((e) => e.seq > detected.seq && e.kind === 'download.fetch' && e.fields.ok && e.fields.chunk_id === chunk && (e.fields.role === 'secondary' || e.fields.role === 'hedge'));
  const repaired = since.find((e) => (e.kind === 'download.repair' && e.fields.ok && sameChunk(e)) || (e.kind === 'gc.restore' && sameChunk(e)));
  const risk = chunks.degraded + chunks.unavailable;
  const isDamage = anchor.kind === 'demo.action';
  const step = (title, status, detail, time) => ({ title, status, detail, time });
  return {
    chunk,
    worker,
    steps: [
      step('A copy is damaged', 'done', isDamage ? `A demo control flipped bytes in the copy of ${shortId(chunk)} on ${worker}.` : `A damaged copy of ${shortId(chunk)} was found${worker ? ` on ${worker}` : ''}.`, isDamage ? anchor.time : null),
      step('Corruption detected', detected ? 'done' : 'active',
        detected ? (detected.kind === 'scrub.quarantine' ? `The scrubber's CRC32C check failed on ${detected.fields.worker}; the copy was moved to quarantine.` : `The controller hashed the bytes it received and the SHA-256 did not match the chunk id.`) : 'Waiting for the scrubber (every 30 s) or for a download to touch the chunk. Press "Scrub now" to hurry it.',
        detected ? detected.time : null),
      step('Readers get good data', served ? 'done' : detected ? 'active' : 'pending', served ? 'A download was served from the other copy; the client never saw the damage.' : 'Reads fall back to the other replica; try downloading the object.', served ? served.time : null),
      step('Copy restored', repaired ? 'done' : detected ? 'active' : 'pending', repaired ? 'The good copy was copied back (read-repair or the reconciler).' : 'Read-repair fixes it on the next download; the reconciler also restores it within 30 s.', repaired ? repaired.time : null),
      step('Everything healthy', repaired && risk === 0 ? 'done' : 'pending', repaired && risk === 0 ? 'Every chunk has 2 good copies again.' : 'Waiting for the restore to finish.', null),
    ],
  };
}

function stepCard(s, i) {
  const tone = s.status === 'done' ? 'good' : s.status === 'active' ? 'warning' : 'muted';
  return h('div', { class: `sstep ${s.status}` },
    h('div', { class: 'sn' }, h('span', { 'data-tone': tone }, icon(TONE_ICON[tone], 'glyph', 13)), `${i + 1}. ${s.title}`),
    h('div', { class: 't' }, s.detail),
    s.time ? h('div', { class: 't num', style: { marginTop: '3px' } }, fmtClock(s.time)) : null);
}

export function mount(root) {
  const incidentBox = h('div');
  const corruptBox = h('div');
  const gridBox = h('div');
  const demoBox = h('div');
  let demoSig = '';

  root.append(
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, 'Replication health'), h('p', null, 'one square per distinct chunk: how many copies can be reached right now')), gridBox),
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, 'When a worker dies'), h('p', null, 'built from the real events')), incidentBox),
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, 'When a copy is damaged'), h('p', null, 'built from the real events')), corruptBox),
    h('div', { class: 'card' }, h('div', { class: 'card-head' }, h('h2', null, 'Try it yourself'), h('p', null, 'break something and watch the other panels')), demoBox),
    h('div', { class: 'card' },
      h('div', { class: 'card-head' }, h('h2', null, 'Who repairs what')),
      h('ul', { class: 'steps-list' },
        h('li', null, h('b', null, 'Health monitor:'), ' declares a worker OFFLINE after 6 seconds without a heartbeat.'),
        h('li', null, h('b', null, 'Rebalancer:'), ' when a worker goes offline, copies its chunks from the surviving replica to another worker, so every chunk has 2 copies again within seconds.'),
        h('li', null, h('b', null, 'Scrubber (each worker):'), ' re-reads every chunk, checks its CRC32C and quarantines a damaged copy so it is never served.'),
        h('li', null, h('b', null, 'Read-repair:'), ' a download checks the SHA-256 of every chunk; on a mismatch it uses the other copy and overwrites the bad one.'),
        h('li', null, h('b', null, 'Reconciler:'), ' compares what each disk really holds with the records. It restores missing copies, deletes extra copies (for example on a returning worker), and removes chunks nothing uses.'))));

  function renderGrid(s) {
    clear(gridBox);
    const seen = new Map();
    for (const o of s.objects) {
      if (o.status !== 'COMMITTED') continue;
      for (const c of o.chunks) if (!seen.has(c.chunk_id)) seen.set(c.chunk_id, { c, name: `${o.name} #${c.index + 1}` });
    }
    if (!seen.size) {
      gridBox.append(h('div', { class: 'empty' }, 'No committed chunks yet.'));
      return;
    }
    const cells = h('div', { class: 'hgrid', role: 'list' });
    for (const { c, name } of seen.values()) {
      const n = c.reachable_copies;
      const tone = n >= 2 ? 'good' : n === 1 ? 'serious' : 'critical';
      const cell = h('div', { class: 'hcell', role: 'listitem', tabindex: '0', 'data-tone': tone, 'aria-label': `${name}: ${n} reachable ${n === 1 ? 'copy' : 'copies'}` }, `${n}${c.extra_copies.length ? '+' : ''}`);
      tip(cell, () => [name, [['reachable copies', String(n)], ['listed on', c.replicas.map((r) => r.worker).join(', ')], ['stale extras', c.extra_copies.length ? c.extra_copies.join(', ') : 'none'], ['chunk', shortId(c.chunk_id, 12)]]]);
      cells.append(cell);
    }
    gridBox.append(cells, h('div', { class: 'legend', style: { marginTop: '10px' } },
      h('span', { class: 'pill', 'data-tone': 'good' }, icon('check', 'glyph', 12), '2 copies: healthy'),
      h('span', { class: 'pill', 'data-tone': 'serious' }, icon('warn', 'glyph', 12), '1 copy: one failure from data loss'),
      h('span', { class: 'pill', 'data-tone': 'critical' }, icon('cross', 'glyph', 12), '0: cannot be read right now'),
      h('span', { class: 'hint' }, '"+" marks a stale extra copy waiting to be cleaned up.')));
  }

  function renderStory(box, story, idle) {
    clear(box);
    if (!story) {
      box.append(h('div', { class: 'empty' }, idle));
      return;
    }
    box.append(h('div', { class: 'stepper' }, story.steps.map(stepCard)));
  }

  async function act(btn, label, fn) {
    btn.disabled = true;
    try {
      const res = await fn();
      toast(`${label}: done`, 'ok');
      refreshNow();
      return res;
    } catch (e) {
      toast(`${label} failed: ${e.message}`, 'error', 7000);
      return null;
    } finally {
      btn.disabled = false;
    }
  }

  function renderDemo(s) {
    const st = s.state;
    const demo = st.demo;
    const workers = [...st.workers].sort((a, b) => naturalCompare(a.id, b.id));
    const sig = JSON.stringify([demo, workers.map((w) => [w.id, w.health, w.chunk_count])]);
    if (sig === demoSig) return;
    demoSig = sig;
    clear(demoBox);
    if (!demo.enabled) {
      demoBox.append(h('div', { class: 'callout' },
        h('p', null, h('b', null, 'Demo controls are off. '), 'They stop containers and damage chunks, so they only exist when you ask for them. Start the stack with:'),
        h('p', { style: { marginTop: '6px' } }, h('code', null, 'docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.demo.yml up -d --build')),
        h('p', { style: { marginTop: '6px' } }, 'Or break things by hand and watch this page react: ', h('code', null, 'docker stop worker2'), ', ', h('code', null, 'docker start worker2'), '.')));
      return;
    }
    if (!demo.available) {
      demoBox.append(h('div', { class: 'callout' }, 'Demo mode is on, but the controller cannot reach the Docker socket. Check that /var/run/docker.sock is mounted into the controller container.'));
    }
    const killRow = h('div', { class: 'ctl-row' }, workers.map((w) => {
      const up = w.health === 'ONLINE';
      const b = h('button', { class: `btn small${up ? ' danger' : ''}`, type: 'button', disabled: !demo.available },
        icon(up ? 'cross' : 'check', 'glyph', 13), up ? `Stop ${w.id}` : `Start ${w.id}`);
      b.addEventListener('click', () => act(b, up ? `Stop ${w.id}` : `Start ${w.id}`, () => api.demo(`worker/${encodeURIComponent(w.id)}/${up ? 'stop' : 'start'}`)));
      return b;
    }));
    const online = workers.filter((w) => w.health === 'ONLINE' && w.inventory_ok && w.chunk_count > 0);
    const wSel = h('select', { 'aria-label': 'Worker whose copy to damage' }, online.map((w) => h('option', { value: w.id }, `${w.id} (${w.chunk_count} chunks)`)));
    const cSel = h('select', { 'aria-label': 'Chunk copy to damage' });
    const fillChunks = () => {
      clear(cSel);
      const w = workers.find((x) => x.id === wSel.value);
      const names = new Map();
      for (const o of s.objects) for (const c of o.chunks) names.set(c.chunk_id, `${o.name} #${c.index + 1}`);
      for (const id of (w ? w.chunks : [])) cSel.append(h('option', { value: id }, `${names.get(id) || 'unreferenced'} (${shortId(id)})`));
    };
    wSel.addEventListener('change', fillChunks);
    fillChunks();
    const corruptBtn = h('button', { class: 'btn small danger', type: 'button', disabled: !demo.available || !online.length }, icon('bolt', 'glyph', 13), 'Damage this copy');
    corruptBtn.addEventListener('click', () => act(corruptBtn, 'Damage copy', () => api.demo('corrupt', { worker: wSel.value, chunk_id: cSel.value })));
    const scrubSel = h('select', { 'aria-label': 'Worker to scrub' }, workers.filter((w) => w.health === 'ONLINE').map((w) => h('option', { value: w.id }, w.id)));
    const scrubBtn = h('button', { class: 'btn small', type: 'button', disabled: !demo.available || !scrubSel.options.length }, icon('shield', 'glyph', 13), 'Scrub now');
    scrubBtn.addEventListener('click', () => act(scrubBtn, `Scrub ${scrubSel.value}`, () => api.demo(`scrub/${encodeURIComponent(scrubSel.value)}`)));
    const reconBtn = h('button', { class: 'btn small', type: 'button' }, icon('broom', 'glyph', 13), 'Run the reconciler now');
    reconBtn.addEventListener('click', () => act(reconBtn, 'Reconciler pass', () => api.demo('reconcile')));
    demoBox.append(h('div', { class: 'controls' },
      h('div', { class: 'ctl-box' }, h('h4', null, '1. Kill or revive a worker'), h('p', { class: 'hint' }, 'Stops or starts the real Docker container. Watch the Workers tab and the event log.'), killRow),
      h('div', { class: 'ctl-box' }, h('h4', null, '2. Damage one stored copy'), h('p', { class: 'hint' }, 'Flips bytes inside the chunk file on that worker\'s disk, exactly like bit rot.'), h('div', { class: 'ctl-row' }, wSel, cSel, corruptBtn)),
      h('div', { class: 'ctl-box' }, h('h4', null, '3. Make the repair machinery run now'), h('p', { class: 'hint' }, 'They normally run every 30 seconds.'), h('div', { class: 'ctl-row' }, scrubSel, scrubBtn, reconBtn))));
  }

  return {
    update(s) {
      const st = s.state;
      if (!st) return;
      renderGrid(s);
      renderStory(incidentBox, analyzeIncident(s.events, st.cluster.chunks), 'No worker has failed yet. Stop one below, or run docker stop worker2, and the six steps of the recovery fill in live.');
      renderStory(corruptBox, analyzeCorruption(s.events, st.cluster.chunks), 'No damaged copy seen yet. Use "Damage this copy" below, then download the object or press "Scrub now".');
      renderDemo(s);
    },
  };
}
