// Thin client for the controller: UI endpoints, the public REST API and the demo controls.
const NO_CACHE = { cache: 'no-store' };

async function failure(resp) {
  let msg = `${resp.status} ${resp.statusText}`.trim();
  try {
    const text = await resp.text();
    try {
      const j = JSON.parse(text);
      if (j && j.error) msg = j.error;
    } catch {
      if (text) msg = text.trim().slice(0, 200);
    }
  } catch {
    // the status line stays as the message
  }
  const err = new Error(msg);
  err.status = resp.status;
  return err;
}

async function getJSON(path) {
  const resp = await fetch(path, NO_CACHE);
  if (!resp.ok) throw await failure(resp);
  return resp.json();
}

export const api = {
  state: () => getJSON('/ui/api/state'),
  objects: () => getJSON('/ui/api/objects'),
  events: (since, limit) => getJSON(`/ui/api/events?since=${since}&limit=${limit}`),
  locate: (q) => getJSON(`/ui/api/ring/locate?${q}`),
  probe: (q) => getJSON(`/ui/api/bloom/probe?${q}`),

  async demo(path, body) {
    const resp = await fetch(`/ui/api/demo/${path}`, {
      method: 'POST',
      headers: { 'X-Requested-With': 'minidfs-ui', 'Content-Type': 'application/json' },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (!resp.ok) throw await failure(resp);
    return resp.json();
  },

  async deleteObject(id, trace) {
    const resp = await fetch(`/api/v1/objects/${encodeURIComponent(id)}`, { method: 'DELETE', headers: trace ? { 'X-Trace-Id': trace } : {} });
    if (!resp.ok) throw await failure(resp);
  },

  async download(id, trace) {
    const resp = await fetch(`/api/v1/objects/${encodeURIComponent(id)}`, { headers: trace ? { 'X-Trace-Id': trace } : {} });
    if (!resp.ok) throw await failure(resp);
    return resp.blob();
  },

  async metadata(id) {
    const resp = await fetch(`/api/v1/objects/${encodeURIComponent(id)}/metadata`, NO_CACHE);
    if (!resp.ok) throw await failure(resp);
    return { etag: resp.headers.get('ETag'), body: await resp.json() };
  },

  async patchMetadata(id, patch, etag) {
    const headers = { 'Content-Type': 'application/json' };
    if (etag) headers['If-Match'] = etag;
    const resp = await fetch(`/api/v1/objects/${encodeURIComponent(id)}/metadata`, { method: 'PATCH', headers, body: JSON.stringify(patch) });
    if (!resp.ok) throw await failure(resp);
    return resp.json();
  },
};

export function newTrace() {
  if (globalThis.crypto && crypto.randomUUID) return crypto.randomUUID();
  return `t-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

// uploadFile posts one file with progress; it resolves with {status, body} and never rejects on an HTTP error.
export function uploadFile(file, { trace, metadata, onProgress } = {}) {
  return new Promise((resolve, reject) => {
    const form = new FormData();
    form.append('file', file, file.name);
    if (metadata && Object.keys(metadata).length) form.append('metadata', JSON.stringify(metadata));
    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/api/v1/objects');
    if (trace) xhr.setRequestHeader('X-Trace-Id', trace);
    xhr.upload.onprogress = (e) => { if (e.lengthComputable && onProgress) onProgress(e.loaded / e.total); };
    xhr.onload = () => {
      let body = null;
      try { body = JSON.parse(xhr.responseText); } catch { body = { error: xhr.responseText.trim().slice(0, 200) }; }
      resolve({ status: xhr.status, body });
    };
    xhr.onerror = () => reject(new Error('network error while uploading'));
    xhr.onabort = () => reject(new Error('upload aborted'));
    xhr.send(form);
  });
}

// parseMetadata turns "owner=alice, tag=demo" into an object; it throws a readable error for malformed input.
export function parseMetadata(text) {
  const out = {};
  const src = (text || '').trim();
  if (!src) return out;
  for (const part of src.split(',')) {
    const p = part.trim();
    if (!p) continue;
    const i = p.indexOf('=');
    if (i < 1) throw new Error(`"${p}" is not key=value`);
    out[p.slice(0, i).trim()] = p.slice(i + 1).trim();
  }
  return out;
}
