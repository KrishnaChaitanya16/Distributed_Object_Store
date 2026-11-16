// Worker identity colours. The slot follows the worker's name, never its rank among the online ones, so a worker keeps its colour when others come and go.
const SLOTS = ['var(--s1)', 'var(--s2)', 'var(--s3)', 'var(--s4)'];
const ON = ['var(--on-s1)', 'var(--on-s2)', 'var(--on-s3)', 'var(--on-s4)'];
const order = [];

// setWorkers fixes the colour order from the sorted worker ids; ids seen earlier keep their slot.
export function setWorkers(ids) {
  for (const id of [...ids].sort(naturalCompare)) if (!order.includes(id)) order.push(id);
  order.sort(naturalCompare);
}

export function slotOf(id) {
  return order.indexOf(id);
}

export function workerColor(id) {
  const i = slotOf(id);
  return i >= 0 && i < SLOTS.length ? SLOTS[i] : 'var(--s-other)';
}

export function workerOnColor(id) {
  const i = slotOf(id);
  return i >= 0 && i < ON.length ? ON[i] : '#ffffff';
}

export function naturalCompare(a, b) {
  return String(a).localeCompare(String(b), undefined, { numeric: true });
}
