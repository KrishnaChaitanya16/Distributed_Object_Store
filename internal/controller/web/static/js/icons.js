// Inline icons. Status is never colour alone: every state has its own shape.
import { svg } from './util.js';

const PATHS = {
  check: 'M3 8.5l3.2 3.2L13 4.8',
  cross: 'M4 4l8 8M12 4l-8 8',
  warn: 'M8 2.5l6 10.5H2z M8 7v3 M8 11.6v.1',
  dot: 'M8 5a3 3 0 100 6 3 3 0 000-6z',
  clock: 'M8 3v5l3 2 M8 1.5a6.5 6.5 0 100 13 6.5 6.5 0 000-13z',
  question: 'M6 6a2 2 0 114 0c0 1.4-2 1.5-2 3 M8 11.8v.1',
  upload: 'M8 11V3 M4.5 6.5L8 3l3.5 3.5 M3 13h10',
  download: 'M8 3v8 M4.5 7.5L8 11l3.5-3.5 M3 13h10',
  hash: 'M6 2.5L5 13.5 M11 2.5L10 13.5 M3 6h10 M2.5 10h10',
  ring: 'M8 2a6 6 0 100 12A6 6 0 008 2z M8 5a3 3 0 100 6 3 3 0 000-6z',
  server: 'M3 3.5h10v3.5H3z M3 9h10v3.5H3z M5 5.2v.1 M5 10.7v.1',
  shield: 'M8 2l5 2v4c0 3-2.2 5-5 6-2.8-1-5-3-5-6V4z',
  broom: 'M10 2l4 4 M9 7L3 13 M7 5l4 4',
  bolt: 'M9 1.5L3.5 9H8l-1 5.5L12.5 7H8z',
  filter: 'M2.5 3h11L9 8.5V13l-2-1V8.5z',
  trash: 'M3.5 4.5h9 M6 4.5V3h4v1.5 M5 4.5l.6 8h4.8l.6-8',
  wrench: 'M10.5 2.5a3.2 3.2 0 00-3 4.4L2.5 12l1.5 1.5 5.1-5a3.2 3.2 0 004.4-3l-2 2-1.6-.4-.4-1.6z',
  flask: 'M6 2.5h4 M6.5 2.5v4L3 13h10L9.5 6.5v-4',
  copy: 'M5.5 5.5h7v8h-7z M3.5 10.5v-8h7',
  plus: 'M8 3v10 M3 8h10',
  layers: 'M8 2l6 3-6 3-6-3z M2 8l6 3 6-3 M2 11l6 3 6-3',
};

export function icon(name, cls = 'glyph', size = 16) {
  const el = svg('svg', { class: cls, viewBox: '0 0 16 16', width: size, height: size, fill: 'none', stroke: 'currentColor', 'stroke-width': 1.6, 'stroke-linecap': 'round', 'stroke-linejoin': 'round', 'aria-hidden': 'true' });
  el.append(svg('path', { d: PATHS[name] || PATHS.dot }));
  return el;
}

export const TONE_ICON = { good: 'check', warning: 'clock', serious: 'warn', critical: 'cross', muted: 'dot' };
