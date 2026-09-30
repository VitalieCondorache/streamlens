/**
 * Presentation-level formatters.
 *
 * Kept in one place so a thousandths separator or a unit never differs between
 * two views — and so the templates stay free of formatting logic.
 */
const compactFormatter = new Intl.NumberFormat(undefined, {
  notation: 'compact',
  maximumFractionDigits: 1,
});

const integerFormatter = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });

export const compact = (value: number | string): string => {
  const numeric = typeof value === 'string' ? Number(value) : value;
  return Number.isFinite(numeric) ? compactFormatter.format(numeric) : String(value);
};

export const integer = (value: number | string): string => {
  const numeric = typeof value === 'string' ? Number(value) : value;
  return Number.isFinite(numeric) ? integerFormatter.format(numeric) : String(value);
};

export const percent = (share: number, digits = 1): string => `${(share * 100).toFixed(digits)}%`;

/** Milliseconds with a sensible unit: pipeline latency lives under 10ms here. */
export const duration = (milliseconds: number): string => {
  if (!Number.isFinite(milliseconds)) return '—';
  if (milliseconds < 1) return `${(milliseconds * 1000).toFixed(0)} µs`;
  if (milliseconds < 1_000) return `${milliseconds.toFixed(milliseconds < 10 ? 1 : 0)} ms`;
  return `${(milliseconds / 1_000).toFixed(1)} s`;
};

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

export const timeOfDay = (timestamp: number): string => timeFormatter.format(new Date(timestamp));

/** `order.created` -> `Order created`: useful as a chip label. */
export const humanize = (token: string): string => {
  const [namespace, ...rest] = token.split('.');
  const words = (rest.length > 0 ? rest : [namespace]).join(' ').replaceAll('_', ' ');
  return words.charAt(0).toUpperCase() + words.slice(1);
};
