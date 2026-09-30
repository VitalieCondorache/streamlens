/**
 * Dependency-free logger: one line per event, machine readable enough to grep,
 * human readable enough for a demo. LOG_LEVEL=debug for the noisy stuff.
 */
const RANK = { debug: 10, info: 20, warn: 30, error: 40 };
const threshold = RANK[process.env.LOG_LEVEL ?? 'info'] ?? RANK.info;

const emit = (level, message, fields) => {
  if (RANK[level] < threshold) return;
  const stamp = new Date().toISOString().slice(11, 23);
  const suffix = fields && Object.keys(fields).length > 0 ? ` ${JSON.stringify(fields)}` : '';
  process.stdout.write(`${stamp} ${level.toUpperCase().padEnd(5)} ${message}${suffix}\n`);
};

export const log = {
  debug: (message, fields) => emit('debug', message, fields),
  info: (message, fields) => emit('info', message, fields),
  warn: (message, fields) => emit('warn', message, fields),
  error: (message, fields) => emit('error', message, fields),
};
