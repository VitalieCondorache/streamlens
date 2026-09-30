/**
 * HTTP surface smoke test.
 *
 * Boots the bridge on a spare port, walks every read endpoint and asserts the shape of
 * the answer, then shuts it down. The unit tests cannot see this layer at all: a typo in
 * a route handler is invisible to `ng build`, and only shows up as a 500 in the browser.
 * (That is exactly how `TOPICS is not defined` reached the UI once.)
 *
 *   cd bff && npm run routes:smoke        # needs a reachable broker
 */
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

const port = Number(process.env.ROUTES_SMOKE_PORT ?? 4123);
const base = `http://127.0.0.1:${port}`;

const server = spawn('node', ['src/server.mjs'], {
  env: { ...process.env, PORT: String(port), SIMULATOR_ENABLED: 'false' },
  stdio: ['ignore', 'pipe', 'pipe'],
});

const failures = [];
const say = (step, detail = '') => console.log(`${step.padEnd(34, '.')} ${detail}`);

const check = async (step, path, assert) => {
  try {
    const response = await fetch(`${base}${path}`);
    const body = await response.json();

    if (response.status !== 200) {
      failures.push(`${step}: HTTP ${response.status} ${JSON.stringify(body).slice(0, 200)}`);
      say(step, `HTTP ${response.status}`);
      return;
    }

    const problem = assert(body);
    if (problem !== null) failures.push(`${step}: ${problem}`);
    say(step, problem ?? 'ok');
  } catch (error) {
    failures.push(`${step}: ${error.message}`);
    say(step, error.message);
  }
};

const ready = async (attempts = 40) => {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetch(`${base}/api/health`);
      const body = await response.json();
      if (body?.status === 'ok') return true;
    } catch {
      /* not listening yet */
    }
    await delay(500);
  }
  return false;
};

try {
  if (!(await ready())) {
    throw new Error('the bridge never reported a healthy status');
  }

  await check('GET /api/health', '/api/health', (body) =>
    body.status === 'ok' && body.kafka?.connected === true ? null : 'broker not connected',
  );

  await check('GET /api/topics', '/api/topics', (body) => {
    if (!Array.isArray(body.topics) || body.topics.length === 0) return 'no topics returned';
    const [topic] = body.topics;
    if (!Array.isArray(topic.partitions) || topic.partitions.length === 0) {
      return `topic ${topic.name} has no partitions`;
    }
    const missing = topic.partitions.find((entry) => entry.nextOffset === undefined);
    return missing ? `partition ${missing.partition} is missing nextOffset` : null;
  });

  await check('GET /api/lag', '/api/lag', (body) => {
    if (!Array.isArray(body.partitions) || body.partitions.length === 0) return 'no lag rows';
    return body.partitions.some((row) => row.nextOffset === undefined)
      ? 'a lag row is missing nextOffset'
      : null;
  });

  await check('GET / (self description)', '/', (body) =>
    Array.isArray(body.endpoints) && body.endpoints.length > 0 ? null : 'no endpoint list',
  );

  // The unknown-route fallback must answer 404 instead of hanging or crashing.
  const notFound = await fetch(`${base}/api/unknown-route`);
  if (notFound.status === 404) {
    say('GET /api/unknown-route', '404 as expected');
  } else {
    failures.push(`GET /api/unknown-route returned ${notFound.status}, expected 404`);
    say('GET /api/unknown-route', `HTTP ${notFound.status}`);
  }
} catch (error) {
  failures.push(error.message);
} finally {
  server.kill('SIGTERM');
}

if (failures.length > 0) {
  console.error('\nROUTE SMOKE FAILED:');
  for (const failure of failures) console.error(` - ${failure}`);
  process.exitCode = 1;
}

process.exit(process.exitCode ?? 0);
