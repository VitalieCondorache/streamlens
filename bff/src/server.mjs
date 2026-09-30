/**
 * StreamLens BFF — composition root.
 *
 *   browser (Angular)  --SSE-->  this process  --kafkajs-->  Kafka broker
 *
 * Local run: docker compose up -d kafka  &&  (cd bff && npm start)
 */
import http from 'node:http';
import { config, TOPICS, topicNames, partitionCount } from './config.mjs';
import { log } from './log.mjs';
import { createKafkaBridge, sleep } from './kafka.mjs';
import { createStreamHub } from './stream.mjs';
import { createSimulator } from './simulator.mjs';
import { createRouter, json, readJsonBody } from './http.mjs';

const startedAt = Date.now();
const bridge = createKafkaBridge();
const hub = createStreamHub(bridge);
const simulator = createSimulator(bridge);
const router = createRouter();

/**
 * KafkaJS 2.2.x schedules a timer with a negative delay while its sockets warm up
 * (`TimeoutNegativeWarning`), which Node prints on stderr. It is cosmetic, it comes
 * from the library and not from this process, and it would be noise for whoever
 * runs `docker compose up`. Filter exactly that warning, keep everything else visible.
 */
const nodeWarnings = process.listeners('warning');
process.removeAllListeners('warning');
process.on('warning', (warning) => {
  if (warning.name === 'TimeoutNegativeWarning') return;
  for (const listener of nodeWarnings) listener(warning);
});

let kafkaReady = false;
let auditConsumer = null;

/** Retries an async operation — a group rewind fails while a member is still leaving. */
const retry = async (
  operation,
  { attempts = 5, delayMs = 200, deadlineMs = 8_000, label = 'operation' } = {},
) => {
  const deadline = Date.now() + deadlineMs;
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      lastError = error;
      log.debug(`${label} failed, retrying`, { attempt, error: error.message });
      if (Date.now() + delayMs > deadline) break;
      await sleep(delayMs);
    }
  }

  throw lastError;
};

/* ------------------------------------------------------------------ routes */

router.get('/', (req, res) => {
  json(res, 200, {
    service: 'streamlens-bff',
    version: config.version,
    description: 'Bridges Kafka topics to the browser over Server-Sent Events.',
    kafka: { connected: kafkaReady, brokers: config.brokers, topics: topicNames },
    endpoints: router.list(),
  });
});

router.get('/api/health', async (req, res) => {
  const kafka = await bridge.health();
  kafkaReady = kafka.connected;

  json(res, 200, {
    status: kafka.connected ? 'ok' : 'degraded',
    kafka,
    bff: {
      version: config.version,
      uptimeMs: Date.now() - startedAt,
      stream: { flushMs: config.stream.flushMs, maxBuffer: config.stream.maxBuffer },
    },
    stream: hub.stats(),
    simulator: simulator.state(),
  });
});

router.get('/api/topics', async (req, res) => {
  const metadata = await bridge.describeTopics();
  json(res, 200, { ...metadata, subscribers: hub.stats().activeSessions });
});

router.get('/api/lag', async (req, res, url) => {
  const groupId = url.searchParams.get('group');
  json(res, 200, await bridge.groupLag(groupId || config.auditGroup));
});

/**
 * "Where would this key land?" — answered by the broker, not by us. The Angular
 * app predicts the partition with its own murmur2 implementation and compares the
 * two; see src/app/core/kafka/partitioning.ts.
 */
router.post('/api/partition-probe', async (req, res) => {
  const body = await readJsonBody(req);
  const keys = (Array.isArray(body.keys) ? body.keys : [])
    .map((key) => String(key).slice(0, 256))
    .filter((key) => key.length > 0)
    .slice(0, 24);

  if (keys.length === 0) {
    json(res, 400, { error: 'missing_keys', message: 'Provide { keys: string[] } (max 24).' });
    return;
  }

  const topic = topicNames.includes(body.topic) ? body.topic : TOPICS[0].name;
  const result = await bridge.probePartitioning(keys, partitionCount(topic));
  json(res, 200, { ...result, keyCount: keys.length, checkedAt: Date.now() });
});

router.get('/api/simulator', (req, res) => json(res, 200, simulator.state()));

router.post('/api/simulator', async (req, res) => {
  const body = await readJsonBody(req);
  const action = body.action ?? 'start';

  if (action === 'stop') {
    json(res, 200, { ...simulator.stop(), requested: action });
    return;
  }

  if (!kafkaReady) {
    json(res, 503, { error: 'kafka_unavailable', message: 'Broker is not connected.' });
    return;
  }

  json(res, 200, { ...simulator.start(body.ratePerSecond), requested: action });
});

router.get('/api/stream', (req, res, url) => hub.handle(req, res, url));

/**
 * The replay control. Rewinding a consumer group is how "show me the last N
 * events" is expressed natively in Kafka: the client reconnects afterwards and
 * resumes from the offsets we just rewrote.
 */
router.post('/api/replay', async (req, res) => {
  const body = await readJsonBody(req);
  const sessionId = String(body.sessionId ?? '').replace(/[^a-zA-Z0-9_-]/g, '');
  const count = Math.max(1, Math.min(Number(body.count ?? 200) || 200, 5_000));
  const topic = topicNames.includes(body.topic) ? body.topic : TOPICS[0].name;

  if (sessionId.length === 0) {
    json(res, 400, { error: 'missing_session', message: 'sessionId is required.' });
    return;
  }
  if (hub.isActive(sessionId)) {
    json(res, 409, {
      error: 'stream_active',
      message: 'Close the stream before rewinding its consumer group.',
    });
    return;
  }

  const groupId = `streamlens-ui-${sessionId}`;
  const result = await retry(() => bridge.rewindGroup({ groupId, topic, count }), {
    attempts: 12,
    delayMs: 250,
    label: 'rewind consumer group',
  });

  json(res, 200, { ...result, reconnect: true });
});

/* ------------------------------------------------------------------ wiring */

/**
 * The audit consumer exists purely so the lag view shows numbers the broker
 * really tracks: without a live group member there is no lag to display.
 */
const startAuditConsumer = async () => {
  auditConsumer = bridge.newConsumer({ groupId: config.auditGroup });
  await auditConsumer.connect();
  await auditConsumer.subscribe({ topics: topicNames, fromBeginning: false });
  await auditConsumer.run({
    autoCommit: true,
    autoCommitInterval: 1_000,
    eachBatch: async ({ batch }) => {
      log.debug('audit batch', {
        topic: batch.topic,
        partition: batch.partition,
        size: batch.messages.length,
      });
    },
  });
  log.info('audit consumer running', { groupId: config.auditGroup });
};

/**
 * Kafka may not be up yet (compose start order, broker restart...). Serving HTTP
 * anyway lets the UI render a meaningful "degraded" state instead of a hard
 * failure, and the bridge recovers on its own once the broker answers.
 */
const connectWithRetry = async () => {
  if (kafkaReady) return;
  try {
    await bridge.connect();
    kafkaReady = true;
    await startAuditConsumer();
    if (config.simulator.running) simulator.start();
  } catch {
    kafkaReady = false;
    setTimeout(() => void connectWithRetry(), 5_000).unref();
  }
};

const server = http.createServer((req, res) => void router.route(req, res));

const shutdown = async (signal) => {
  log.info('shutting down', { signal });
  simulator.stop();
  await hub.closeAll();
  await auditConsumer?.disconnect().catch(() => {});
  await bridge.disconnect().catch(() => {});
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2_000).unref();
};

process.on('SIGINT', () => void shutdown('SIGINT'));
process.on('SIGTERM', () => void shutdown('SIGTERM'));

server.listen(config.port, () => {
  log.info('streamlens bff listening', {
    port: config.port,
    brokers: config.brokers.join(','),
    endpoints: router.list().length,
  });
});

void connectWithRetry();
