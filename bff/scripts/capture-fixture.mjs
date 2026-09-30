/**
 * Records a slice of the live stream into public/demo-stream.json.
 *
 * The Angular app replays that fixture when no BFF is reachable (GitHub Pages,
 * a reviewer's laptop) using the real inter-arrival times, so the static demo
 * behaves like the live one without shipping fake logic in the frontend.
 *
 *   cd bff && npm run capture
 */
import { writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { config, TOPICS, topicNames } from '../src/config.mjs';

const COUNT = Math.max(10, Number(process.env.CAPTURE_COUNT ?? 300));
const TIMEOUT_MS = Number(process.env.CAPTURE_TIMEOUT_MS ?? 30_000);
const OUT_FILE = path.resolve(
  fileURLToPath(new URL('../../public/demo-stream.json', import.meta.url)),
);

const kafka = new Kafka({
  clientId: 'streamlens-capture',
  brokers: config.brokers,
  logLevel: logLevel.ERROR,
});
const consumer = kafka.consumer({ groupId: `streamlens-capture-${randomUUID().slice(0, 8)}` });

const events = [];
let stopCollecting = () => {};

/** Resolved from inside `eachBatch`, consumed outside it — see the note below. */
const collected = new Promise((resolve) => {
  stopCollecting = resolve;
});

/**
 * Never call `consumer.disconnect()` from inside `eachBatch`: the consumer waits for
 * the handler to finish before it can leave the group, so disconnecting there
 * deadlocks. We only signal completion and do the teardown from the top level.
 */
const writeFixture = async () => {
  const ordered = [...events].sort((a, b) => a.at - b.at || a.partition - b.partition);
  const withDeltas = ordered.map((event, index) => ({
    ...event,
    // Clamped so a burst of silence does not turn into a 20s pause on replay.
    deltaMs: index === 0 ? 0 : Math.min(2_000, Math.max(0, event.at - ordered[index - 1].at)),
  }));

  await writeFile(
    OUT_FILE,
    `${JSON.stringify(
      {
        capturedAt: Date.now(),
        source: {
          brokers: config.brokers,
          topics: topicNames,
          // Topology travels with the recording so the UI can show real partition
          // counts offline instead of guessing from whatever arrived.
          partitions: Object.fromEntries(TOPICS.map((topic) => [topic.name, topic.partitions])),
          count: withDeltas.length,
        },
        events: withDeltas,
      },
      null,
      2,
    )}\n`,
  );

  const noiselessBytes = Buffer.byteLength(JSON.stringify(withDeltas));
  const span = withDeltas.length === 0 ? 0 : (withDeltas.at(-1)?.at ?? 0) - (withDeltas[0].at ?? 0);
  const observedRate = span > 0 ? (withDeltas.length / span) * 1_000 : 0;
  const replaySeconds = withDeltas.reduce((total, event) => total + event.deltaMs, 0) / 1_000;

  console.log(
    `captured ${withDeltas.length} events (~${Math.round(noiselessBytes / 1024)} kB) in ` +
      `${(span / 1_000).toFixed(1)}s → ${observedRate.toFixed(1)} records/s observed, ` +
      `${replaySeconds.toFixed(1)}s of replay`,
  );
  console.log(`written to ${OUT_FILE}`);
};

await consumer.connect();
await consumer.subscribe({ topics: topicNames, fromBeginning: false });
await consumer.run({
  autoCommit: false,
  eachBatch: async ({ batch }) => {
    for (const message of batch.messages) {
      let decoded;
      try {
        decoded = JSON.parse(message.value?.toString() ?? 'null');
      } catch {
        continue;
      }

      events.push({
        id: `${batch.topic}:${batch.partition}:${message.offset}`,
        topic: batch.topic,
        partition: batch.partition,
        offset: message.offset,
        key: message.key ? message.key.toString() : null,
        type: decoded?.type ?? 'unknown',
        at: Number(decoded?.at ?? message.timestamp),
        payload: decoded?.payload ?? {},
      });

      if (events.length >= COUNT) {
        stopCollecting();
        return;
      }
    }
  },
});

const timedOut = await Promise.race([
  collected.then(() => false),
  new Promise((resolve) => setTimeout(() => resolve(true), TIMEOUT_MS).unref()),
]);

if (timedOut) {
  console.warn(`only ${events.length} events captured before the timeout — writing what we have`);
}

await writeFixture();
await consumer.disconnect().catch(() => {});
process.exit(0);
