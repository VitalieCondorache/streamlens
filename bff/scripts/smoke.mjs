/**
 * Broker compatibility smoke test.
 *
 * Verifies that the KafkaJS client can talk to the pinned broker version for every
 * operation the BFF relies on: admin (create/describe topics, offsets) and the
 * produce/consume round trip. Run it after `docker compose up -d kafka`:
 *
 *   cd bff && npm run smoke
 */
import { Kafka, logLevel } from 'kafkajs';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const topic = `streamlens.smoke.${Date.now()}`;

const kafka = new Kafka({ clientId: 'streamlens-smoke', brokers, logLevel: logLevel.NOTHING });

const say = (step, detail = '') => console.log(`${step.padEnd(34, '.')} ${detail}`);

const groupId = `streamlens-smoke-${Date.now()}`;
const admin = kafka.admin();
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId });

const received = [];

try {
  await admin.connect();
  say('admin.connect', 'ok');

  const cluster = await admin.describeCluster();
  say('admin.describeCluster', `clusterId=${cluster.clusterId} brokers=${cluster.brokers.length}`);

  await admin.createTopics({
    waitForLeaders: true,
    topics: [{ topic, numPartitions: 3, replicationFactor: 1 }],
  });
  say('admin.createTopics', `${topic} (3 partitions)`);

  await producer.connect();
  const produced = await producer.send({
    topic,
    acks: -1,
    messages: Array.from({ length: 9 }, (_, i) => ({
      key: `order-${i % 3}`,
      value: JSON.stringify({ seq: i }),
    })),
  });
  say('producer.send', `${produced.length} record-metadata entries`);

  const offsets = await admin.fetchTopicOffsets(topic);
  say('admin.fetchTopicOffsets', offsets.map((o) => `p${o.partition}=${o.high}`).join(' '));

  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      received.push({ partition, key: message.key?.toString(), offset: message.offset });
    },
  });

  const deadline = Date.now() + 5000;
  while (received.length < 9 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  say('consume round trip', `${received.length}/9 messages`);
  say('key -> partition', received.map((r) => `${r.key}=p${r.partition}`).join(' '));

  // Commit offsets so that group lag can be read back through the admin API.
  await consumer.commitOffsets([
    ...received.map(({ partition, offset }) => ({
      topic,
      partition,
      offset: String(BigInt(offset) + 1n),
    })),
  ]);
  const [first] = await admin.fetchOffsets({ groupId, topics: [topic] });
  say(
    'admin.fetchOffsets',
    first.partitions.map((entry) => `p${entry.partition}=${entry.offset}`).join(' '),
  );
} catch (error) {
  console.error('\nSMOKE FAILED:', error.message);
  if (error.cause) console.error('cause:', error.cause?.message ?? error.cause);
  process.exitCode = 1;
} finally {
  await consumer.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  await admin.disconnect().catch(() => {});
}
