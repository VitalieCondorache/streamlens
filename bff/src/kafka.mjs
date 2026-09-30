/**
 * Everything that talks to the broker lives here — the HTTP layer never imports
 * KafkaJS directly, so the transport can be swapped (or faked in a test) without
 * touching the routes.
 */
import { Kafka, logLevel, Partitioners } from 'kafkajs';
import { config, TOPICS, allTopics, topicNames } from './config.mjs';
import { log } from './log.mjs';

/** Offsets are int64 on the wire: never let them near a JS number. */
const toBig = (value, fallback = 0n) => {
  try {
    return BigInt(value ?? fallback);
  } catch {
    return fallback;
  }
};

const asString = (value) => value?.toString() ?? null;

export const createKafkaBridge = ({
  clientId = config.clientId,
  brokers = config.brokers,
} = {}) => {
  const kafka = new Kafka({
    clientId,
    brokers,
    logLevel: process.env.KAFKA_LOG_LEVEL === 'debug' ? logLevel.DEBUG : logLevel.ERROR,
    // Bounded on purpose: the bridge does its own retrying where it has context,
    // and stacked retry policies are how "rewind" turns into a two-minute request.
    retry: { retries: 3, initialRetryTime: 150, multiplier: 1.5, maxRetryTime: 1_000 },
  });

  const admin = kafka.admin();
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  /**
   * The probe producer is separate from the traffic producer: it sends one record
   * per request to a topic that is never consumed by the UI, and keeping the two
   * apart means a probe can never disturb the metadata of the live producer.
   */
  const probeProducer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    idempotent: false,
  });

  let connected = false;
  let cluster = null;
  let lastError = null;

  const ensureTopics = async () => {
    const existing = new Set(await admin.listTopics());
    const missing = allTopics
      .filter((topic) => !existing.has(topic.name))
      .map((topic) => ({
        topic: topic.name,
        numPartitions: topic.partitions,
        replicationFactor: 1,
        configEntries: [{ name: 'retention.ms', value: String(6 * 60 * 60 * 1000) }],
      }));

    if (missing.length > 0) {
      await admin.createTopics({ waitForLeaders: true, timeout: 10_000, topics: missing });
      log.info('created topics', { topics: missing.map((topic) => topic.topic) });
    }

    // Cleanup for topics left behind by an earlier version of the probe, which
    // created and deleted one topic per call. Safe here: no producer is connected yet.
    const stale = [...existing].filter((name) => name.startsWith(`${config.probeTopic}.`));
    if (stale.length > 0) {
      await admin.deleteTopics({ topics: stale }).catch(() => {});
      log.info('removed legacy probe topics', { count: stale.length });
    }
  };

  const connect = async () => {
    try {
      await admin.connect();
      await ensureTopics();
      await Promise.all([producer.connect(), probeProducer.connect()]);
      cluster = await admin.describeCluster();
      connected = true;
      lastError = null;
      log.info('connected to kafka', {
        brokers: config.brokers.join(','),
        clusterId: cluster.clusterId,
        topics: topicNames.join(' '),
      });
    } catch (error) {
      connected = false;
      lastError = error.message;
      log.error('kafka connection failed', {
        error: error.message,
        brokers: config.brokers.join(','),
      });
      throw error;
    }
  };

  const health = async () => {
    if (!connected) {
      return {
        connected: false,
        error: lastError,
        brokers: config.brokers,
        clusterId: null,
        brokerCount: 0,
      };
    }
    try {
      cluster = await admin.describeCluster();
      lastError = null;
      return {
        connected: true,
        error: null,
        brokers: config.brokers,
        clusterId: cluster.clusterId,
        brokerCount: cluster.brokers.length,
        controllerId: asString(cluster.controller),
      };
    } catch (error) {
      connected = false;
      lastError = error.message;
      return {
        connected: false,
        error: lastError,
        brokers: config.brokers,
        clusterId: null,
        brokerCount: 0,
      };
    }
  };

  /** Per-partition watermarks: the data behind the "Partitions" view. */
  const describeTopics = async () => {
    const summary = await Promise.all(
      TOPICS.map(async (topic) => {
        const offsets = await admin.fetchTopicOffsets(topic.name);
        const partitions = offsets
          .map((entry) => {
            const low = toBig(entry.low);
            const high = toBig(entry.high);
            return {
              partition: entry.partition,
              oldestOffset: asString(low),
              nextOffset: asString(high),
              messageCount: asString(high > low ? high - low : 0n),
            };
          })
          .sort((a, b) => a.partition - b.partition);

        return { name: topic.name, description: topic.description, partitions };
      }),
    );

    return { generatedAt: Date.now(), topics: summary };
  };

  /** Real consumer-group lag, straight from __consumer_offsets. */
  const groupLag = async (groupId = config.auditGroup) => {
    const [topicOffsets, groupOffsets] = await Promise.all([
      Promise.all(
        topicNames.map(async (topic) => ({ topic, offsets: await admin.fetchTopicOffsets(topic) })),
      ),
      admin.fetchOffsets({ groupId, topics: topicNames }),
    ]);

    const committed = new Map();
    for (const entry of groupOffsets) {
      for (const partition of entry.partitions) {
        committed.set(`${entry.topic}:${partition.partition}`, partition.offset);
      }
    }

    const partitions = [];
    let totalLag = 0n;

    for (const { topic, offsets } of topicOffsets) {
      for (const entry of offsets) {
        const raw = committed.get(`${topic}:${entry.partition}`);
        const committedOffset = raw === undefined || raw === '-1' ? null : toBig(raw);
        const nextOffset = toBig(entry.high);
        const lag = committedOffset === null ? null : nextOffset - committedOffset;
        if (lag !== null && lag > 0n) totalLag += lag;
        partitions.push({
          topic,
          partition: entry.partition,
          committedOffset: asString(committedOffset),
          nextOffset: asString(nextOffset),
          lag: asString(lag),
        });
      }
    }

    return { groupId, generatedAt: Date.now(), totalLag: asString(totalLag), partitions };
  };
  /**
   * Answers the only question the Key Router view really asks: "where did the
   * broker actually put this key?"
   *
   * One record per request is the whole trick: KafkaJS returns produce metadata
   * per *batch*, so a request carrying several keys can only tell you the
   * partitions that were used, not which key went where. Sending them one by one
   * makes every response describe exactly one key.
   */
  const probePartitioning = async (keys, partitions = config.probePartitions) => {
    const topic = config.probeTopic;
    const marker = Date.now();

    const results = await Promise.all(
      keys.map(async (key) => {
        try {
          const [metadata] = await probeProducer.send({
            topic,
            acks: -1,
            timeout: 5_000,
            messages: [{ key, value: JSON.stringify({ key, marker }) }],
          });
          return {
            key,
            partition: metadata?.partition ?? null,
            offset: metadata?.baseOffset ?? null,
          };
        } catch (error) {
          log.warn('partition probe failed', { key, error: error.message });
          return { key, partition: null, offset: null };
        }
      }),
    );

    return { topic, partitions, probes: results };
  };

  const newConsumer = ({ groupId, sessionTimeout = 10_000, heartbeatInterval = 3_000 }) =>
    kafka.consumer({ groupId, sessionTimeout, heartbeatInterval, retry: { retries: 5 } });

  /**
   * Moves a session's consumer group back in history — the replay control.
   * Rewriting committed offsets is how "show me the last N events" is expressed
   * natively in Kafka, instead of keeping a history buffer in the frontend.
   */
  const rewindGroup = async ({ groupId, topic, count }) => {
    const offsets = await admin.fetchTopicOffsets(topic);
    const perPartition = Math.max(1, Math.ceil(count / offsets.length));

    const partitions = offsets.map((entry) => {
      const high = toBig(entry.high);
      const low = toBig(entry.low);
      const span = high - low;
      const target = span > BigInt(perPartition) ? high - BigInt(perPartition) : low;
      return {
        partition: entry.partition,
        offset: target.toString(),
        from: target.toString(),
        to: entry.high,
      };
    });

    await admin.setOffsets({
      groupId,
      topic,
      partitions: partitions.map(({ partition, offset }) => ({ partition, offset })),
    });
    return { groupId, topic, requested: count, perPartition, partitions };
  };

  const produce = (records) => producer.send(records);

  const disconnect = async () => {
    connected = false;
    await Promise.allSettled([
      producer.disconnect(),
      probeProducer.disconnect(),
      admin.disconnect(),
    ]);
    log.info('kafka clients disconnected');
  };

  return {
    connect,
    disconnect,
    health,
    describeTopics,
    groupLag,
    probePartitioning,
    rewindGroup,
    newConsumer,
    produce,
    isConnected: () => connected,
  };
};

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
