/**
 * Every knob the bridge exposes lives here, behind env vars, so the very same
 * image runs on a laptop, inside Docker or in CI without code changes.
 */
const int = (raw, fallback) => {
  const parsed = Number.parseInt(raw ?? '', 10);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const bool = (raw, fallback) => (raw === undefined ? fallback : raw === 'true');

export const config = {
  version: '1.0.0',
  port: int(process.env.PORT, 4000),
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'streamlens-bff',
  corsOrigin: process.env.CORS_ORIGIN ?? '*',

  /** Live-tail tuning. See stream.mjs — these two values drive the batching. */
  stream: {
    flushMs: int(process.env.STREAM_FLUSH_MS, 120),
    maxBuffer: int(process.env.STREAM_MAX_BUFFER, 2_000),
    /** SSE comment frames keep proxies from closing an idle connection. */
    heartbeatMs: int(process.env.STREAM_HEARTBEAT_MS, 15_000),
  },

  simulator: {
    running: bool(process.env.SIMULATOR_ENABLED, true),
    defaultRate: int(process.env.SIMULATOR_RATE, 14),
    maxRate: int(process.env.SIMULATOR_MAX_RATE, 500),
    /** Probability of entering a "failure burst" window (drives the alert panel). */
    burstChance: 0.004,
  },

  /** Consumer group that only exists so the UI can show real lag numbers. */
  auditGroup: process.env.AUDIT_GROUP ?? 'streamlens-audit',

  /**
   * Throwaway topic used to answer "which partition does this key land on?".
   * It is never deleted on purpose: deleting a topic that the shared producer has
   * metadata for poisons its cache and every later send fails with
   * "This server does not host this topic-partition".
   */
  probeTopic: process.env.PROBE_TOPIC ?? 'streamlens.partition-probe',
  probePartitions: int(process.env.PROBE_PARTITIONS, 6),
};

/**
 * Topology is declared in code on purpose: the broker runs with
 * `auto.create.topics.enable=false`, so partition counts are owned by the
 * application. A partition view is only meaningful if the topology is explicit.
 */
export const TOPICS = [
  {
    name: 'streamlens.orders',
    partitions: 6,
    description: 'Order lifecycle of a fictional storefront',
    weights: {
      'order.created': 3,
      'payment.authorized': 3,
      'payment.failed': 1,
      'order.shipped': 2,
    },
    /** Weights used while a "failure burst" window is active (see events.mjs). */
    burstWeights: {
      'order.created': 2,
      'payment.authorized': 2,
      'payment.failed': 9,
      'order.shipped': 1,
    },
  },
  {
    name: 'streamlens.telemetry',
    partitions: 3,
    description: 'Warehouse scanner heartbeats',
    weights: {
      'scanner.heartbeat': 9,
      'scanner.degraded': 1,
    },
    burstWeights: {
      'scanner.heartbeat': 4,
      'scanner.degraded': 8,
    },
  },
];

export const topicNames = TOPICS.map((topic) => topic.name);

/** Topics every client must know about, including the non-streamed probe topic. */
export const allTopics = [
  ...TOPICS,
  {
    name: config.probeTopic,
    partitions: config.probePartitions,
    description: 'Throwaway topic for partition probes (never streamed to the UI)',
  },
];

export const findTopic = (name) => TOPICS.find((topic) => topic.name === name);

export const partitionCount = (name) => findTopic(name)?.partitions ?? 1;
