/**
 * Wire contract with the BFF (see bff/src/stream.mjs and bff/src/server.mjs).
 * Kept in one file so the shape of an API change is obvious in a diff.
 */

/** A record as it was published to Kafka, enriched with its Kafka coordinates. */
export interface StreamEvent {
  /** `topic:partition:offset` — unique across the cluster, used for deduplication. */
  readonly id: string;
  readonly topic: string;
  readonly partition: number;
  /** Kafka offsets are int64: a JS number would silently lose precision. */
  readonly offset: string;
  readonly key: string | null;
  /** Domain type, e.g. `payment.failed`. */
  readonly type: string;
  /** Event time, milliseconds, from the producer. */
  readonly at: number;
  readonly payload: Readonly<Record<string, unknown>>;
  /** When the BFF observed the record; `ingestedAt - at` is pipeline latency. */
  readonly ingestedAt: number;
}

export interface PartitionMeta {
  readonly partition: number;
  readonly oldestOffset: string;
  readonly nextOffset: string;
  readonly messageCount: string;
}

export interface TopicMeta {
  readonly name: string;
  readonly description: string;
  readonly partitions: PartitionMeta[];
}

export interface TopicsResponse {
  readonly generatedAt: number;
  readonly topics: TopicMeta[];
  readonly subscribers: number;
}

export interface LagRow {
  readonly topic: string;
  readonly partition: number;
  readonly committedOffset: string | null;
  readonly nextOffset: string;
  readonly lag: string | null;
}

export interface LagResponse {
  readonly groupId: string;
  readonly generatedAt: number;
  readonly totalLag: string;
  readonly partitions: LagRow[];
}

export interface BrokerHealth {
  readonly connected: boolean;
  readonly error: string | null;
  readonly brokers: readonly string[];
  readonly clusterId: string | null;
  readonly brokerCount: number;
  readonly controllerId?: string;
}

export interface SessionInfo {
  readonly sessionId: string;
  readonly groupId: string;
  readonly topics: readonly string[];
  readonly delivered: number;
  readonly buffered: number;
  readonly paused: boolean;
  readonly openedAt: number;
}

export interface StreamStats {
  readonly opened: number;
  readonly closed: number;
  readonly frames: number;
  readonly events: number;
  readonly backpressurePauses: number;
  readonly supersededConnections: number;
  readonly crashedConsumers: number;
  readonly activeSessions: number;
  readonly sessions: readonly SessionInfo[];
}

export interface SimulatorState {
  readonly running: boolean;
  readonly ratePerSecond: number;
  readonly produced: number;
  readonly failed: number;
  readonly burst: boolean;
  readonly burstUntil: number | null;
}

export interface HealthResponse {
  readonly status: 'ok' | 'degraded';
  readonly kafka: BrokerHealth;
  readonly bff: {
    readonly version: string;
    readonly uptimeMs: number;
    readonly stream: { readonly flushMs: number; readonly maxBuffer: number };
  };
  readonly stream: StreamStats;
  readonly simulator: SimulatorState;
}

export interface PartitionProbe {
  readonly key: string;
  readonly partition: number | null;
  readonly offset: string | null;
}

export interface ProbeResponse {
  readonly topic: string;
  readonly partitions: number;
  readonly probes: readonly PartitionProbe[];
  readonly keyCount: number;
  readonly checkedAt: number;
}

export interface StreamNotice {
  readonly level: 'info' | 'warn' | 'error';
  readonly code: string;
  readonly message: string;
  /** Present on `group-join`: partition assignment reported by the group leader. */
  readonly assignment?: Readonly<Record<string, readonly number[]>>;
}

export interface StreamReady {
  readonly sessionId: string;
  readonly groupId: string;
  readonly topics: readonly string[];
  readonly fromBeginning: boolean;
  readonly flushMs: number;
  readonly kafka: readonly string[];
  /** Only a recording knows the topology without asking the broker. */
  readonly partitions?: Readonly<Record<string, number>>;
}

/** Recorded by `npm run capture` and replayed when no BFF is reachable. */
export interface FixtureEvent extends Omit<StreamEvent, 'ingestedAt'> {
  readonly deltaMs: number;
}

export interface FixtureFile {
  readonly capturedAt: number;
  readonly source: {
    readonly brokers: readonly string[];
    readonly topics: readonly string[];
    /** Partition count per topic, as declared on the broker that produced the recording. */
    readonly partitions?: Readonly<Record<string, number>>;
    readonly count: number;
  };
  readonly events: readonly FixtureEvent[];
}

/**
 * `live`/`reconnecting` come from the SSE transport, `demo` from the recorded
 * fixture, `offline` when neither is available.
 */
export type ConnectionMode = 'connecting' | 'live' | 'reconnecting' | 'demo' | 'offline';
