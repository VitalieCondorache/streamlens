import type { StreamEvent } from '../models/stream.models';

export interface TypeCount {
  readonly type: string;
  readonly count: number;
  readonly share: number;
}

export interface TopicCount {
  readonly topic: string;
  readonly count: number;
}

export interface KeyCount {
  readonly key: string;
  readonly count: number;
}

export interface PartitionCount {
  readonly topic: string;
  readonly partition: number;
  readonly count: number;
}

export interface LatencyStats {
  readonly latest: number;
  readonly p50: number;
  readonly p95: number;
  readonly max: number;
}

export interface StreamStats {
  readonly buffered: number;
  readonly byType: readonly TypeCount[];
  readonly byTopic: readonly TopicCount[];
  readonly byPartition: readonly PartitionCount[];
  readonly topKeys: readonly KeyCount[];
  readonly failures: number;
  readonly failureRate: number;
  readonly latency: LatencyStats;
}

/** A domain type counts as a failure when it says so in its name. */
const isFailure = (type: string): boolean => type.includes('failed') || type.includes('degraded');

/** Nearest-rank percentile on an ascending, already sorted array. */
export const percentile = (sorted: readonly number[], quantile: number): number => {
  if (sorted.length === 0) return 0;
  const rank = Math.ceil(quantile * sorted.length) - 1;
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank))];
};

/**
 * Aggregates the buffer into the numbers the dashboard shows.
 *
 * Pure on purpose: the interesting behaviour (percentiles, shares, partition
 * distribution) is unit tested without Angular, TestBed or a browser.
 */
export const buildStreamStats = (events: readonly StreamEvent[]): StreamStats => {
  const byType = new Map<string, number>();
  const byTopic = new Map<string, number>();
  const byPartition = new Map<string, PartitionCount>();
  const byKey = new Map<string, number>();
  const latencies: number[] = [];
  let failures = 0;

  for (const event of events) {
    byType.set(event.type, (byType.get(event.type) ?? 0) + 1);
    byTopic.set(event.topic, (byTopic.get(event.topic) ?? 0) + 1);

    const partitionKey = `${event.topic}#${event.partition}`;
    const partition = byPartition.get(partitionKey);
    if (partition === undefined) {
      byPartition.set(partitionKey, { topic: event.topic, partition: event.partition, count: 1 });
    } else {
      byPartition.set(partitionKey, { ...partition, count: partition.count + 1 });
    }

    if (event.key !== null) byKey.set(event.key, (byKey.get(event.key) ?? 0) + 1);
    if (isFailure(event.type)) failures += 1;

    // Clock skew between producer and BFF can make this negative in theory.
    latencies.push(Math.max(0, event.ingestedAt - event.at));
  }

  const total = events.length;
  const sortedLatencies = [...latencies].sort((a, b) => a - b);
  const descending = (a: { count: number }, b: { count: number }) => b.count - a.count;

  return {
    buffered: total,
    byType: [...byType]
      .map(([type, count]) => ({ type, count, share: total === 0 ? 0 : count / total }))
      .sort(descending),
    byTopic: [...byTopic].map(([topic, count]) => ({ topic, count })).sort(descending),
    byPartition: [...byPartition.values()].sort(
      (a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition,
    ),
    topKeys: [...byKey]
      .map(([key, count]) => ({ key, count }))
      .sort(descending)
      .slice(0, 6),
    failures,
    failureRate: total === 0 ? 0 : failures / total,
    latency: {
      latest: latencies.at(-1) ?? 0,
      p50: percentile(sortedLatencies, 0.5),
      p95: percentile(sortedLatencies, 0.95),
      max: sortedLatencies.at(-1) ?? 0,
    },
  };
};

export interface StreamFilters {
  readonly topic: 'all' | string;
  readonly type: 'all' | string;
  readonly query: string;
}

const matchesQuery = (event: StreamEvent, needle: string): boolean => {
  if (needle.length === 0) return true;
  const haystack = `${event.key ?? ''} ${event.type} ${event.topic} ${JSON.stringify(event.payload)}`;
  return haystack.toLowerCase().includes(needle);
};

export const filterEvents = (
  events: readonly StreamEvent[],
  filters: StreamFilters,
): readonly StreamEvent[] =>
  events.filter(
    (event) =>
      (filters.topic === 'all' || event.topic === filters.topic) &&
      (filters.type === 'all' || event.type === filters.type) &&
      matchesQuery(event, filters.query.trim().toLowerCase()),
  );
