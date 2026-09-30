import { buildStreamStats, filterEvents, percentile } from './stream-stats';
import type { StreamEvent } from '../models/stream.models';

const event = (overrides: Partial<StreamEvent> = {}): StreamEvent => ({
  id: 'orders:0:1',
  topic: 'orders',
  partition: 0,
  offset: '1',
  key: 'ord-1',
  type: 'order.created',
  at: 1_000,
  payload: { orderId: 'ord-1' },
  ingestedAt: 1_010,
  ...overrides,
});

describe('percentile', () => {
  it('returns zero for an empty series', () => {
    expect(percentile([], 0.95)).toBe(0);
  });

  it('uses the nearest rank', () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(percentile(sorted, 0.5)).toBe(5);
    expect(percentile(sorted, 0.9)).toBe(9);
    expect(percentile(sorted, 1)).toBe(10);
  });
});

describe('buildStreamStats', () => {
  it('handles an empty buffer', () => {
    const stats = buildStreamStats([]);

    expect(stats.buffered).toBe(0);
    expect(stats.byType).toEqual([]);
    expect(stats.failureRate).toBe(0);
    expect(stats.latency).toEqual({ latest: 0, p50: 0, p95: 0, max: 0 });
  });

  it('counts types, topics, partitions and keys', () => {
    const stats = buildStreamStats([
      event({ id: 'a', type: 'order.created', key: 'ord-1', partition: 0 }),
      event({ id: 'b', type: 'payment.failed', key: 'ord-1', partition: 0 }),
      event({ id: 'c', type: 'payment.failed', key: 'ord-2', partition: 1 }),
      event({ id: 'd', type: 'scanner.heartbeat', topic: 'telemetry', key: null, partition: 0 }),
    ]);

    expect(stats.buffered).toBe(4);
    expect(stats.byType[0]).toEqual({ type: 'payment.failed', count: 2, share: 0.5 });
    expect(stats.byTopic).toEqual([
      { topic: 'orders', count: 3 },
      { topic: 'telemetry', count: 1 },
    ]);
    expect(stats.byPartition).toEqual([
      { topic: 'orders', partition: 0, count: 2 },
      { topic: 'orders', partition: 1, count: 1 },
      { topic: 'telemetry', partition: 0, count: 1 },
    ]);
    expect(stats.topKeys[0]).toEqual({ key: 'ord-1', count: 2 });
  });

  it('treats failed and degraded types as failures', () => {
    const stats = buildStreamStats([
      event({ id: 'a', type: 'payment.failed' }),
      event({ id: 'b', type: 'scanner.degraded' }),
      event({ id: 'c', type: 'order.created' }),
      event({ id: 'd', type: 'order.created' }),
    ]);

    expect(stats.failures).toBe(2);
    expect(stats.failureRate).toBe(0.5);
  });

  it('reports pipeline latency percentiles and never goes negative', () => {
    const stats = buildStreamStats([
      event({ id: 'a', at: 1_000, ingestedAt: 1_005 }),
      event({ id: 'b', at: 1_000, ingestedAt: 1_050 }),
      event({ id: 'c', at: 1_000, ingestedAt: 1_100 }),
      // Clock skew between producer and bridge must not produce negative latency.
      event({ id: 'd', at: 2_000, ingestedAt: 1_990 }),
    ]);

    expect(stats.latency.max).toBe(100);
    expect(stats.latency.p95).toBe(100);
    expect(stats.latency.latest).toBe(0);
  });
});

describe('filterEvents', () => {
  const events = [
    event({ id: 'a', topic: 'orders', type: 'order.created', key: 'ord-1' }),
    event({ id: 'b', topic: 'orders', type: 'payment.failed', key: 'ord-2' }),
    event({ id: 'c', topic: 'telemetry', type: 'scanner.heartbeat', key: 'scan-1' }),
  ];

  it('passes everything through by default', () => {
    expect(filterEvents(events, { topic: 'all', type: 'all', query: '' })).toHaveLength(3);
  });

  it('filters by topic and type together', () => {
    const filtered = filterEvents(events, { topic: 'orders', type: 'payment.failed', query: '' });
    expect(filtered.map((item) => item.id)).toEqual(['b']);
  });

  it('searches keys and payload content, case insensitively', () => {
    expect(
      filterEvents(events, { topic: 'all', type: 'all', query: 'SCAN-1' }).map((item) => item.id),
    ).toEqual(['c']);
    expect(
      filterEvents(events, { topic: 'all', type: 'all', query: 'ord-2' }).map((item) => item.id),
    ).toEqual(['b']);
  });
});
