import { PartitionWatermarks } from './partition-watermarks';
import type { StreamEvent } from '../models/stream.models';

const event = (partition: number, offset: string, topic = 'orders'): StreamEvent => ({
  id: `${topic}:${partition}:${offset}`,
  topic,
  partition,
  offset,
  key: 'ord-1',
  type: 'order.created',
  at: 1,
  payload: {},
  ingestedAt: 2,
});

describe('PartitionWatermarks', () => {
  it('accepts a fresh sequence in order', () => {
    const watermarks = new PartitionWatermarks();

    const outcome = watermarks.accept([event(0, '1'), event(0, '2'), event(0, '3')]);

    expect(outcome.accepted.map((item) => item.offset)).toEqual(['1', '2', '3']);
    expect(outcome.duplicates).toBe(0);
    expect(outcome.missed).toBe(0);
    expect(outcome.late).toBe(0);
  });

  it('drops exact re-deliveries and counts them as duplicates', () => {
    const watermarks = new PartitionWatermarks();
    watermarks.accept([event(0, '7')]);

    const outcome = watermarks.accept([event(0, '7'), event(0, '8')]);

    expect(outcome.accepted.map((item) => item.offset)).toEqual(['8']);
    expect(outcome.duplicates).toBe(1);
  });

  it('counts the records it never saw when offsets jump', () => {
    const watermarks = new PartitionWatermarks();
    watermarks.accept([event(0, '10')]);

    const outcome = watermarks.accept([event(0, '15')]);

    expect(outcome.missed).toBe(4);
    expect(outcome.accepted.map((item) => item.offset)).toEqual(['15']);
  });

  it('treats an older offset as late, not as a new record', () => {
    const watermarks = new PartitionWatermarks();
    watermarks.accept([event(0, '10')]);

    const outcome = watermarks.accept([event(0, '9')]);

    expect(outcome.accepted).toHaveLength(0);
    expect(outcome.late).toBe(1);
  });

  it('tracks every partition independently', () => {
    const watermarks = new PartitionWatermarks();
    watermarks.accept([event(0, '5'), event(1, '2'), event(2, '9')]);

    const outcome = watermarks.accept([event(0, '5'), event(1, '3'), event(2, '8')]);

    expect(outcome.duplicates).toBe(1);
    expect(outcome.late).toBe(1);
    expect(outcome.accepted.map((item) => item.partition)).toEqual([1]);
  });

  it('does not lose precision on int64 offsets', () => {
    const watermarks = new PartitionWatermarks();
    // 2^53 + 1 cannot be represented exactly by a JS number.
    const first = '9007199254740993';
    const second = '9007199254740994';

    expect(watermarks.accept([event(0, first)]).accepted).toHaveLength(1);

    const outcome = watermarks.accept([event(0, second)]);
    expect(outcome.accepted.map((item) => item.offset)).toEqual([second]);
    expect(outcome.missed).toBe(0);
  });

  it('snapshots the resume token per partition and can reset', () => {
    const watermarks = new PartitionWatermarks();
    watermarks.accept([event(1, '4'), event(0, '2')]);

    expect(watermarks.snapshot()).toEqual([
      { topic: 'orders', partition: 0, offset: '2' },
      { topic: 'orders', partition: 1, offset: '4' },
    ]);

    watermarks.reset();
    expect(watermarks.snapshot()).toEqual([]);
  });
});
