import { TestBed } from '@angular/core/testing';

import { StreamlensApi } from '../streamlens-api';
import type { StreamEvent, StreamReady } from '../models/stream.models';
import type { StreamOptions, StreamSink, StreamTransport } from './stream-transport';
import { StreamStore } from './stream-store';

/** Records what the store asked for and lets the test push events back in. */
class FakeTransport implements StreamTransport {
  readonly kind = 'sse' as const;
  sink: StreamSink | null = null;
  options: StreamOptions | null = null;
  starts = 0;
  stops = 0;

  start(options: StreamOptions, sink: StreamSink): void {
    this.starts += 1;
    this.options = options;
    this.sink = sink;
    sink.ready({
      sessionId: options.sessionId,
      groupId: `streamlens-ui-${options.sessionId}`,
      topics: ['orders'],
      fromBeginning: options.fromBeginning,
      flushMs: 120,
      kafka: ['localhost:9092'],
    });
    sink.state('live');
  }

  stop(): void {
    this.stops += 1;
  }

  /** Simulates what the SSE transport does with a batch of frames. */
  emit(events: readonly StreamEvent[]): void {
    this.sink?.events(events);
  }

  announce(ready: Partial<StreamReady>): void {
    this.sink?.ready({
      sessionId: 'other-session',
      groupId: 'other',
      topics: ['orders'],
      fromBeginning: false,
      flushMs: 120,
      kafka: [],
      ...ready,
    });
  }
}

const event = (
  partition: number,
  offset: string,
  overrides: Partial<StreamEvent> = {},
): StreamEvent => ({
  id: `orders:${partition}:${offset}`,
  topic: 'orders',
  partition,
  offset,
  key: 'ord-1',
  type: 'order.created',
  at: 1_000,
  payload: {},
  ingestedAt: 1_010,
  ...overrides,
});

describe('StreamStore', () => {
  let store: StreamStore;
  let transport: FakeTransport;
  let replay: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    replay = vi.fn(async () => ({
      groupId: 'streamlens-ui-test',
      topic: 'orders',
      requested: 200,
      perPartition: 34,
      partitions: [],
    }));

    TestBed.configureTestingModule({
      providers: [{ provide: StreamlensApi, useValue: { replay } }],
    });

    store = TestBed.inject(StreamStore);
    transport = new FakeTransport();
    store.attach(transport);
  });

  afterEach(() => {
    store.disconnect();
    TestBed.resetTestingModule();
  });

  it('connects through the attached transport using the session consumer group', () => {
    store.connect();

    expect(transport.starts).toBe(1);
    expect(transport.options?.sessionId).toBe(store.sessionId());
    expect(store.transportKind()).toBe('sse');
    expect(store.mode()).toBe('live');
    expect(store.ready()?.groupId).toBe(`streamlens-ui-${store.sessionId()}`);
  });

  it('keeps the newest record first and drops re-deliveries', () => {
    store.connect();

    transport.emit([event(0, '1'), event(0, '2')]);
    transport.emit([event(0, '2'), event(0, '3')]);

    expect(store.events().map((item) => item.offset)).toEqual(['3', '2', '1']);
    expect(store.counters().accepted).toBe(3);
    expect(store.counters().duplicates).toBe(1);
    expect(store.counters().batches).toBe(2);
  });

  it('caps the buffer at bufferLimit, keeping the newest records', () => {
    store.connect();

    const batch = Array.from({ length: store.bufferLimit + 25 }, (_, index) =>
      event(0, String(index + 1)),
    );
    transport.emit(batch);

    expect(store.events()).toHaveLength(store.bufferLimit);
    expect(store.events()[0].offset).toBe(String(store.bufferLimit + 25));
    expect(store.bufferFull()).toBe(true);
  });

  it('counts gaps in the offsets it never received', () => {
    store.connect();

    transport.emit([event(0, '10')]);
    transport.emit([event(0, '14')]);

    expect(store.counters().missed).toBe(3);
  });

  it('discards records while paused and reports how many', () => {
    store.connect();
    store.togglePause();

    transport.emit([event(0, '1'), event(0, '2')]);

    expect(store.events()).toEqual([]);
    expect(store.counters().droppedWhilePaused).toBe(2);

    store.togglePause();
    transport.emit([event(0, '3')]);
    expect(store.events()).toHaveLength(1);
  });

  it('filters by type and free text', () => {
    store.connect();
    transport.emit([
      event(0, '1', { type: 'order.created' }),
      event(0, '2', { type: 'payment.failed', key: 'ord-9' }),
      event(1, '1', { type: 'payment.failed', key: 'ord-9' }),
    ]);

    store.setFilters({ type: 'payment.failed' });
    expect(store.filtered()).toHaveLength(2);

    store.setFilters({ query: 'ord-9' });
    expect(store.filtered()).toHaveLength(2);

    store.resetFilters();
    expect(store.filtered()).toHaveLength(3);
  });

  it('clears the buffer when a new stream generation starts', () => {
    store.connect();
    transport.emit([event(0, '1')]);
    expect(store.events()).toHaveLength(1);

    transport.announce({ sessionId: 'demo-2' });

    expect(store.events()).toEqual([]);
    expect(store.positions()).toEqual([]);
  });

  it('rewinds the consumer group, drops history and reconnects', async () => {
    store.connect();
    transport.emit([event(0, '1')]);

    await store.rewind(200, 'orders');

    expect(replay).toHaveBeenCalledWith(store.sessionId(), 200, 'orders');
    expect(store.events()).toEqual([]);
    expect(store.counters().accepted).toBe(0);
    expect(store.replaying()).toBe(true);
    expect(transport.starts).toBe(2);
    expect(store.notices().some((notice) => notice.code === 'rewound')).toBe(true);
  });

  it('keeps streaming when the rewind fails', async () => {
    replay.mockRejectedValueOnce(new Error('409 stream is still active'));
    store.connect();

    await store.rewind(200, 'orders');

    expect(store.notices().some((notice) => notice.code === 'rewind-failed')).toBe(true);
    expect(transport.starts).toBe(2);
    expect(store.replaying()).toBe(false);
  });

  it('starts over with a brand new consumer group', () => {
    store.connect();
    const previous = store.sessionId();

    store.startOver({ fromBeginning: true });

    expect(store.sessionId()).not.toBe(previous);
    expect(transport.options?.sessionId).toBe(store.sessionId());
    expect(transport.options?.fromBeginning).toBe(true);
    expect(store.notices().some((notice) => notice.code === 'session-rotated')).toBe(true);
  });

  it('exposes per-partition positions for the resume panel', () => {
    store.connect();

    transport.emit([event(0, '4'), event(1, '9')]);

    expect(store.positions()).toEqual([
      { topic: 'orders', partition: 0, offset: '4' },
      { topic: 'orders', partition: 1, offset: '9' },
    ]);
  });
});
