import { FixtureStreamTransport } from './fixture-transport';
import type { FixtureFile, StreamEvent, StreamNotice, StreamReady } from '../models/stream.models';
import type { StreamSink } from './stream-transport';

const fixture: FixtureFile = {
  capturedAt: 1_700_000_000_000,
  source: { brokers: ['localhost:9092'], topics: ['orders'], count: 3 },
  events: [
    {
      id: 'orders:0:1',
      topic: 'orders',
      partition: 0,
      offset: '1',
      key: 'ord-1',
      type: 'order.created',
      at: 1,
      payload: { orderId: 'ord-1' },
      deltaMs: 0,
    },
    {
      id: 'orders:1:1',
      topic: 'orders',
      partition: 1,
      offset: '1',
      key: 'ord-2',
      type: 'payment.failed',
      at: 2,
      payload: { orderId: 'ord-2' },
      deltaMs: 50,
    },
    {
      id: 'orders:0:2',
      topic: 'orders',
      partition: 0,
      offset: '2',
      key: 'ord-1',
      type: 'payment.authorized',
      at: 3,
      payload: { orderId: 'ord-1' },
      deltaMs: 50,
    },
  ],
};

const createSink = () => {
  const ready: StreamReady[] = [];
  const events: StreamEvent[] = [];
  const notices: StreamNotice[] = [];
  const states: string[] = [];

  const sink: StreamSink = {
    ready: (value) => ready.push(value),
    events: (batch) => events.push(...batch),
    notice: (value) => notices.push(value),
    state: (value) => states.push(value),
  };

  return { sink, ready, events, notices, states };
};

describe('FixtureStreamTransport', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('announces the recording before replaying it', async () => {
    const transport = new FixtureStreamTransport({ fixture, minGapMs: 0 });
    const capture = createSink();

    await transport.start({ sessionId: 'ignored', topics: [], fromBeginning: false }, capture.sink);

    expect(capture.ready).toHaveLength(1);
    expect(capture.ready[0].groupId).toBe('replay:recorded-session');
    expect(capture.ready[0].topics).toEqual(['orders']);
    expect(capture.states).toContain('connecting');
    expect(capture.states).toContain('demo');
    expect(capture.notices[0].code).toBe('demo-mode');

    transport.stop();
  });

  it('replays the recorded order, one record per delay', async () => {
    const transport = new FixtureStreamTransport({ fixture, minGapMs: 0 });
    const capture = createSink();
    await transport.start({ sessionId: 'ignored', topics: [], fromBeginning: false }, capture.sink);

    await vi.advanceTimersByTimeAsync(0);
    expect(capture.events.map((event) => event.id)).toEqual(['orders:0:1']);

    await vi.advanceTimersByTimeAsync(50);
    expect(capture.events.map((event) => event.id)).toEqual(['orders:0:1', 'orders:1:1']);

    await vi.advanceTimersByTimeAsync(50);
    expect(capture.events.map((event) => event.id)).toEqual([
      'orders:0:1',
      'orders:1:1',
      'orders:0:2',
    ]);

    transport.stop();
  });

  it('re-bases timestamps and synthesises a pipeline delay', async () => {
    const transport = new FixtureStreamTransport({ fixture, minGapMs: 0 });
    const capture = createSink();
    await transport.start({ sessionId: 'ignored', topics: [], fromBeginning: false }, capture.sink);
    await vi.advanceTimersByTimeAsync(0);

    const [event] = capture.events;
    const now = Date.now();

    expect(event.at).toBeCloseTo(now, -2);
    expect(event.ingestedAt).toBeGreaterThan(event.at);

    transport.stop();
  });

  it('starts a new pass with a fresh session id after the loop pause', async () => {
    const transport = new FixtureStreamTransport({ fixture, minGapMs: 0, loopPauseMs: 500 });
    const capture = createSink();
    await transport.start({ sessionId: 'ignored', topics: [], fromBeginning: false }, capture.sink);

    await vi.advanceTimersByTimeAsync(150);
    await vi.advanceTimersByTimeAsync(500);

    expect(capture.ready.map((ready) => ready.sessionId)).toEqual(['demo-1', 'demo-2']);
    expect(capture.notices.some((notice) => notice.code === 'replay-restart')).toBe(true);

    transport.stop();
  });

  it('does not emit anything after stop()', async () => {
    const transport = new FixtureStreamTransport({ fixture, minGapMs: 0 });
    const capture = createSink();
    await transport.start({ sessionId: 'ignored', topics: [], fromBeginning: false }, capture.sink);

    transport.stop();
    await vi.advanceTimersByTimeAsync(1_000);

    expect(capture.events).toEqual([]);
  });

  it('reports offline when the recording is not available', async () => {
    const transport = new FixtureStreamTransport({ url: 'http://127.0.0.1:1/demo-stream.json' });
    const capture = createSink();

    await transport.start({ sessionId: 'ignored', topics: [], fromBeginning: false }, capture.sink);

    expect(capture.states).toContain('offline');
    expect(capture.ready).toEqual([]);
  });
});
