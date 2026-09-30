/**
 * The SSE hub — the half of the pipeline the README makes the loudest claims about.
 *
 * Everything here is observable from the wire: the frame format, the resume token in
 * every `id:`, the pause/drain backpressure, and the protection against a client that
 * never drains. A fake consumer and a fake socket keep it deterministic.
 */
process.env.STREAM_FLUSH_MS = '10';
process.env.STREAM_HEARTBEAT_MS = '60';

import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { describe, it } from 'node:test';

const { createStreamHub } = await import('../src/stream.mjs');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const quietLogger = { info() {}, warn() {}, error() {}, debug() {} };

class FakeResponse extends EventEmitter {
  constructor({ saturated = false } = {}) {
    super();
    this.chunks = [];
    this.headers = null;
    this.headersSent = false;
    this.saturated = saturated;
    this.ended = false;
  }

  writeHead(status, headers) {
    this.headers = headers;
    this.headersSent = true;
  }

  write(chunk) {
    this.chunks.push(String(chunk));
    return !this.saturated;
  }

  end() {
    this.ended = true;
  }

  get text() {
    return this.chunks.join('');
  }
}

const fakeRequest = () => Object.assign(new EventEmitter(), { headers: { host: 'localhost' } });

const fakeConsumer = () => {
  const consumer = new EventEmitter();
  consumer.events = { GROUP_JOIN: 'GROUP_JOIN', REBALANCING: 'REBALANCING', CRASH: 'CRASH' };
  consumer.paused = [];
  consumer.resumed = [];
  consumer.disconnected = false;
  consumer.connect = async () => {};
  consumer.subscribe = async (options) => {
    consumer.subscription = options;
  };
  consumer.run = async ({ eachBatch }) => {
    consumer.eachBatch = eachBatch;
  };
  consumer.disconnect = async () => {
    consumer.disconnected = true;
  };
  consumer.pause = (assignment) => consumer.paused.push(assignment);
  consumer.resume = (assignment) => consumer.resumed.push(assignment);
  return consumer;
};

const fakeBridge = () => {
  const consumers = [];
  return {
    consumers,
    newConsumer: () => {
      const consumer = fakeConsumer();
      consumers.push(consumer);
      return consumer;
    },
  };
};

/** Boots a hub, opens one connection and hands back everything the test needs. */
const open = async ({ query = '', saturated = false } = {}) => {
  const bridge = fakeBridge();
  const hub = createStreamHub(bridge, quietLogger);
  const res = new FakeResponse({ saturated });
  await hub.handle(fakeRequest(), res, new URL(`/api/stream${query}`, 'http://localhost'));
  return { bridge, hub, res, consumer: bridge.consumers.at(-1) };
};

/** Pulls the JSON payload of one SSE event out of the raw stream. */
const dataOf = (text, event) => {
  const block = text.split('\n\n').find((candidate) => candidate.includes(`event: ${event}`));
  if (!block) return null;
  const line = block.split('\n').find((candidate) => candidate.startsWith('data: '));
  return JSON.parse(line.slice('data: '.length));
};

const flatRecord = (overrides = {}) => ({
  key: Buffer.from('ord-1000'),
  value: Buffer.from(
    JSON.stringify({ type: 'payment.authorized', at: 1_700_000_000_000, payload: {} }),
  ),
  offset: '41',
  timestamp: '1700000000000',
  ...overrides,
});

describe('stream hub', () => {
  it('opens with a retry hint and a ready frame that names the consumer group', async () => {
    const { hub, res } = await open({ query: '?session=abc123&topics=streamlens.orders' });

    try {
      assert.match(res.text, /^retry: 2000\n\n/);
      const ready = dataOf(res.text, 'stream-ready');
      assert.equal(ready.sessionId, 'abc123');
      assert.equal(ready.groupId, 'streamlens-ui-abc123');
      assert.deepEqual(ready.topics, ['streamlens.orders']);
      assert.equal(ready.fromBeginning, false);
      assert.equal(res.headers['X-StreamLens-Session'], 'abc123');
      assert.equal(res.headers['Cache-Control'], 'no-cache, no-transform');
    } finally {
      await hub.closeAll();
    }
  });

  it('sanitises the session id, because it becomes a consumer group name', async () => {
    const { hub, res } = await open({ query: '?session=..%2Fevil%20$%7Bx%7D' });

    try {
      assert.equal(dataOf(res.text, 'stream-ready').sessionId, 'evilx');
    } finally {
      await hub.closeAll();
    }
  });

  it('generates a session id when the client does not send one', async () => {
    const { hub, res } = await open();

    try {
      const ready = dataOf(res.text, 'stream-ready');
      assert.match(ready.sessionId, /^[0-9a-f-]{36}$/);
      assert.deepEqual(ready.topics, ['streamlens.orders', 'streamlens.telemetry']);
    } finally {
      await hub.closeAll();
    }
  });

  it('drops topics it does not own instead of subscribing to them', async () => {
    const { hub, consumer } = await open({ query: '?topics=streamlens.telemetry,nope' });

    try {
      assert.deepEqual(consumer.subscription.topics, ['streamlens.telemetry']);
      assert.equal(consumer.subscription.fromBeginning, false);
    } finally {
      await hub.closeAll();
    }
  });

  it('puts Kafka coordinates in the frame id, with offsets kept as strings', async () => {
    const { hub, res, consumer } = await open({ query: '?session=s1' });

    try {
      await consumer.eachBatch({
        batch: {
          topic: 'streamlens.orders',
          partition: 3,
          messages: [flatRecord({ offset: '9007199254740993' })],
        },
      });
      await sleep(40);

      const event = dataOf(res.text, 'stream-event');
      assert.equal(event.id, 'streamlens.orders:3:9007199254740993');
      assert.equal(event.offset, '9007199254740993');
      assert.equal(event.partition, 3);
      assert.equal(event.key, 'ord-1000');
      assert.equal(event.type, 'payment.authorized');
      assert.match(res.text, /id: streamlens.orders:3:9007199254740993/);
    } finally {
      await hub.closeAll();
    }
  });

  it('reports an undecodable value as unparseable rather than dying', async () => {
    const { hub, res, consumer } = await open({ query: '?session=s2' });

    try {
      await consumer.eachBatch({
        batch: {
          topic: 'streamlens.orders',
          partition: 0,
          messages: [flatRecord({ key: null, value: Buffer.from('{ broken'), offset: '7' })],
        },
      });
      await sleep(40);

      const event = dataOf(res.text, 'stream-event');
      assert.equal(event.type, 'unparseable');
      assert.equal(event.key, null);
      assert.match(event.id, /:0:7$/);
    } finally {
      await hub.closeAll();
    }
  });

  it('replaces a half-dead connection instead of leaking its group member', async () => {
    const bridge = fakeBridge();
    const hub = createStreamHub(bridge, quietLogger);
    const url = new URL('/api/stream?session=dup', 'http://localhost');

    try {
      const first = new FakeResponse();
      await hub.handle(fakeRequest(), first, url);
      const second = new FakeResponse();
      await hub.handle(fakeRequest(), second, url);

      assert.equal(bridge.consumers[0].disconnected, true);
      assert.equal(first.ended, true);
      assert.equal(hub.stats().supersededConnections, 1);
      assert.equal(hub.stats().activeSessions, 1);
      assert.equal(hub.isActive('dup'), true);
    } finally {
      await hub.closeAll();
    }
  });

  it('pauses the consumer when the socket is saturated and resumes on drain', async () => {
    const { hub, res, consumer } = await open({
      query: '?session=slow&topics=streamlens.orders',
      saturated: true,
    });
    const assignment = [{ topic: 'streamlens.orders', partitions: [0, 1, 2, 3, 4, 5] }];

    try {
      await consumer.eachBatch({
        batch: { topic: 'streamlens.orders', partition: 1, messages: [flatRecord()] },
      });
      await sleep(60);

      assert.equal(hub.stats().backpressurePauses, 1);
      assert.deepEqual(consumer.paused, [assignment]);
      assert.equal(dataOf(res.text, 'stream-notice').code, 'backpressure');

      res.emit('drain');
      await sleep(60);

      assert.deepEqual(consumer.resumed, [assignment]);
      assert.ok(res.text.includes('"code":"resumed"'));
    } finally {
      await hub.closeAll();
    }
  });

  it('keeps an idle connection alive with heartbeat comments', async () => {
    const { hub, res } = await open({ query: '?session=idle' });

    try {
      assert.equal(res.text.includes(': heartbeat'), false);
      await sleep(200);

      assert.match(res.text, /: heartbeat \d+/);
    } finally {
      await hub.closeAll();
    }
  });
});
