/**
 * The other half of the backpressure contract: a client that never drains must not be
 * allowed to grow the server's buffer without bound. This file sets a tiny ceiling via
 * the environment (each test file runs in its own process) and watches the hub drop the
 * session instead.
 */
process.env.STREAM_FLUSH_MS = '1000';
process.env.STREAM_HEARTBEAT_MS = '5';
process.env.STREAM_MAX_BUFFER = '1';

import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { describe, it } from 'node:test';

const { createStreamHub } = await import('../src/stream.mjs');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class SaturatedResponse extends EventEmitter {
  constructor() {
    super();
    this.ended = false;
    this.headersSent = false;
  }

  writeHead() {
    this.headersSent = true;
  }

  write() {
    return false;
  }

  end() {
    this.ended = true;
  }
}

describe('stream hub buffer ceiling', () => {
  it('closes a session whose buffer runs past the ceiling', async () => {
    const consumers = [];
    const hub = createStreamHub(
      {
        newConsumer: () => {
          const consumer = Object.assign(new EventEmitter(), {
            events: { GROUP_JOIN: 'G', REBALANCING: 'R', CRASH: 'C' },
            connect: async () => {},
            subscribe: async () => {},
            run: async ({ eachBatch }) => {
              consumer.eachBatch = eachBatch;
            },
            disconnect: async () => {
              consumer.disconnected = true;
            },
            pause: () => {},
            resume: () => {},
          });
          consumers.push(consumer);
          return consumer;
        },
      },
      { info() {}, warn() {}, error() {}, debug() {} },
    );

    const res = new SaturatedResponse();
    const req = Object.assign(new EventEmitter(), { headers: { host: 'localhost' } });

    try {
      await hub.handle(req, res, new URL('/api/stream?session=flood', 'http://localhost'));

      // Five frames arrive in one tick, while the flush timer is a full second away.
      await consumers[0].eachBatch({
        batch: {
          topic: 'streamlens.orders',
          partition: 0,
          messages: Array.from({ length: 5 }, (_, index) => ({
            key: null,
            value: Buffer.from(JSON.stringify({ type: 'order.created', at: 1, payload: {} })),
            offset: String(index),
            timestamp: '1',
          })),
        },
      });
      await sleep(60);

      assert.equal(consumers[0].disconnected, true);
      assert.equal(res.ended, true);
      assert.equal(hub.isActive('flood'), false);
      assert.equal(hub.stats().closed, 1);
    } finally {
      await hub.closeAll();
    }
  });
});
