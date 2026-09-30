/**
 * The traffic generator's rate handling and its reaction to a failing broker.
 *
 * It talks to the bridge through the same interface the real one uses, so a fake
 * bridge is enough to pin the behaviour without a Kafka in sight.
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { createSimulator } from '../src/simulator.mjs';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const fakeBridge = ({ fail = false } = {}) => {
  const batches = [];
  return {
    batches,
    produce: async (batch) => {
      if (fail) throw new Error('broker down');
      batches.push(batch);
    },
  };
};

describe('simulator', () => {
  it('clamps the rate to the declared ceiling and floor', () => {
    const simulator = createSimulator(fakeBridge());

    assert.equal(simulator.setRate(10_000).ratePerSecond, 500);
    assert.equal(simulator.setRate(-5).ratePerSecond, 0);
    assert.equal(simulator.setRate(14.6).ratePerSecond, 15);
    assert.equal(simulator.state().running, false);
  });

  it('produces records that carry a key and a decodable payload', async () => {
    const bridge = fakeBridge();
    const simulator = createSimulator(bridge, () => 0.5);

    try {
      simulator.start(200);
      await sleep(150);

      const state = simulator.state();
      assert.equal(state.running, true);
      assert.ok(state.produced > 0, 'nothing was produced');
      assert.equal(state.failed, 0);

      const [batch] = bridge.batches;
      assert.equal(batch.topic, 'streamlens.orders');
      assert.equal(batch.acks, -1);
      assert.ok(batch.messages.length > 0);
      for (const message of batch.messages) {
        assert.equal(typeof message.key, 'string');
        const decoded = JSON.parse(message.value);
        assert.equal(typeof decoded.type, 'string');
        assert.equal(typeof decoded.payload, 'object');
        assert.equal(message.timestamp, String(decoded.at));
      }
    } finally {
      simulator.stop();
    }
  });

  it('stops producing when it is stopped', async () => {
    const bridge = fakeBridge();
    const simulator = createSimulator(bridge, () => 0.5);

    simulator.start(200);
    await sleep(150);
    const produced = simulator.state().produced;
    simulator.stop();

    await sleep(200);
    assert.equal(simulator.state().running, false);
    assert.equal(simulator.state().produced, produced);
  });

  it('counts a failed produce instead of throwing', async () => {
    const bridge = fakeBridge({ fail: true });
    const simulator = createSimulator(bridge, () => 0.5);

    try {
      simulator.start(200);
      await sleep(150);

      assert.equal(simulator.state().failed, 1);
      assert.equal(simulator.state().produced, 0);
    } finally {
      simulator.stop();
    }
  });
});
