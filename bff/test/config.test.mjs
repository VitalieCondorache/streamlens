/**
 * Configuration is the contract between the deployment and the code: every value
 * is env driven, with a fallback that has to survive junk.
 */
process.env.PORT = '5199';
process.env.KAFKA_BROKERS = 'broker-a:9092, broker-b:9092 ,';
process.env.STREAM_FLUSH_MS = 'nonsense';
process.env.SIMULATOR_ENABLED = 'false';
process.env.SIMULATOR_RATE = '77';

const { config, TOPICS, allTopics, findTopic, partitionCount, topicNames } =
  await import('../src/config.mjs');

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

describe('config', () => {
  it('reads integers from the environment', () => {
    assert.equal(config.port, 5199);
    assert.equal(config.simulator.defaultRate, 77);
  });

  it('falls back when a numeric value is junk', () => {
    assert.equal(config.stream.flushMs, 120);
  });

  it('parses the broker list, trimming and dropping empties', () => {
    assert.deepEqual(config.brokers, ['broker-a:9092', 'broker-b:9092']);
  });

  it('reads booleans as opt-in', () => {
    assert.equal(config.simulator.running, false);
  });

  it('declares the topology in code, because auto-creation is off', () => {
    assert.deepEqual(topicNames, ['streamlens.orders', 'streamlens.telemetry']);
    assert.equal(partitionCount('streamlens.orders'), 6);
    assert.equal(partitionCount('streamlens.telemetry'), 3);
    assert.equal(partitionCount('does-not-exist'), 1);
  });

  it('keeps the window and burst weights in sync with the events they describe', () => {
    for (const topic of TOPICS) {
      const weights = Object.keys(topic.weights);
      assert.ok(weights.length > 0, `${topic.name} has no weights`);
      assert.deepEqual(Object.keys(topic.burstWeights).sort(), [...weights].sort());
      assert.ok(Object.values(topic.weights).every((weight) => weight > 0));
      assert.ok(Object.values(topic.burstWeights).every((weight) => weight > 0));
    }
  });

  it('includes the probe topic in the topics the BFF knows about', () => {
    assert.equal(allTopics.at(-1).name, config.probeTopic);
    assert.equal(allTopics.length, TOPICS.length + 1);
    assert.equal(findTopic(config.probeTopic), undefined);
  });
});
