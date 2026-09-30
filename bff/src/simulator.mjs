/**
 * Traffic generator.
 *
 * It publishes through the very same producer the bridge uses, and the UI reads
 * those records back through a real consumer group — the whole pipeline is
 * exercised end to end, nothing is faked in the middle.
 */
import { config, TOPICS } from './config.mjs';
import { createEventFactory } from './events.mjs';
import { log } from './log.mjs';

const TICK_MS = 100;
const ORDERS_SHARE = 0.72;
const BURST_MIN_MS = 6_000;
const BURST_MAX_MS = 15_000;

const clampRate = (value) => Math.max(0, Math.min(Math.round(value), config.simulator.maxRate));

export const createSimulator = (bridge, random = Math.random) => {
  const factory = createEventFactory(random);
  let timer = null;
  let rate = clampRate(config.simulator.defaultRate);
  let carry = 0;
  let produced = 0;
  let failed = 0;
  let burstUntil = 0;
  let backingOff = false;

  const state = () => ({
    running: timer !== null,
    ratePerSecond: rate,
    produced,
    failed,
    burst: factory.burst,
    burstUntil: factory.burst ? burstUntil : null,
  });

  const openBurstWindows = () => {
    const now = Date.now();
    if (factory.burst && now > burstUntil) {
      factory.setBurst(false);
      log.info('simulator burst ended', { produced });
      return;
    }
    if (!factory.burst && random() < config.simulator.burstChance) {
      burstUntil = Math.round(now + BURST_MIN_MS + random() * (BURST_MAX_MS - BURST_MIN_MS));
      factory.setBurst(true);
      log.warn('simulator burst started', { until: burstUntil });
    }
  };

  const buildMessages = () => {
    carry += (rate * TICK_MS) / 1000;
    const count = Math.floor(carry);
    carry -= count;
    if (count === 0) return new Map();

    const byTopic = new Map();
    for (let index = 0; index < count; index += 1) {
      const topic = random() < ORDERS_SHARE ? TOPICS[0].name : TOPICS[1].name;
      const event = factory.next(topic);
      const at = Date.now();
      const messages = byTopic.get(topic) ?? [];
      messages.push({
        key: event.key,
        value: JSON.stringify({ type: event.type, at, payload: event.payload }),
        timestamp: String(at),
      });
      byTopic.set(topic, messages);
    }
    return byTopic;
  };

  const tick = async () => {
    openBurstWindows();
    if (backingOff) return;

    const byTopic = buildMessages();
    if (byTopic.size === 0) return;

    try {
      await Promise.all(
        [...byTopic].map(([topic, messages]) => bridge.produce({ topic, messages, acks: -1 })),
      );
      produced += [...byTopic.values()].reduce((sum, messages) => sum + messages.length, 0);
    } catch (error) {
      failed += 1;
      // The broker may be restarting: skip a beat instead of hammering it.
      backingOff = true;
      log.warn('simulator produce failed', { error: error.message });
      setTimeout(() => {
        backingOff = false;
      }, 2_000);
    }
  };

  const start = (nextRate) => {
    if (nextRate !== undefined) rate = clampRate(nextRate);
    if (timer === null) {
      timer = setInterval(() => void tick(), TICK_MS);
      log.info('simulator started', { ratePerSecond: rate });
    }
    return state();
  };

  const stop = () => {
    if (timer !== null) {
      clearInterval(timer);
      timer = null;
      log.info('simulator stopped', { produced, failed });
    }
    return state();
  };

  const setRate = (value) => {
    rate = clampRate(value);
    return state();
  };

  return { start, stop, setRate, state };
};
