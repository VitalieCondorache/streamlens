/**
 * The synthetic domain traffic.
 *
 * The generator takes its randomness as an argument, so these tests drive it with a
 * fixed source and assert the shape of what the dashboard will show — including the
 * invariants the Key Router view depends on (a stable key pool, coherent payloads).
 */
import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { createEventFactory } from '../src/events.mjs';

const ORDERS = 'streamlens.orders';
const TELEMETRY = 'streamlens.telemetry';

/** A deterministic stand-in for Math.random, so a "random" stream is reproducible. */
const seeded = (seed = 1) => {
  let state = seed;
  return () => {
    state = (state * 1_103_515_245 + 12_345) % 2_147_483_648;
    return state / 2_147_483_648;
  };
};

describe('event factory', () => {
  it('refuses a topic it does not know', () => {
    const factory = createEventFactory(() => 0);

    assert.throws(() => factory.next('streamlens.nope'), /unknown topic/);
  });

  it('is deterministic when the random source is', () => {
    const first = createEventFactory(() => 0).next(ORDERS);
    const second = createEventFactory(() => 0).next(ORDERS);

    assert.deepEqual(first, second);
    assert.equal(first.type, 'order.created');
    assert.equal(first.key, 'ord-1000');
  });

  it('picks types by weight, not uniformly', () => {
    // A roll at the very start lands on the first declared type, at the very end on the last.
    assert.equal(createEventFactory(() => 0).next(ORDERS).type, 'order.created');
    assert.equal(createEventFactory(() => 0.999).next(ORDERS).type, 'order.shipped');
    assert.equal(createEventFactory(() => 0.5).next(ORDERS).type, 'payment.authorized');
  });

  it('skews towards failures while a burst window is open', () => {
    const factory = createEventFactory(() => 0.5);

    assert.equal(factory.next(TELEMETRY).type, 'scanner.heartbeat');
    factory.setBurst(true);
    assert.equal(factory.burst, true);
    assert.equal(factory.next(TELEMETRY).type, 'scanner.degraded');
  });

  it('only emits types the topology declares', () => {
    const factory = createEventFactory(seeded(7));
    const declared = new Set([
      'order.created',
      'payment.authorized',
      'payment.failed',
      'order.shipped',
      'scanner.heartbeat',
      'scanner.degraded',
    ]);

    for (let index = 0; index < 200; index += 1) {
      const event = factory.next(index % 2 === 0 ? ORDERS : TELEMETRY);
      assert.ok(declared.has(event.type), `undeclared type ${event.type}`);
      assert.equal(typeof event.key, 'string');
    }
  });

  it('keeps order payloads coherent', () => {
    const factory = createEventFactory(seeded(3));
    const created = Array.from({ length: 50 }, () => factory.next(ORDERS)).find(
      (event) => event.type === 'order.created',
    );

    assert.ok(created, 'the seeded run never produced an order.created');
    assert.equal(created.payload.customerId, `cus-${created.payload.orderId.slice(4)}`);
    assert.ok(created.payload.items.length >= 1);
    const expected = created.payload.items.reduce(
      (sum, item) => sum + item.qty * item.unitPrice,
      0,
    );
    assert.equal(created.payload.amount, Math.round(expected * 100) / 100);
  });

  it('keeps telemetry payloads coherent', () => {
    const factory = createEventFactory(seeded(11));

    for (let index = 0; index < 40; index += 1) {
      const event = factory.next(TELEMETRY);
      const { deviceId, warehouse, batteryPct, scannedPerMinute, errorRate } = event.payload;

      assert.equal(warehouse, deviceId.split('-')[1]);
      assert.ok(batteryPct >= 20 && batteryPct < 100);
      if (event.type === 'scanner.degraded') {
        assert.ok(scannedPerMinute < 12, `degraded scanner scanned ${scannedPerMinute}`);
        assert.ok(errorRate >= 0.15, `degraded error rate ${errorRate}`);
      } else {
        assert.ok(scannedPerMinute >= 40, `healthy scanner scanned ${scannedPerMinute}`);
        // `money()` rounds to cents, so a healthy rate can land exactly on the ceiling.
        assert.ok(errorRate <= 0.03, `healthy error rate ${errorRate}`);
      }
    }
  });
});
