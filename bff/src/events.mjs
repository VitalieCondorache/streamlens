/**
 * Synthetic domain traffic.
 *
 * Keys come from a small pool on purpose: the same key landing on the same
 * partition, in order, is the whole point of the Key Router view — a stream of
 * unique keys would hide the partitioning behaviour we want to show.
 */
import { findTopic } from './config.mjs';

const REGIONS = ['eu-central', 'eu-west', 'us-east', 'ap-south'];
const PROVIDERS = ['adyen', 'stripe', 'worldline'];
const CARRIERS = ['dhl', 'dpd', 'gls', 'ups'];
const FAILURE_REASONS = ['insufficient_funds', 'card_expired', 'risk_block', '3ds_timeout'];
const WAREHOUSES = ['berlin-1', 'rotterdam-2', 'cluj-1'];
const PRICES = {
  'sku-8841': 19.9,
  'sku-2210': 149.5,
  'sku-7715': 8.25,
  'sku-3390': 74.0,
  'sku-9912': 32.4,
};

const ORDER_POOL = Array.from({ length: 48 }, (_, i) => `ord-${1000 + i * 7}`);
const DEVICE_POOL = Array.from({ length: 12 }, (_, i) => `scan-${WAREHOUSES[i % 3]}-${i + 1}`);

const money = (value) => Math.round(value * 100) / 100;

export const createEventFactory = (random = Math.random) => {
  const pick = (list) => list[Math.floor(random() * list.length)];

  const pickType = (topic) => {
    const weights = (burst && topic.burstWeights) || topic.weights;
    const entries = Object.entries(weights);
    const total = entries.reduce((sum, [, weight]) => sum + weight, 0);
    let roll = random() * total;
    for (const [type, weight] of entries) {
      roll -= weight;
      if (roll <= 0) return type;
    }
    return entries[0][0];
  };

  let burst = false;

  const orderEvent = (type) => {
    const orderId = pick(ORDER_POOL);
    const amount = money(
      Object.values(PRICES).reduce((sum, price) => sum + price, 0) * (0.4 + random() * 0.9),
    );

    switch (type) {
      case 'order.created': {
        const itemCount = 1 + Math.floor(random() * 3);
        const items = Array.from({ length: itemCount }, () => {
          const sku = pick(Object.keys(PRICES));
          return { sku, qty: 1 + Math.floor(random() * 4), unitPrice: PRICES[sku] };
        });
        return {
          key: orderId,
          payload: {
            orderId,
            customerId: `cus-${orderId.slice(4)}`,
            region: pick(REGIONS),
            items,
            amount: money(items.reduce((sum, item) => sum + item.qty * item.unitPrice, 0)),
            currency: 'EUR',
          },
        };
      }
      case 'payment.authorized':
        return {
          key: orderId,
          payload: {
            orderId,
            amount,
            currency: 'EUR',
            provider: pick(PROVIDERS),
            latencyMs: 40 + Math.floor(random() * 260),
            attempt: 1 + Math.floor(random() * 2),
          },
        };
      case 'payment.failed':
        return {
          key: orderId,
          payload: {
            orderId,
            amount,
            currency: 'EUR',
            provider: pick(PROVIDERS),
            reason: pick(FAILURE_REASONS),
            retryable: random() > 0.4,
          },
        };
      default:
        return {
          key: orderId,
          payload: {
            orderId,
            carrier: pick(CARRIERS),
            trackingNumber: `TRK${Math.floor(random() * 9_999_999)
              .toString()
              .padStart(7, '0')}`,
            estimatedDays: 1 + Math.floor(random() * 5),
          },
        };
    }
  };

  const telemetryEvent = (type) => {
    const deviceId = pick(DEVICE_POOL);
    return {
      key: deviceId,
      payload: {
        deviceId,
        warehouse: deviceId.split('-')[1],
        batteryPct: 20 + Math.floor(random() * 80),
        scannedPerMinute:
          type === 'scanner.degraded' ? Math.floor(random() * 12) : 40 + Math.floor(random() * 160),
        errorRate:
          type === 'scanner.degraded' ? money(0.15 + random() * 0.6) : money(random() * 0.03),
      },
    };
  };

  return {
    /**
     * @returns {{ key: string, type: string, payload: Record<string, unknown> }}
     */
    next(topicName) {
      const topic = findTopic(topicName);
      if (!topic) throw new Error(`unknown topic: ${topicName}`);
      const type = pickType(topic);
      const built = topicName === 'streamlens.telemetry' ? telemetryEvent(type) : orderEvent(type);
      return { type, ...built };
    },
    setBurst(value) {
      burst = value;
    },
    get burst() {
      return burst;
    },
  };
};
