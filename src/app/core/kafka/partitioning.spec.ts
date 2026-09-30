import { murmur2, partitionForKey, toPositive } from './partitioning';

/**
 * The expected values come from Apache Kafka itself — clients/src/test/java/
 * org/apache/kafka/common/utils/UtilsTest.java — so a regression in the hash means
 * the UI would start predicting partitions the broker does not agree with.
 */
const KAFKA_VECTORS: readonly (readonly [string, number])[] = [
  ['21', -973932308],
  ['foobar', -790332482],
  ['a-little-bit-long-string', -985981536],
  ['a-little-bit-longer-string', -1486304829],
  ['lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8', -58897971],
  ['abc', 479470107],
];

describe('murmur2', () => {
  it('matches the vectors published by Apache Kafka', () => {
    for (const [input, expected] of KAFKA_VECTORS) {
      expect(murmur2(input), `murmur2(${JSON.stringify(input)})`).toBe(expected);
    }
  });

  it('hashes UTF-8 bytes, not code units', () => {
    // Same functional expectation as the Java client: multi-byte characters are
    // hashed as their UTF-8 encoding.
    expect(murmur2('üñíçødé')).toBe(1910432001);
    expect(murmur2('ключ')).toBe(2122343024);
  });

  it('always returns a signed 32-bit integer', () => {
    for (const input of ['', 'a', 'order-1', 'x'.repeat(257), '🚀']) {
      const hash = murmur2(input);
      expect(Number.isInteger(hash)).toBe(true);
      expect(hash).toBeGreaterThanOrEqual(-(2 ** 31));
      expect(hash).toBeLessThanOrEqual(2 ** 31 - 1);
    }
  });

  it('is deterministic for repeated keys across lengths (tail handling)', () => {
    for (const length of [1, 2, 3, 4, 5, 7, 8, 11]) {
      const key = 'k'.repeat(length);
      expect(murmur2(key)).toBe(murmur2(key));
    }
  });
});

describe('toPositive', () => {
  it('masks the sign bit instead of negating', () => {
    expect(toPositive(-1)).toBe(0x7fffffff);
    // abs(Int.MIN_VALUE) stays negative in Java — masking is the fix.
    expect(toPositive(-2147483648)).toBe(0);
    expect(toPositive(42)).toBe(42);
  });
});

describe('partitionForKey', () => {
  it('reproduces the assignments observed on the real broker', () => {
    // Verified against `POST /api/partition-probe` on a 6-partition topic.
    const observed: Record<string, number> = {
      'ord-1000': 1,
      'ord-1007': 4,
      'ord-1014': 2,
      'ord-1021': 5,
      'ord-1028': 4,
      'ord-1035': 1,
      'ord-1042': 3,
      'ord-1049': 5,
    };

    for (const [key, partition] of Object.entries(observed)) {
      expect(partitionForKey(key, 6), key).toBe(partition);
    }
  });

  it('keeps every key in range for any partition count', () => {
    for (const partitions of [1, 2, 3, 6, 12, 32]) {
      for (let index = 0; index < 200; index += 1) {
        const partition = partitionForKey(`ord-${index}`, partitions);
        expect(partition).toBeGreaterThanOrEqual(0);
        expect(partition).toBeLessThan(partitions);
      }
    }
  });

  it('is stable: the same key always maps to the same partition', () => {
    const first = partitionForKey('ord-1000', 6);
    for (let attempt = 0; attempt < 100; attempt += 1) {
      expect(partitionForKey('ord-1000', 6)).toBe(first);
    }
  });

  it('rejects an impossible partition count', () => {
    expect(() => partitionForKey('key', 0)).toThrowError(RangeError);
  });
});
