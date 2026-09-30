/**
 * Kafka's key → partition mapping, implemented the way the JVM client does it.
 *
 * A producer that uses the default partitioner computes:
 *
 *   Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions
 *
 * `murmur2` is MurmurHash2 with seed 0x9747b28c operating on plain 32-bit
 * overflow arithmetic, and `toPositive` masks the sign bit off instead of taking
 * the absolute value (`abs(Int.MIN_VALUE)` is still negative — a classic bug).
 *
 * Reproducing it in the browser is what lets the UI predict the partition a key
 * will land on. The prediction is verified against the broker itself through
 * `POST /api/partition-probe`.
 */

const SEED = 0x9747b28c;
const M = 0x5bd1e995;
const R = 24;

/** MurmurHash2, matching org.apache.kafka.common.utils.Utils#murmur2. */
export function murmur2(key: string): number {
  const bytes = utf8Bytes(key);
  const length = bytes.length;

  let h = (SEED ^ length) >>> 0;
  const length4 = length >> 2;

  for (let index = 0; index < length4; index += 1) {
    const base = index * 4;
    let k =
      (bytes[base] & 0xff) |
      ((bytes[base + 1] & 0xff) << 8) |
      ((bytes[base + 2] & 0xff) << 16) |
      ((bytes[base + 3] & 0xff) << 24);

    k = Math.imul(k, M) >>> 0;
    k = (k ^ (k >>> R)) >>> 0;
    k = Math.imul(k, M) >>> 0;

    h = Math.imul(h, M) >>> 0;
    h = (h ^ k) >>> 0;
  }

  const tail = length4 * 4;
  const remainder = length % 4;
  // Mirrors the JVM implementation, which relies on switch fallthrough.
  if (remainder === 3) {
    h = (h ^ ((bytes[tail + 2] & 0xff) << 16)) >>> 0;
  }
  if (remainder >= 2) {
    h = (h ^ ((bytes[tail + 1] & 0xff) << 8)) >>> 0;
  }
  if (remainder >= 1) {
    h = (h ^ (bytes[tail] & 0xff)) >>> 0;
    h = Math.imul(h, M) >>> 0;
  }

  h = (h ^ (h >>> 13)) >>> 0;
  h = Math.imul(h, M) >>> 0;
  h = (h ^ (h >>> 15)) >>> 0;

  // Back to a signed 32-bit value: that is what the Java client hashes with.
  return h | 0;
}

/** `Utils.toPositive(x) = x & 0x7fffffff`. */
export function toPositive(hash: number): number {
  return hash & 0x7fffffff;
}

/** The broker's default partitioner, for a key and a topic partition count. */
export function partitionForKey(key: string, numPartitions: number): number {
  if (numPartitions <= 0) throw new RangeError('numPartitions must be >= 1');
  return toPositive(murmur2(key)) % numPartitions;
}

/** UTF-8 encoding without TextEncoder, so the hash works in any JS runtime. */
function utf8Bytes(value: string): Uint8Array {
  const encoded = unescape(encodeURIComponent(value));
  const bytes = new Uint8Array(encoded.length);
  for (let index = 0; index < encoded.length; index += 1) {
    bytes[index] = encoded.charCodeAt(index) & 0xff;
  }
  return bytes;
}
