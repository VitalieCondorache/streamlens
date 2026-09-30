import type { StreamEvent } from '../models/stream.models';

export interface IngestOutcome {
  /** Events that are new for their partition and should be rendered. */
  readonly accepted: readonly StreamEvent[];
  /** Exact re-deliveries (offset already seen) — normal with at-least-once delivery. */
  readonly duplicates: number;
  /** Records that were skipped: `offset > watermark + 1` on the same partition. */
  readonly missed: number;
  /** Events older than the watermark, i.e. delivered after a newer one. */
  readonly late: number;
}

export interface Watermark {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
}

const keyOf = (topic: string, partition: number) => `${topic}#${partition}`;

/**
 * Per-partition watermarks, the client half of "at-least-once delivery".
 *
 * Kafka guarantees the order *inside* a partition and delivers at least once, so
 * a reconnect (or a rebalance after we rewound a group) can hand us records we
 * have already rendered. Tracking the highest offset per partition — and counting
 * the gaps instead of hiding them — is what makes the live tail trustworthy.
 */
export class PartitionWatermarks {
  private readonly marks = new Map<string, { topic: string; partition: number; offset: bigint }>();

  accept(events: readonly StreamEvent[]): IngestOutcome {
    const accepted: StreamEvent[] = [];
    let duplicates = 0;
    let missed = 0;
    let late = 0;

    for (const event of events) {
      const key = keyOf(event.topic, event.partition);
      const offset = BigInt(event.offset);
      const current = this.marks.get(key);

      if (current === undefined) {
        this.marks.set(key, { topic: event.topic, partition: event.partition, offset });
        accepted.push(event);
        continue;
      }

      if (offset <= current.offset) {
        if (offset === current.offset) duplicates += 1;
        else late += 1;
        continue;
      }

      // A jump forwards means records we never saw: retention, a paused consumer
      // on the broker side, or a topic that was recreated.
      if (offset > current.offset + 1n) {
        missed += Number(offset - current.offset - 1n);
      }

      current.offset = offset;
      accepted.push(event);
    }

    return { accepted, duplicates, missed, late };
  }

  /** Resume token: exactly what the BFF consumer group has committed so far. */
  snapshot(): readonly Watermark[] {
    return [...this.marks.values()]
      .map((mark) => ({
        topic: mark.topic,
        partition: mark.partition,
        offset: mark.offset.toString(),
      }))
      .sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);
  }

  reset(): void {
    this.marks.clear();
  }
}
