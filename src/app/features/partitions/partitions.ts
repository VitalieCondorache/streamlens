import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';

import { DiagnosticsStore } from '../../core/diagnostics-store';
import { StreamStore } from '../../core/stream/stream-store';
import type { PartitionCount } from '../../core/stream/stream-stats';
import { StatCard } from '../../shared/ui/stat-card/stat-card';
import { compact, integer, percent } from '../../shared/format';

export interface PartitionRow {
  readonly partition: number;
  readonly buffered: number;
  /** Share of every buffered record, across all topics. */
  readonly share: number;
  /** Relative to the busiest partition of the same topic — drives the bar width. */
  readonly peakShare: number;
  readonly oldestOffset: string | null;
  readonly nextOffset: string | null;
  readonly committed: string | null;
  readonly lag: string | null;
}

/**
 * `byPartition` from the buffer shows what this consumer received; the broker
 * metadata shows where the partition currently ends; the lag row shows where the
 * audit group has committed. Three independent views of the same partition.
 */
@Component({
  selector: 'app-partitions',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [StatCard],
  templateUrl: './partitions.html',
  styleUrl: './partitions.scss',
})
export class Partitions {
  protected readonly stream = inject(StreamStore);
  protected readonly diagnostics = inject(DiagnosticsStore);

  protected readonly stats = this.stream.stats;
  protected readonly integer = integer;
  protected readonly compact = compact;
  protected readonly percent = percent;

  protected readonly activePartitions = computed(() => this.stats().byPartition.length);
  protected readonly hottestPartition = computed<PartitionCount | null>(() =>
    this.stats().byPartition.reduce<PartitionCount | null>(
      (busiest, entry) => (busiest === null || entry.count > busiest.count ? entry : busiest),
      null,
    ),
  );
  protected readonly groupId = computed(() => this.diagnostics.lag.value()?.groupId ?? '—');
  protected readonly bffReachable = computed(() => this.diagnostics.health.value() !== undefined);

  /** Topics declared by the broker, then the recording, then what the buffer has seen. */
  protected readonly topics = computed(() => {
    const declared = this.diagnostics.topicMetadata().map((topic) => topic.name);
    if (declared.length > 0) return declared;

    const recorded = Object.keys(this.stream.ready()?.partitions ?? {});
    return recorded.length > 0 ? recorded : this.stream.topics();
  });

  protected partitionRows(topic: string): readonly PartitionRow[] {
    const observed = new Map(
      this.stats()
        .byPartition.filter((entry) => entry.topic === topic)
        .map((entry) => [entry.partition, entry.count]),
    );
    const metadata = this.diagnostics.topicMetadata().find((entry) => entry.name === topic);
    const offsets = new Map(metadata?.partitions.map((entry) => [entry.partition, entry]) ?? []);
    const lags = new Map(
      this.diagnostics
        .lagRows()
        .filter((entry) => entry.topic === topic)
        .map((entry) => [entry.partition, entry]),
    );

    // Prefer the declared topology so an idle partition still shows up in the table.
    const declaredCount =
      metadata?.partitions.length ?? this.stream.ready()?.partitions?.[topic] ?? 0;
    const declared = Array.from({ length: declaredCount }, (_, partition) => partition);
    const partitions = [
      ...new Set([...declared, ...offsets.keys(), ...observed.keys(), ...lags.keys()]),
    ].sort((a, b) => a - b);

    const peak = Math.max(1, ...observed.values());
    const total = Math.max(1, this.stats().buffered);

    return partitions.map((partition) => {
      const lag = lags.get(partition);
      const offset = offsets.get(partition);
      const buffered = observed.get(partition) ?? 0;

      return {
        partition,
        buffered,
        share: buffered / total,
        peakShare: buffered / peak,
        oldestOffset: offset?.oldestOffset ?? null,
        nextOffset: lag?.nextOffset ?? offset?.nextOffset ?? null,
        committed: lag?.committedOffset ?? null,
        lag: lag?.lag ?? null,
      };
    });
  }

  protected topicCount(topic: string): number {
    return this.stats().byTopic.find((entry) => entry.topic === topic)?.count ?? 0;
  }
}
