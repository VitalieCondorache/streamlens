import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { RouterLink } from '@angular/router';

import { DiagnosticsStore } from '../../core/diagnostics-store';
import { StreamStore } from '../../core/stream/stream-store';
import { Sparkline } from '../../shared/ui/sparkline/sparkline';
import { StatCard } from '../../shared/ui/stat-card/stat-card';
import { compact, duration, integer, percent, timeOfDay } from '../../shared/format';

/** Failure share above which the dashboard raises its hand. */
const FAILURE_ALERT = 0.12;

@Component({
  selector: 'app-overview',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, Sparkline, StatCard],
  templateUrl: './overview.html',
  styleUrl: './overview.scss',
})
export class Overview {
  protected readonly stream = inject(StreamStore);
  protected readonly diagnostics = inject(DiagnosticsStore);

  protected readonly rates = signal('25');
  protected readonly rateOptions = ['10', '25', '60', '200'];
  protected readonly showAllNotices = signal(false);

  protected readonly stats = this.stream.stats;
  protected readonly counters = this.stream.counters;

  protected readonly alerting = computed(() => this.stats().failureRate >= FAILURE_ALERT);
  protected readonly alertText = computed(
    () =>
      `${percent(this.stats().failureRate)} of the buffered records are failures (${this.stats().failures} of ${this.stats().buffered}).`,
  );

  protected readonly visibleNotices = computed(() =>
    this.showAllNotices() ? this.stream.notices() : this.stream.notices().slice(0, 4),
  );

  protected readonly integer = integer;
  protected readonly compact = compact;
  protected readonly percent = percent;
  protected readonly duration = duration;
  protected readonly timeOfDay = timeOfDay;

  protected readonly partitionsInUse = computed(() => this.stats().byPartition.length);
  protected readonly sessionCounters = computed(
    () => this.diagnostics.streamStats()?.sessions.length ?? 0,
  );
  /** In replay mode the BFF is not there at all, so those panels have nothing to show. */
  protected readonly bffReachable = computed(() => this.diagnostics.health.value() !== undefined);

  protected readonly generatorRunning = computed(
    () => this.diagnostics.simulator()?.running ?? false,
  );
  protected readonly burst = computed(() => this.diagnostics.simulator()?.burst ?? false);

  protected async toggleGenerator(): Promise<void> {
    await this.diagnostics.setSimulator(this.generatorRunning() ? 'stop' : 'start');
  }

  protected async applyRate(rate: string): Promise<void> {
    this.rates.set(rate);
    await this.diagnostics.setSimulator('start', Number(rate));
  }

  protected topicShare(topic: string): number {
    const total = this.stats().buffered;
    if (total === 0) return 0;
    const entry = this.stats().byTopic.find((item) => item.topic === topic);
    return (entry?.count ?? 0) / total;
  }

  protected topicCount(topic: string): number {
    return this.stats().byTopic.find((entry) => entry.topic === topic)?.count ?? 0;
  }

  /** `{ 'topic': [0, 3] }` -> `topic: [0, 3]` — the assignment the group leader reported. */
  protected assignmentLabel(assignment: Readonly<Record<string, readonly number[]>>): string {
    return Object.entries(assignment)
      .map(([topic, partitions]) => `${topic}: [${partitions.join(', ')}]`)
      .join(' · ');
  }
}
