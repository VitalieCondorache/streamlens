import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';

import { DiagnosticsStore } from '../../core/diagnostics-store';
import { murmur2, partitionForKey } from '../../core/kafka/partitioning';
import type { FixtureFile } from '../../core/models/stream.models';
import { StreamStore } from '../../core/stream/stream-store';
import { StreamlensApi } from '../../core/streamlens-api';
import { StatCard } from '../../shared/ui/stat-card/stat-card';

/** Same key pool the generator uses, so a prediction can be checked against live traffic. */
const DEFAULT_KEYS = [
  'ord-1000',
  'ord-1007',
  'ord-1014',
  'ord-1021',
  'ord-1028',
  'ord-1035',
  'ord-1042',
  'ord-1049',
];

export interface KeyRow {
  readonly key: string;
  readonly hash: number;
  readonly partition: number;
  /** Where the broker (or the recording) actually put the record. */
  readonly actual: number | null;
  readonly match: boolean | null;
}

@Component({
  selector: 'app-key-router',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [StatCard],
  templateUrl: './key-router.html',
  styleUrl: './key-router.scss',
})
export class KeyRouter {
  private readonly api = inject(StreamlensApi);
  private readonly diagnostics = inject(DiagnosticsStore);
  protected readonly stream = inject(StreamStore);

  protected readonly input = signal(DEFAULT_KEYS.join(', '));
  protected readonly topic = signal('');
  protected readonly override = signal(0);
  protected readonly verifying = signal(false);
  protected readonly error = signal<string | null>(null);
  protected readonly source = signal<'broker' | 'recording' | null>(null);
  private readonly actuals = signal<ReadonlyMap<string, number>>(new Map());

  protected readonly murmur2 = murmur2;

  protected readonly topics = computed(() => {
    const declared = this.diagnostics.topicMetadata().map((topic) => topic.name);
    return declared.length > 0 ? declared : this.stream.topics();
  });

  protected readonly selectedTopic = computed(() => this.topic() || this.topics()[0] || '');
  protected readonly canAskBroker = computed(() => this.stream.transportKind() === 'sse');

  /** Partition count for the selected topic: broker metadata, then the recording, then what arrived. */
  protected readonly brokerPartitions = computed(() => {
    const metadata = this.diagnostics
      .topicMetadata()
      .find((topic) => topic.name === this.selectedTopic());
    if (metadata) return metadata.partitions.length;

    const recorded = this.stream.ready()?.partitions?.[this.selectedTopic()];
    if (recorded !== undefined) return recorded;

    const observed = this.stream
      .stats()
      .byPartition.filter((entry) => entry.topic === this.selectedTopic()).length;
    return observed > 0 ? observed : 6;
  });

  /** Says out loud where the partition count came from. */
  protected readonly partitionSource = computed(() => {
    if (this.diagnostics.topicMetadata().length > 0) return 'declared by the broker';
    if (this.stream.ready()?.partitions !== undefined) return 'from the recording';
    return 'observed in the stream';
  });

  protected readonly effectivePartitions = computed(
    () => this.override() || this.brokerPartitions(),
  );
  protected readonly overriding = computed(() => this.override() > 0);

  protected readonly keys = computed(() =>
    [
      ...new Set(
        this.input()
          .split(/[\s,;]+/)
          .map((key) => key.trim())
          .filter((key) => key.length > 0),
      ),
    ].slice(0, 24),
  );

  protected readonly rows = computed<readonly KeyRow[]>(() => {
    const partitions = this.effectivePartitions();
    const actuals = this.actuals();
    if (partitions <= 0) return [];

    return this.keys().map((key) => {
      const partition = partitionForKey(key, partitions);
      const actual = actuals.get(key) ?? null;
      return {
        key,
        hash: murmur2(key),
        partition,
        actual,
        match: actual === null ? null : actual === partition,
      };
    });
  });

  protected readonly compared = computed(
    () => this.rows().filter((row) => row.match !== null).length,
  );
  protected readonly matched = computed(
    () => this.rows().filter((row) => row.match === true).length,
  );
  protected readonly mismatched = computed(
    () => this.rows().filter((row) => row.match === false).length,
  );
  protected readonly verdict = computed<'exact' | 'mismatch' | 'pending'>(() => {
    if (this.compared() === 0) return 'pending';
    return this.mismatched() === 0 ? 'exact' : 'mismatch';
  });

  /** Wording follows the source: the broker answers live, the recording answers offline. */
  protected readonly verdictHint = computed(() => {
    const against = this.source() === 'recording' ? 'the recording' : 'the broker';
    if (this.verdict() === 'pending') return 'run a verification to compare';
    if (this.verdict() === 'exact') return `prediction matches ${against}`;
    return `${this.mismatched()} disagreements with ${against}`;
  });

  /* ---------------------------------------------------------------- actions */

  protected updateInput(event: Event): void {
    this.input.set((event.target as HTMLTextAreaElement).value);
  }

  protected setTopic(event: Event): void {
    this.topic.set((event.target as HTMLSelectElement).value);
  }

  protected setOverride(event: Event): void {
    this.override.set(Number((event.target as HTMLSelectElement).value));
  }

  protected shuffle(): void {
    const keys = Array.from(
      { length: 8 },
      (_, index) => `ord-${1000 + index * 7 + Math.floor(Math.random() * 6)}`,
    );
    this.input.set(keys.join(', '));
  }

  /** Asks the broker where these keys really land, then compares with the prediction. */
  protected async verify(): Promise<void> {
    this.error.set(null);

    if (!this.canAskBroker()) {
      await this.verifyAgainstRecording();
      return;
    }

    this.verifying.set(true);
    try {
      const response = await this.api.probePartitions(this.keys(), this.selectedTopic());
      const actuals = new Map<string, number>();
      for (const probe of response.probes) {
        if (probe.partition !== null) actuals.set(probe.key, probe.partition);
      }
      this.actuals.set(actuals);
      this.source.set('broker');
    } catch (error) {
      this.error.set(error instanceof Error ? error.message : 'probe failed');
    } finally {
      this.verifying.set(false);
    }
  }

  /**
   * Without a broker the answer is still not a guess: the recording carries the
   * partitions the real broker assigned, so predictions can be checked against it
   * — just not for keys that never appeared in the recording.
   */
  private async verifyAgainstRecording(): Promise<void> {
    this.verifying.set(true);
    try {
      const response = await fetch('demo-stream.json');
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const fixture = (await response.json()) as FixtureFile;

      const actuals = new Map<string, number>();
      for (const event of fixture.events) {
        if (event.key !== null && !actuals.has(event.key)) {
          actuals.set(event.key, event.partition);
        }
      }
      this.actuals.set(actuals);
      this.source.set('recording');
    } catch (error) {
      this.error.set(error instanceof Error ? error.message : 'could not load the recording');
    } finally {
      this.verifying.set(false);
    }
  }
}
