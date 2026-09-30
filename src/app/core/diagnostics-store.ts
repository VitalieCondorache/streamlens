import { computed, DestroyRef, inject, Injectable, signal } from '@angular/core';
import { httpResource } from '@angular/common/http';
import { StreamlensApi } from './streamlens-api';
import type {
  HealthResponse,
  LagResponse,
  SimulatorState,
  TopicsResponse,
} from './models/stream.models';

/**
 * Everything that is *read* from the BFF is polled through `httpResource`, which
 * gives loading/error/value signals for free and refetches whenever the request
 * params signal changes — here, the tick counter.
 */
@Injectable({ providedIn: 'root' })
export class DiagnosticsStore {
  private readonly api = inject(StreamlensApi);
  private readonly destroyRef = inject(DestroyRef);

  /** Bumping this invalidates the requests below. */
  private readonly tick = signal(0);
  private timer: ReturnType<typeof setInterval> | null = null;

  readonly pollMs = 3_000;
  readonly health = httpResource<HealthResponse>(() => ({
    url: '/api/health',
    params: { tick: this.tick() },
  }));
  readonly topics = httpResource<TopicsResponse>(() => ({
    url: '/api/topics',
    params: { tick: this.tick() },
  }));
  readonly lag = httpResource<LagResponse>(() => ({
    url: '/api/lag',
    params: { tick: this.tick() },
  }));

  /** Optimistic copy of the generator state so the buttons feel instant. */
  private readonly simulatorOverride = signal<SimulatorState | null>(null);
  readonly busy = signal(false);
  readonly actionError = signal<string | null>(null);

  readonly available = computed(
    () => this.health.hasValue() && this.health.value().kafka.connected,
  );
  readonly simulator = computed(
    () => this.simulatorOverride() ?? this.health.value()?.simulator ?? null,
  );
  readonly broker = computed(() => this.health.value()?.kafka ?? null);
  readonly streamStats = computed(() => this.health.value()?.stream ?? null);
  readonly sessionGroups = computed(() => this.health.value()?.stream.sessions ?? []);
  readonly lagRows = computed(() => this.lag.value()?.partitions ?? []);
  readonly totalLag = computed(() => this.lag.value()?.totalLag ?? '0');
  readonly topicMetadata = computed(() => this.topics.value()?.topics ?? []);

  constructor() {
    this.destroyRef.onDestroy(() => this.stopPolling());
  }

  startPolling(): void {
    if (this.timer !== null) return;
    this.timer = setInterval(() => this.refresh(), this.pollMs);
  }

  stopPolling(): void {
    if (this.timer === null) return;
    clearInterval(this.timer);
    this.timer = null;
  }

  refresh(): void {
    this.tick.update((value) => value + 1);
  }

  async setSimulator(action: 'start' | 'stop', ratePerSecond?: number): Promise<void> {
    this.busy.set(true);
    this.actionError.set(null);
    try {
      this.simulatorOverride.set(await this.api.simulator(action, ratePerSecond));
      this.refresh();
    } catch (error) {
      this.actionError.set(error instanceof Error ? error.message : 'simulator call failed');
    } finally {
      this.busy.set(false);
    }
  }
}
