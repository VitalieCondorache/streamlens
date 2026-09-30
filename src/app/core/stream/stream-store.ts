import { computed, DestroyRef, inject, Injectable, signal } from '@angular/core';
import type {
  ConnectionMode,
  StreamEvent,
  StreamNotice,
  StreamReady,
} from '../models/stream.models';
import { PartitionWatermarks, type Watermark } from './partition-watermarks';
import type { StreamOptions, StreamSink, StreamTransport } from './stream-transport';
import {
  buildStreamStats,
  filterEvents,
  type StreamFilters,
  type StreamStats,
} from './stream-stats';
import { readSessionId, rotateSessionId } from '../session';
import { StreamlensApi } from '../streamlens-api';

export interface StreamCounters {
  readonly batches: number;
  readonly accepted: number;
  /** Exact re-deliveries — expected with at-least-once delivery. */
  readonly duplicates: number;
  /** Records that were never delivered to this client. */
  readonly missed: number;
  readonly late: number;
  readonly droppedWhilePaused: number;
}

export interface NoticeEntry extends StreamNotice {
  readonly at: number;
}

export interface RateSample {
  readonly t: number;
  readonly rate: number;
}

const INITIAL_COUNTERS: StreamCounters = {
  batches: 0,
  accepted: 0,
  duplicates: 0,
  missed: 0,
  late: 0,
  droppedWhilePaused: 0,
};

const INITIAL_FILTERS: StreamFilters = { topic: 'all', type: 'all', query: '' };

/**
 * Single source of truth for the live stream.
 *
 * Deliberately framework-light: signals in, signals out, no observable plumbing
 * and no state library. Everything that needs to be *reasoned about* (dedup,
 * aggregation) lives in pure functions that are unit tested on their own.
 */
@Injectable({ providedIn: 'root' })
export class StreamStore {
  private readonly api = inject(StreamlensApi);
  private readonly destroyRef = inject(DestroyRef);

  /** Rows kept in memory. Independent of what is rendered: the list is virtualised. */
  readonly bufferLimit = 1_500;
  private readonly rateWindow = 90;

  private readonly watermarks = new PartitionWatermarks();
  private transport: StreamTransport | null = null;
  private options: StreamOptions | null = null;
  private sampling: ReturnType<typeof setInterval> | null = null;
  private acceptedSinceSample = 0;

  readonly sessionId = signal(readSessionId());
  readonly mode = signal<ConnectionMode>('offline');
  readonly modeDetail = signal<string>('Not connected');
  readonly ready = signal<StreamReady | null>(null);
  readonly transportKind = signal<'sse' | 'fixture' | null>(null);
  readonly replaying = signal(false);

  /** Newest first: index 0 is the record that just arrived. */
  readonly events = signal<readonly StreamEvent[]>([]);
  /** The most recent accepted batch — the live tail uses it to keep its scroll anchor. */
  readonly lastBatch = signal<readonly StreamEvent[]>([]);
  readonly counters = signal<StreamCounters>(INITIAL_COUNTERS);
  readonly notices = signal<readonly NoticeEntry[]>([]);
  readonly positions = signal<readonly Watermark[]>([]);
  readonly rates = signal<readonly RateSample[]>([]);
  readonly filters = signal<StreamFilters>(INITIAL_FILTERS);
  readonly paused = signal(false);

  readonly filtered = computed(() => filterEvents(this.events(), this.filters()));
  readonly stats = computed<StreamStats>(() => buildStreamStats(this.events()));
  readonly types = computed(() =>
    [...new Set(this.events().map((event) => event.type))].sort((a, b) => a.localeCompare(b)),
  );
  readonly topics = computed(() =>
    [...new Set(this.events().map((event) => event.topic))].sort((a, b) => a.localeCompare(b)),
  );
  /** Records per second over the last sample window, oldest first. */
  readonly rate = computed(() => this.rates().map((sample) => sample.rate));
  readonly latestRate = computed(() => this.rates().at(-1)?.rate ?? 0);
  readonly bufferFull = computed(() => this.events().length >= this.bufferLimit);

  private readonly sink: StreamSink = {
    ready: (ready) => this.onReady(ready),
    events: (events) => this.onEvents(events),
    notice: (notice) => this.onNotice(notice),
    state: (mode, detail) => this.onState(mode, detail),
  };

  constructor() {
    this.destroyRef.onDestroy(() => this.disconnect());
  }

  /* ------------------------------------------------------------- lifecycle */

  /**
   * Binds a transport. The stream service decides *which* transport (live SSE or
   * the recorded fixture); the store only cares that it speaks the same protocol.
   */
  attach(transport: StreamTransport, options: Partial<StreamOptions> = {}): void {
    const wasConnected = this.options !== null;
    this.transport?.stop();
    this.transport = transport;
    this.transportKind.set(transport.kind);
    this.options = {
      sessionId: this.sessionId(),
      topics: options.topics ?? [],
      fromBeginning: options.fromBeginning ?? false,
    };
    if (wasConnected) this.connect();
  }

  connect(overrides: Partial<StreamOptions> = {}): void {
    if (!this.transport) return;

    this.options = {
      sessionId: this.sessionId(),
      topics: overrides.topics ?? this.options?.topics ?? [],
      fromBeginning: overrides.fromBeginning ?? this.options?.fromBeginning ?? false,
    };

    this.startSampling();
    this.transport.start(this.options, this.sink);
  }

  disconnect(): void {
    this.transport?.stop();
    this.stopSampling();
    this.replaying.set(false);
    this.onState('offline', 'Disconnected');
  }

  /**
   * Rewinds this session's consumer group and restarts the stream.
   *
   * Buffer and watermarks are cleared on purpose: the records that come back are
   * ones we have already seen, so keeping the watermarks would classify the whole
   * replay as duplicates and render nothing.
   */
  async rewind(count: number, topic: string): Promise<void> {
    this.transport?.stop();
    this.mode.set('connecting');
    this.modeDetail.set(`Rewriting committed offsets for ${topic}…`);

    try {
      const result = await this.api.replay(this.sessionId(), count, topic);
      this.watermarks.reset();
      this.events.set([]);
      this.positions.set([]);
      this.counters.set(INITIAL_COUNTERS);
      this.rates.set([]);
      this.replaying.set(true);
      this.onNotice({
        level: 'info',
        code: 'rewound',
        message: `Group ${result.groupId} moved back ~${result.perPartition} records per partition.`,
      });
    } catch (error) {
      this.onNotice({ level: 'error', code: 'rewind-failed', message: describe(error) });
    }

    this.connect();
  }

  /** New consumer group: `earliest` replays the whole retention window. */
  startOver(options: { fromBeginning?: boolean } = {}): void {
    const next = rotateSessionId();
    this.transport?.stop();
    this.replaying.set(false);
    this.watermarks.reset();
    this.events.set([]);
    this.positions.set([]);
    this.counters.set(INITIAL_COUNTERS);
    this.rates.set([]);
    this.notices.set([]);
    this.sessionId.set(next);
    this.onNotice({
      level: 'info',
      code: 'session-rotated',
      message: `New consumer group streamlens-ui-${next}`,
    });
    this.connect({ fromBeginning: options.fromBeginning ?? false });
  }

  /* --------------------------------------------------------------- actions */

  togglePause(): void {
    this.paused.update((paused) => !paused);
  }

  /** Clears the view only: watermarks keep deduplicating, counters keep counting. */
  clearBuffer(): void {
    this.events.set([]);
  }

  /** Lets the stream service report its transport decision in the notice feed. */
  pushNotice(notice: StreamNotice): void {
    this.onNotice(notice);
  }

  setMode(mode: ConnectionMode, detail?: string): void {
    this.onState(mode, detail);
  }

  setFilters(patch: Partial<StreamFilters>): void {
    this.filters.update((current) => ({ ...current, ...patch }));
  }

  resetFilters(): void {
    this.filters.set(INITIAL_FILTERS);
  }

  /* ------------------------------------------------------------------ sink */

  private onReady(ready: StreamReady): void {
    // A different session id means a new stream generation (a new replay pass); a
    // repeated one means the BFF resumed the very same group after a reconnect.
    if (ready.sessionId !== this.generation) {
      this.generation = ready.sessionId;
      this.watermarks.reset();
      this.events.set([]);
      this.positions.set([]);
    }
    this.ready.set(ready);
  }

  private onEvents(batch: readonly StreamEvent[]): void {
    if (this.paused()) {
      this.counters.update((counters) => ({
        ...counters,
        droppedWhilePaused: counters.droppedWhilePaused + batch.length,
      }));
      return;
    }

    const outcome = this.watermarks.accept(batch);
    this.counters.update((counters) => ({
      batches: counters.batches + 1,
      accepted: counters.accepted + outcome.accepted.length,
      duplicates: counters.duplicates + outcome.duplicates,
      missed: counters.missed + outcome.missed,
      late: counters.late + outcome.late,
      droppedWhilePaused: counters.droppedWhilePaused,
    }));

    if (outcome.accepted.length === 0) return;

    this.acceptedSinceSample += outcome.accepted.length;
    this.lastBatch.set(outcome.accepted);
    this.events.update((current) => {
      const merged = [...[...outcome.accepted].reverse(), ...current];
      return merged.length > this.bufferLimit ? merged.slice(0, this.bufferLimit) : merged;
    });
    this.positions.set(this.watermarks.snapshot());
  }

  private onNotice(notice: StreamNotice): void {
    this.notices.update((current) => [{ ...notice, at: Date.now() }, ...current].slice(0, 40));
  }

  private onState(mode: ConnectionMode, detail?: string): void {
    this.mode.set(mode);
    if (detail !== undefined) this.modeDetail.set(detail);
  }

  /* -------------------------------------------------------------- sampling */

  private startSampling(): void {
    if (this.sampling !== null) return;

    this.sampling = setInterval(() => {
      const rate = this.acceptedSinceSample;
      this.acceptedSinceSample = 0;
      this.rates.update((samples) => [...samples, { t: Date.now(), rate }].slice(-this.rateWindow));
    }, 1_000);
  }

  private stopSampling(): void {
    if (this.sampling === null) return;
    clearInterval(this.sampling);
    this.sampling = null;
  }

  /** Last `sessionId` reported by the transport — identifies a stream generation. */
  private generation = '';
}

const describe = (error: unknown): string =>
  error instanceof Error ? error.message : typeof error === 'string' ? error : 'unknown error';
