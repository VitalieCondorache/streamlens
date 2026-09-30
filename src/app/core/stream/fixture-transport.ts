import type { FixtureEvent, FixtureFile, StreamEvent } from '../models/stream.models';
import type { StreamOptions, StreamSink, StreamTransport } from './stream-transport';

export interface FixtureTransportConfig {
  /** Static asset recorded by `npm run capture` in the BFF. */
  readonly url?: string;
  /** 1 = the original pace, 4 = four times faster. */
  readonly speed?: number;
  readonly loopPauseMs?: number;
  readonly minGapMs?: number;
  /** Pre-loaded fixture, so tests never touch the network. */
  readonly fixture?: FixtureFile;
}

/**
 * Replays a session recorded from a real broker.
 *
 * This is what makes the deployed demo honest: the same components, the same
 * event shapes and the same ordering guarantees as the live mode — only the
 * transport differs. Timestamps are re-based to "now" so the latency panel keeps
 * showing a plausible pipeline delay instead of days-old numbers.
 */
export class FixtureStreamTransport implements StreamTransport {
  readonly kind = 'fixture' as const;

  private readonly config: Required<Omit<FixtureTransportConfig, 'fixture'>> & {
    fixture?: FixtureFile;
  };

  private timer: ReturnType<typeof setTimeout> | null = null;
  private sink: StreamSink | null = null;
  private fixture: FixtureFile | null = null;
  private active = false;
  private pass = 0;

  constructor(config: FixtureTransportConfig = {}) {
    this.config = {
      url: config.url ?? 'demo-stream.json',
      speed: config.speed ?? 1,
      loopPauseMs: config.loopPauseMs ?? 1_500,
      minGapMs: config.minGapMs ?? 8,
      fixture: config.fixture,
    };
  }

  async start(options: StreamOptions, sink: StreamSink): Promise<void> {
    this.stop();

    this.sink = sink;
    this.active = true;
    this.pass = 0;
    sink.state('connecting', 'BFF unavailable, loading the recorded session');

    try {
      this.fixture = this.config.fixture ?? (await this.load());
    } catch (error) {
      this.sink?.state('offline', `No recorded session at ${this.config.url}: ${describe(error)}`);
      return;
    }

    if (!this.active) return;
    this.sink?.notice({
      level: 'info',
      code: 'demo-mode',
      message: `Replaying ${this.fixture.source.count} records captured from ${this.fixture.source.brokers.join(', ')}.`,
    });
    this.startPass();
  }

  stop(): void {
    this.active = false;
    if (this.timer !== null) clearTimeout(this.timer);
    this.timer = null;
    this.sink = null;
  }

  get currentPass(): number {
    return this.pass;
  }

  private async load(): Promise<FixtureFile> {
    const response = await fetch(this.config.url);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return (await response.json()) as FixtureFile;
  }

  private startPass(): void {
    const fixture = this.fixture;
    const sink = this.sink;
    if (!fixture || !sink || !this.active) return;

    this.pass += 1;

    // A new session id per pass is deliberate: the store resets its watermarks on
    // a fresh session, otherwise the second pass would look like 100% duplicates.
    sink.ready({
      sessionId: `demo-${this.pass}`,
      groupId: 'replay:recorded-session',
      topics: fixture.source.topics,
      fromBeginning: false,
      flushMs: 120,
      kafka: fixture.source.brokers,
      partitions: fixture.source.partitions,
    });
    sink.state(
      'demo',
      `Recorded ${new Date(fixture.capturedAt).toISOString()} · pass ${this.pass}`,
    );

    if (this.pass > 1) {
      sink.notice({
        level: 'info',
        code: 'replay-restart',
        message: `Replay pass ${this.pass} — same records, same partitions, same order.`,
      });
    }

    this.scheduleNext(0);
  }

  private scheduleNext(index: number): void {
    const fixture = this.fixture;
    const sink = this.sink;
    if (!fixture || !sink || !this.active) return;

    if (index >= fixture.events.length) {
      this.timer = setTimeout(() => this.startPass(), this.config.loopPauseMs);
      return;
    }

    const event = fixture.events[index];
    const delay = Math.max(this.config.minGapMs, Math.round(event.deltaMs / this.config.speed));

    this.timer = setTimeout(() => {
      if (!this.active || !this.sink) return;
      this.sink.events([this.toStreamEvent(event)]);
      this.scheduleNext(index + 1);
    }, delay);
  }

  private toStreamEvent(event: FixtureEvent): StreamEvent {
    const at = Date.now();
    return {
      id: event.id,
      topic: event.topic,
      partition: event.partition,
      offset: event.offset,
      key: event.key,
      type: event.type,
      at,
      payload: event.payload,
      // Synthetic pipeline delay so the latency widget keeps its meaning offline.
      ingestedAt: at + 18 + Math.floor(Math.random() * 70),
    };
  }
}

const describe = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);
