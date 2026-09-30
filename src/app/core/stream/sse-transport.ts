import type {
  ConnectionMode,
  StreamEvent,
  StreamNotice,
  StreamReady,
} from '../models/stream.models';
import { createFrameBatcher, type FrameBatcher } from './frame-batcher';
import type { StreamOptions, StreamSink, StreamTransport } from './stream-transport';

export interface SseTransportConfig {
  /** Empty string means same-origin — in dev the Angular proxy forwards /api to the BFF. */
  readonly baseUrl?: string;
  /**
   * EventSource reconnects on its own, forever. After this many consecutive
   * failures we stop pretending it is recoverable and report `offline`, which is
   * the signal the stream service uses to fall back to the recorded fixture.
   */
  readonly offlineAfterErrors?: number;
}

/**
 * Talks to the BFF over Server-Sent Events.
 *
 * SSE rather than WebSocket on purpose: the flow is strictly server → browser, it
 * survives proxies, and `EventSource` reconnects natively. Every frame carries
 * `id: topic:partition:offset`, which the store turns into per-partition
 * watermarks so a reconnect cannot duplicate rows.
 */
export class SseStreamTransport implements StreamTransport {
  readonly kind = 'sse' as const;

  private source: EventSource | null = null;
  private sink: StreamSink | null = null;
  private batcher: FrameBatcher<StreamEvent> | null = null;
  private consecutiveErrors = 0;
  private active = false;

  private readonly baseUrl: string;
  private readonly offlineAfterErrors: number;

  constructor(config: SseTransportConfig = {}) {
    this.baseUrl = config.baseUrl ?? '';
    this.offlineAfterErrors = config.offlineAfterErrors ?? 6;
  }

  start(options: StreamOptions, sink: StreamSink): void {
    this.stop();

    this.sink = sink;
    this.active = true;
    this.consecutiveErrors = 0;

    // Coalesce the frames the BFF pushes: one signal write per animation frame.
    this.batcher = createFrameBatcher<StreamEvent>((events) => sink.events(events));

    const query = new URLSearchParams({
      session: options.sessionId,
      topics: options.topics.join(','),
      from: options.fromBeginning ? 'earliest' : 'latest',
    });

    sink.state('connecting');
    const source = new EventSource(`${this.baseUrl}/api/stream?${query.toString()}`);
    this.source = source;

    source.addEventListener('open', () => {
      if (!this.active) return;
      this.consecutiveErrors = 0;
    });

    source.addEventListener('stream-ready', (message) => {
      if (!this.active) return;
      this.consecutiveErrors = 0;
      this.sink?.ready(parse<StreamReady>(message) as StreamReady);
      this.sink?.state('live');
    });

    source.addEventListener('stream-event', (message) => {
      if (!this.active) return;
      const event = parse<StreamEvent>(message);
      if (event) this.batcher?.push([event]);
    });

    source.addEventListener('stream-notice', (message) => {
      if (!this.active) return;
      const notice = parse<StreamNotice>(message);
      if (notice) this.sink?.notice(notice);
    });

    source.addEventListener('error', () => {
      if (!this.active) return;
      this.consecutiveErrors += 1;

      if (this.consecutiveErrors >= this.offlineAfterErrors) {
        this.sink?.state('offline', `No SSE frames after ${this.consecutiveErrors} attempts`);
        return;
      }

      const mode: ConnectionMode =
        source.readyState === EventSource.CONNECTING ? 'reconnecting' : 'offline';
      this.sink?.state(mode, 'EventSource will retry');
    });
  }

  stop(): void {
    this.active = false;
    this.consecutiveErrors = 0;
    this.batcher?.flushNow();
    this.batcher?.dispose();
    this.batcher = null;
    this.source?.close();
    this.source = null;
    this.sink = null;
  }
}

const parse = <T>(message: MessageEvent<string>): T | null => {
  try {
    return JSON.parse(message.data) as T;
  } catch {
    return null;
  }
};
