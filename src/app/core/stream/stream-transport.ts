import { InjectionToken } from '@angular/core';
import type {
  ConnectionMode,
  StreamEvent,
  StreamNotice,
  StreamReady,
} from '../models/stream.models';

/** Callbacks a transport reports to whoever owns the stream state. */
export interface StreamSink {
  ready(ready: StreamReady): void;
  events(events: readonly StreamEvent[]): void;
  notice(notice: StreamNotice): void;
  state(state: ConnectionMode, detail?: string): void;
}

export interface StreamOptions {
  readonly sessionId: string;
  readonly topics: readonly string[];
  /** `earliest` only makes sense on a brand new group (or after a rewind). */
  readonly fromBeginning: boolean;
}

/**
 * A transport produces a `StreamEvent` flow from *somewhere*.
 *
 * Two implementations exist on purpose: `SseStreamTransport` talks to the BFF and
 * therefore to a real broker, `FixtureStreamTransport` replays a stream recorded
 * from one. The UI code cannot tell them apart, which is what makes the deployed
 * demo (no backend) behave like the live product.
 */
export interface StreamTransport {
  readonly kind: 'sse' | 'fixture';
  start(options: StreamOptions, sink: StreamSink): void;
  stop(): void;
}

export const STREAM_TRANSPORT = new InjectionToken<StreamTransport>('streamlens.transport');
