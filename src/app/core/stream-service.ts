import { effect, inject, Injectable } from '@angular/core';
import { StreamlensApi } from './streamlens-api';
import { FixtureStreamTransport } from './stream/fixture-transport';
import { SseStreamTransport } from './stream/sse-transport';
import { StreamStore } from './stream/stream-store';

/**
 * Decides *how* the app is connected and owns that decision.
 *
 * Live mode talks to the BFF, so it needs a broker. When the broker (or the BFF
 * itself) is not there, the app does not show an error screen: it replays a
 * session recorded from a real broker, and it keeps trying to get back live.
 */
@Injectable({ providedIn: 'root' })
export class StreamService {
  private readonly api = inject(StreamlensApi);
  private readonly store = inject(StreamStore);

  private live = false;
  private started = false;
  private switching = false;

  /** Watches for a dead live connection and degrades to the recording. */
  private readonly watchdog = effect(() => {
    const mode = this.store.mode();
    if (!this.started || !this.live || this.switching) return;
    if (mode !== 'offline') return;
    void this.useReplay('The live connection dropped');
  });

  async start(): Promise<void> {
    const probe = await this.probe();
    this.started = true;

    if (probe.live) {
      this.live = true;
      this.store.pushNotice({ level: 'info', code: 'live', message: `Live: ${probe.reason}` });
      this.store.attach(new SseStreamTransport());
    } else {
      this.live = false;
      this.store.pushNotice({
        level: 'warn',
        code: 'replay-fallback',
        message: `${probe.reason}.`,
      });
      this.store.attach(new FixtureStreamTransport());
    }

    this.store.connect();
  }

  /** A button in the UI: probe again and go back to the broker if it answers. */
  async reconnect(): Promise<void> {
    this.store.pushNotice({ level: 'info', code: 'probe', message: 'Probing the BFF…' });
    const probe = await this.probe();

    if (!probe.live) {
      this.store.pushNotice({ level: 'error', code: 'live-unavailable', message: probe.reason });
      return;
    }

    this.live = true;
    this.switching = true;
    this.store.pushNotice({ level: 'info', code: 'live', message: `Live: ${probe.reason}` });
    this.store.attach(new SseStreamTransport());
    this.store.connect();
    this.switching = false;
  }

  private async useReplay(reason: string): Promise<void> {
    this.switching = true;
    this.live = false;
    this.store.pushNotice({
      level: 'warn',
      code: 'replay-fallback',
      message: `${reason} — replaying the recorded session.`,
    });
    this.store.attach(new FixtureStreamTransport());
    this.store.connect();
    this.switching = false;
  }

  private async probe(): Promise<{ live: boolean; reason: string }> {
    try {
      const health = await this.api.health();
      if (health.kafka.connected) {
        const cluster = health.kafka.clusterId ?? 'unknown cluster';
        return { live: true, reason: `${health.kafka.brokers.join(', ')} · ${cluster}` };
      }
      return {
        live: false,
        reason: `Broker not connected (${health.kafka.error ?? 'no error reported'})`,
      };
    } catch (error) {
      return {
        live: false,
        reason: `BFF not reachable at /api/health (${error instanceof Error ? error.message : error})`,
      };
    }
  }
}
