import { inject, Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import type {
  HealthResponse,
  LagResponse,
  ProbeResponse,
  SimulatorState,
  TopicsResponse,
} from './models/stream.models';

export interface ReplayResult {
  readonly groupId: string;
  readonly topic: string;
  readonly requested: number;
  readonly perPartition: number;
  readonly partitions: readonly { partition: number; from: string; to: string }[];
}

/**
 * The command half of the BFF API (the query half is polled through
 * `httpResource` in the diagnostics store).
 *
 * Endpoints are called on demand and return promises, because every one of them
 * is triggered by a user gesture: start the traffic generator, rewind the group,
 * ask the broker where a key lands.
 */
@Injectable({ providedIn: 'root' })
export class StreamlensApi {
  private readonly http = inject(HttpClient);
  private readonly base = '/api';

  /** Cheap probe used to decide between live mode and the recorded demo. */
  health(): Promise<HealthResponse> {
    return firstValueFrom(this.http.get<HealthResponse>(`${this.base}/health`));
  }

  topics(): Promise<TopicsResponse> {
    return firstValueFrom(this.http.get<TopicsResponse>(`${this.base}/topics`));
  }

  lag(groupId?: string): Promise<LagResponse> {
    const query = groupId ? `?group=${encodeURIComponent(groupId)}` : '';
    return firstValueFrom(this.http.get<LagResponse>(`${this.base}/lag${query}`));
  }

  simulator(action: 'start' | 'stop', ratePerSecond?: number): Promise<SimulatorState> {
    return firstValueFrom(
      this.http.post<SimulatorState>(`${this.base}/simulator`, { action, ratePerSecond }),
    );
  }

  /**
   * Asks the broker which partition each key landed on. The frontend compares this
   * with its own murmur2 prediction — see the Key Router view.
   */
  probePartitions(keys: readonly string[], topic: string): Promise<ProbeResponse> {
    return firstValueFrom(
      this.http.post<ProbeResponse>(`${this.base}/partition-probe`, { keys: [...keys], topic }),
    );
  }

  /** Rewrites the offsets of this session's consumer group (the replay control). */
  replay(sessionId: string, count: number, topic: string): Promise<ReplayResult> {
    return firstValueFrom(
      this.http.post<ReplayResult>(`${this.base}/replay`, { sessionId, count, topic }),
    );
  }
}
