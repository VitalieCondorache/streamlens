import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import type { ConnectionMode } from '../../../core/models/stream.models';

const LABELS: Record<ConnectionMode, string> = {
  connecting: 'Connecting',
  live: 'Live from broker',
  reconnecting: 'Reconnecting',
  demo: 'Recorded session',
  offline: 'Offline',
};

const TONES: Record<ConnectionMode, string> = {
  connecting: 'pending',
  live: 'ok',
  reconnecting: 'warn',
  demo: 'info',
  offline: 'bad',
};

/** One pill for five connection states — the single place that maps state to colour. */
@Component({
  selector: 'app-status-pill',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <span class="status" [class]="'status--' + tone()" [title]="detail()">
      <span class="dot" aria-hidden="true"></span>
      <span>{{ label() }}</span>
    </span>
  `,
  styles: `
    .status {
      display: inline-flex;
      align-items: center;
      gap: var(--space-2);
      padding: 0.2rem 0.65rem;
      border-radius: 999px;
      border: 1px solid var(--border);
      background: var(--surface-2);
      font-size: var(--step--1);
      white-space: nowrap;
    }

    .dot {
      width: 0.5rem;
      height: 0.5rem;
      border-radius: 50%;
      background: var(--text-faint);
    }

    .status--ok .dot {
      background: var(--ok);
      box-shadow: 0 0 0 3px color-mix(in oklab, var(--ok) 25%, transparent);
    }
    .status--warn .dot {
      background: var(--warn);
    }
    .status--bad .dot {
      background: var(--bad);
    }
    .status--info .dot {
      background: var(--brand);
    }
    .status--pending .dot {
      background: var(--text-faint);
      animation: pulse 1.2s ease-in-out infinite;
    }

    @keyframes pulse {
      50% {
        opacity: 0.3;
      }
    }
  `,
})
export class StatusPill {
  readonly mode = input.required<ConnectionMode>();
  readonly detail = input('');

  protected readonly label = computed(() => LABELS[this.mode()]);
  protected readonly tone = computed(() => TONES[this.mode()]);
}
