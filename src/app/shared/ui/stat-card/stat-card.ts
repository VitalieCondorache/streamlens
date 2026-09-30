import { ChangeDetectionStrategy, Component, input } from '@angular/core';

export type StatTone = 'neutral' | 'brand' | 'ok' | 'warn' | 'bad';

@Component({
  selector: 'app-stat-card',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <article class="stat" [class]="'stat--' + tone()">
      <p class="stat__label">{{ label() }}</p>
      <p class="stat__value">
        {{ value() }}<span class="stat__unit">{{ unit() }}</span>
      </p>
      @if (hint()) {
        <p class="stat__hint">{{ hint() }}</p>
      }
    </article>
  `,
  styles: `
    .stat {
      background: var(--surface);
      border: 1px solid var(--border);
      border-left: 3px solid var(--border);
      border-radius: var(--radius);
      padding: var(--space-3) var(--space-4);
      display: flex;
      flex-direction: column;
      gap: var(--space-1);
      min-width: 0;
    }

    .stat__label {
      margin: 0;
      font-size: var(--step--1);
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--text-faint);
    }

    .stat__value {
      margin: 0;
      font-family: var(--font-mono);
      font-size: var(--step-2);
      font-variant-numeric: tabular-nums;
      line-height: 1.1;
      overflow-wrap: anywhere;
    }

    .stat__unit {
      font-size: var(--step-0);
      color: var(--text-muted);
      margin-left: 0.25rem;
    }

    .stat__hint {
      margin: 0;
      font-size: var(--step--1);
      color: var(--text-faint);
    }

    .stat--brand {
      border-left-color: var(--brand);
    }
    .stat--ok {
      border-left-color: var(--ok);
    }
    .stat--warn {
      border-left-color: var(--warn);
    }
    .stat--bad {
      border-left-color: var(--bad);
    }
  `,
})
export class StatCard {
  readonly label = input.required<string>();
  readonly value = input.required<string | number>();
  readonly unit = input('');
  readonly hint = input('');
  readonly tone = input<StatTone>('neutral');
}
