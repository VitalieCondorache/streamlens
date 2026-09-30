import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';

interface Entry {
  readonly key: string;
  readonly value: unknown;
}

const toEntries = (value: unknown): readonly Entry[] | null => {
  if (Array.isArray(value)) {
    return value.map((item, index) => ({ key: String(index), value: item }));
  }
  if (value !== null && typeof value === 'object') {
    return Object.entries(value as Record<string, unknown>).map(([key, item]) => ({
      key,
      value: item,
    }));
  }
  return null;
};

const renderLeaf = (value: unknown): string => {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (typeof value === 'string') return `"${value}"`;
  return String(value);
};

const kindOf = (value: unknown): string => {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
};

const summarize = (value: unknown): string => {
  const entries = toEntries(value);
  if (entries === null) return '';
  return Array.isArray(value) ? `[${entries.length}]` : `{${entries.length}}`;
};

/**
 * Payload inspector built from native `<details>` elements.
 *
 * Recursion is the honest way to render arbitrary JSON, and `<details>` brings
 * keyboard support, focus management and screen-reader semantics along for free —
 * a hand-rolled accordion would have to reimplement all three.
 */
@Component({
  selector: 'app-json-node',
  imports: [JsonNode],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    @if (entries(); as children) {
      <details [open]="depth() < 1">
        <summary>
          @if (label()) {
            <span class="key">{{ label() }}</span>
          }
          <span class="faint">{{ summary() }}</span>
        </summary>
        <div class="children">
          @for (child of children; track child.key) {
            <app-json-node [label]="child.key" [value]="child.value" [depth]="depth() + 1" />
          }
        </div>
      </details>
    } @else {
      <div class="leaf">
        @if (label()) {
          <span class="key">{{ label() }}</span>
        }
        <span [class]="'val val--' + kind()">{{ display() }}</span>
      </div>
    }
  `,
  styles: `
    :host {
      display: block;
      font-family: var(--font-mono);
      font-size: var(--step--1);
    }

    details {
      border-left: 1px solid var(--border);
      padding-left: var(--space-3);
      margin-left: var(--space-1);
    }

    summary {
      cursor: pointer;
      display: flex;
      gap: var(--space-2);
      align-items: baseline;
    }

    summary:focus-visible {
      outline: 2px solid var(--brand);
      outline-offset: 2px;
    }

    .children {
      margin-top: var(--space-1);
      display: flex;
      flex-direction: column;
      gap: 2px;
    }

    .leaf {
      display: flex;
      gap: var(--space-2);
      padding-left: var(--space-3);
    }

    .key {
      color: var(--text-muted);
    }
    .val {
      overflow-wrap: anywhere;
    }
    .val--string {
      color: var(--ok);
    }
    .val--number {
      color: var(--brand);
    }
    .val--boolean {
      color: var(--warn);
    }
    .val--null,
    .val--undefined {
      color: var(--text-faint);
    }
  `,
})
export class JsonNode {
  readonly label = input('');
  readonly value = input.required<unknown>();
  readonly depth = input(0);

  protected readonly entries = computed(() => toEntries(this.value()));
  protected readonly display = computed(() => renderLeaf(this.value()));
  protected readonly kind = computed(() => kindOf(this.value()));
  protected readonly summary = computed(() => summarize(this.value()));
}
