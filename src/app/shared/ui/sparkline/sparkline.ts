import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';

/**
 * A sparkline drawn by hand — no chart library, no canvas, no layout thrash.
 *
 * The path is a `computed`: the SVG `d` attribute is only recalculated when the
 * samples change, and it is regenerated in one string, so the browser does a
 * single attribute write per update.
 */
@Component({
  selector: 'app-sparkline',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <svg
      class="spark"
      [attr.viewBox]="'0 0 ' + width() + ' ' + height()"
      preserveAspectRatio="none"
      role="img"
      [attr.aria-label]="ariaLabel()"
    >
      <polygon class="spark__area" [attr.points]="areaPoints()" />
      <polyline class="spark__line" [attr.points]="linePoints()" />
    </svg>
  `,
  styles: `
    .spark {
      display: block;
      width: 100%;
      height: 100%;
      overflow: visible;
    }

    .spark__line {
      fill: none;
      stroke: var(--brand);
      stroke-width: 2;
      stroke-linejoin: round;
      vector-effect: non-scaling-stroke;
    }

    .spark__area {
      fill: color-mix(in oklab, var(--brand) 22%, transparent);
      stroke: none;
    }
  `,
})
export class Sparkline {
  readonly values = input.required<readonly number[]>();
  readonly width = input(240);
  readonly height = input(48);
  readonly label = input('Series');

  protected readonly linePoints = computed(() => this.points(false));
  protected readonly areaPoints = computed(() => this.points(true));

  protected readonly ariaLabel = computed(() => {
    const values = this.values();
    if (values.length === 0) return `${this.label()}: no data yet`;
    const latest = values.at(-1) ?? 0;
    const peak = Math.max(...values);
    return `${this.label()}: latest ${latest}, peak ${peak} over ${values.length} samples`;
  });

  private points(close: boolean): string {
    const values = this.values();
    const width = this.width();
    const height = this.height();
    if (values.length === 0) return `0,${height} ${width},${height}`;

    const max = Math.max(...values, 1);
    const step = values.length > 1 ? width / (values.length - 1) : width;
    const coordinates = values.map((value, index) => {
      const x = index * step;
      const y = height - (value / max) * (height - 4) - 2;
      return `${round(x)},${round(y)}`;
    });

    return close
      ? `0,${height} ${coordinates.join(' ')} ${width},${height}`
      : coordinates.join(' ');
  }
}

const round = (value: number): number => Math.round(value * 100) / 100;
