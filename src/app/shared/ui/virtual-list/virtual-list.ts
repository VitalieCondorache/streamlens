import {
  ChangeDetectionStrategy,
  Component,
  computed,
  ElementRef,
  inject,
  input,
  signal,
  TemplateRef,
} from '@angular/core';
import { NgTemplateOutlet } from '@angular/common';

interface Row<T> {
  readonly item: T;
  readonly index: number;
}

/**
 * Windowed rendering, hand-written.
 *
 * Only the rows inside the viewport (plus a small overscan) exist in the DOM, so
 * the live tail can hold thousands of records in a signal without paying for
 * thousands of elements. Rows are positioned with a transform: no layout, just
 * compositing.
 */
@Component({
  selector: 'app-virtual-list',
  imports: [NgTemplateOutlet],
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div
      class="viewport"
      tabindex="0"
      role="log"
      [attr.aria-label]="label()"
      [style.height.px]="height()"
      [style.--row-height.px]="rowHeight()"
      (scroll)="onScroll($event)"
    >
      <div class="spacer" [style.height.px]="items().length * rowHeight()">
        @for (row of visibleRows(); track row.item) {
          <div
            class="virtual-row"
            [style.transform]="'translateY(' + row.index * rowHeight() + 'px)'"
          >
            <ng-container
              [ngTemplateOutlet]="rowTemplate()"
              [ngTemplateOutletContext]="{ $implicit: row.item, index: row.index }"
            />
          </div>
        }
      </div>
    </div>
  `,
  styles: `
    .viewport {
      overflow-y: auto;
      overscroll-behavior: contain;
      border: 1px solid var(--border);
      border-radius: var(--radius);
      background: var(--surface-2);
      contain: strict;
    }

    .spacer {
      position: relative;
    }

    /* Positioned by index: a transform keeps rows out of the layout path entirely. */
    .virtual-row {
      position: absolute;
      inset-inline: 0;
      top: 0;
      height: var(--row-height);
      will-change: transform;
    }
  `,
})
export class VirtualList<T> {
  readonly items = input.required<readonly T[]>();
  readonly rowTemplate = input.required<TemplateRef<unknown>>();
  readonly rowHeight = input(32);
  readonly height = input(460);
  readonly overscan = input(8);
  /** Announced to assistive tech: the viewport is focusable so it can be scrolled by keyboard. */
  readonly label = input('Scrollable records');

  private readonly scrollTop = signal(0);
  private readonly host = inject(ElementRef);

  protected readonly visibleRows = computed<readonly Row<T>[]>(() => {
    const items = this.items();
    const rowHeight = this.rowHeight();
    const overscan = this.overscan();

    const first = Math.max(0, Math.floor(this.scrollTop() / rowHeight) - overscan);
    const count = Math.ceil(this.height() / rowHeight) + overscan * 2;
    const last = Math.min(items.length, first + count);

    const rows: Row<T>[] = [];
    for (let index = first; index < last; index += 1) {
      rows.push({ item: items[index], index });
    }
    return rows;
  });

  protected onScroll(event: Event): void {
    this.scrollTop.set((event.target as HTMLElement).scrollTop);
  }

  /** Jump back to the newest row (the "follow" mode of the live tail). */
  scrollToTop(): void {
    const viewport = this.viewport();
    if (viewport === null) return;
    viewport.scrollTop = 0;
    this.scrollTop.set(0);
  }

  /**
   * Keeps the reader's anchor in place after rows were prepended.
   *
   * Rows are positioned by index, so inserting N rows at the top visually pushes
   * everything down by N * rowHeight while `scrollTop` stays where it was. Adding
   * that distance back cancels the shift — the classic "keep my place while the
   * feed grows" trick.
   */
  keepAnchor(prependedRows: number): void {
    const viewport = this.viewport();
    if (viewport === null || prependedRows <= 0) return;
    viewport.scrollTop += prependedRows * this.rowHeight();
    this.scrollTop.set(viewport.scrollTop);
  }

  private viewport(): HTMLElement | null {
    return (this.host.nativeElement as HTMLElement).querySelector('.viewport');
  }
}
