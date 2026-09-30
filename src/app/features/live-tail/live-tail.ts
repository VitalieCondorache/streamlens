import {
  afterRenderEffect,
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
  TemplateRef,
  untracked,
  viewChild,
} from '@angular/core';

import { StreamStore } from '../../core/stream/stream-store';
import { filterEvents } from '../../core/stream/stream-stats';
import type { StreamEvent } from '../../core/models/stream.models';
import { JsonNode } from '../../shared/ui/json-node/json-node';
import { StatCard } from '../../shared/ui/stat-card/stat-card';
import { VirtualList } from '../../shared/ui/virtual-list/virtual-list';
import { compact, duration, integer, percent, timeOfDay } from '../../shared/format';

/** Context handed to the row template by the virtual list. */
export interface EventRowContext {
  readonly $implicit: StreamEvent;
  readonly index: number;
}

@Component({
  selector: 'app-live-tail',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [JsonNode, StatCard, VirtualList],
  templateUrl: './live-tail.html',
  styleUrl: './live-tail.scss',
})
export class LiveTail {
  protected readonly stream = inject(StreamStore);

  protected readonly eventRowTemplate =
    viewChild.required<TemplateRef<EventRowContext>>('eventRow');
  private readonly list = viewChild<VirtualList<StreamEvent>>(VirtualList);

  protected readonly filters = this.stream.filters;
  protected readonly rows = this.stream.filtered;
  protected readonly stats = this.stream.stats;
  protected readonly counters = this.stream.counters;

  protected readonly selected = signal<StreamEvent | null>(null);
  protected readonly follow = signal(true);
  protected readonly rewindCount = signal('500');
  protected readonly rewindTopic = signal('');
  protected readonly rewindBusy = signal(false);
  protected readonly rewindOptions = ['100', '500', '2000'];

  protected readonly integer = integer;
  protected readonly compact = compact;
  protected readonly percent = percent;
  protected readonly duration = duration;

  /** Rewinding means rewriting committed offsets, so it needs a live BFF. */
  protected readonly canRewind = computed(() => this.stream.transportKind() === 'sse');
  protected readonly rewindTargets = computed(() => this.stream.topics());

  /**
   * Keeps the viewport honest while new rows land on top.
   *
   * `afterRenderEffect` is deliberate: the scroll compensation must happen after
   * the DOM has been updated with the new spacer height, otherwise the numbers we
   * read back from the element are stale. `untracked` keeps the effect from
   * re-running when the user toggles follow or changes a filter.
   */
  private readonly scrollAnchor = afterRenderEffect(() => {
    const batch = this.stream.lastBatch();
    if (batch.length === 0) return;

    const prepended = filterEvents(
      batch,
      untracked(() => this.stream.filters()),
    ).length;
    const list = untracked(() => this.list());
    if (prepended === 0 || list === undefined) return;

    if (untracked(() => this.follow())) list.scrollToTop();
    else list.keepAnchor(prepended);
  });

  /** Deselect when the chosen record scrolls out of the buffer. */
  private readonly clearStaleSelection = effect(() => {
    const selected = this.selected();
    if (selected === null) return;
    if (this.stream.events().some((event) => event.id === selected.id)) return;
    untracked(() => this.selected.set(null));
  });

  protected onRowClick(event: StreamEvent): void {
    this.selected.set(this.selected()?.id === event.id ? null : event);
  }

  protected trackById(_index: number, event: StreamEvent): string {
    return event.id;
  }

  protected time(event: StreamEvent): string {
    return timeOfDay(event.at);
  }

  protected togglePause(): void {
    this.stream.togglePause();
  }

  protected setTopic(event: Event): void {
    this.stream.setFilters({ topic: (event.target as HTMLSelectElement).value });
  }

  protected setType(event: Event): void {
    this.stream.setFilters({ type: (event.target as HTMLSelectElement).value });
  }

  protected setQuery(event: Event): void {
    this.stream.setFilters({ query: (event.target as HTMLInputElement).value });
  }

  protected setRewindTopic(event: Event): void {
    this.rewindTopic.set((event.target as HTMLSelectElement).value);
  }

  protected setRewindCount(event: Event): void {
    this.rewindCount.set((event.target as HTMLSelectElement).value);
  }

  protected toggleFollow(): void {
    const next = !this.follow();
    this.follow.set(next);
    if (next) this.list()?.scrollToTop();
  }

  protected clear(): void {
    this.selected.set(null);
    this.stream.clearBuffer();
  }

  protected async rewind(): Promise<void> {
    const topic = this.rewindTopic() || this.rewindTargets()[0];
    if (topic === undefined) return;

    this.rewindBusy.set(true);
    this.selected.set(null);
    await this.stream.rewind(Number(this.rewindCount()), topic);
    this.rewindBusy.set(false);
    this.list()?.scrollToTop();
  }

  protected startOver(fromBeginning: boolean): void {
    this.selected.set(null);
    this.stream.startOver({ fromBeginning });
  }
}
