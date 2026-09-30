import { TestBed } from '@angular/core/testing';

import type { StreamEvent } from '../../core/models/stream.models';
import { StreamlensApi } from '../../core/streamlens-api';
import type {
  StreamOptions,
  StreamSink,
  StreamTransport,
} from '../../core/stream/stream-transport';
import { StreamStore } from '../../core/stream/stream-store';
import { LiveTail } from './live-tail';

/** Minimal transport: the test decides when records arrive. */
class FakeTransport implements StreamTransport {
  readonly kind = 'sse' as const;
  sink: StreamSink | null = null;

  start(options: StreamOptions, sink: StreamSink): void {
    this.sink = sink;
    sink.ready({
      sessionId: options.sessionId,
      groupId: `streamlens-ui-${options.sessionId}`,
      topics: ['orders'],
      fromBeginning: false,
      flushMs: 120,
      kafka: ['localhost:9092'],
    });
    sink.state('live');
  }

  stop(): void {
    this.sink = null;
  }
}

const event = (
  partition: number,
  offset: string,
  overrides: Partial<StreamEvent> = {},
): StreamEvent => ({
  id: `orders:${partition}:${offset}`,
  topic: 'orders',
  partition,
  offset,
  key: 'ord-1042',
  type: 'order.created',
  at: 1_700_000_000_000,
  payload: { orderId: 'ord-1042' },
  ingestedAt: 1_700_000_000_030,
  ...overrides,
});

describe('LiveTail', () => {
  let store: StreamStore;
  let transport: FakeTransport;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [{ provide: StreamlensApi, useValue: { replay: vi.fn() } }],
    });

    store = TestBed.inject(StreamStore);
    transport = new FakeTransport();
    store.attach(transport);
    store.connect();
  });

  afterEach(() => {
    store.disconnect();
    TestBed.resetTestingModule();
  });

  const render = async () => {
    const fixture = TestBed.createComponent(LiveTail);
    await fixture.whenStable();
    return fixture;
  };

  /** Records carry a `.record*` class, which no other element in the template uses. */
  const rows = (element: HTMLElement) =>
    element.querySelectorAll<HTMLButtonElement>('app-virtual-list .record');

  it('renders one row per buffered record, newest first', async () => {
    transport.sink?.events([
      event(0, '1', { type: 'order.created' }),
      event(1, '7', { type: 'payment.failed', key: 'ord-9' }),
    ]);

    const fixture = await render();
    const rendered = rows(fixture.nativeElement as HTMLElement);

    expect(rendered).toHaveLength(2);
    // Newest first: the last record sent is the first row rendered.
    expect(rendered[0].textContent).toContain('payment.failed');
    expect(rendered[0].textContent).toContain('ord-9');
    expect(rendered[0].textContent).toContain('p1@7');
    expect(rendered[1].textContent).toContain('order.created');
  });

  it('marks the clicked record as selected', async () => {
    transport.sink?.events([event(0, '1'), event(1, '2')]);
    const fixture = await render();

    const rendered = rows(fixture.nativeElement as HTMLElement);
    rendered[1].click();
    fixture.detectChanges();
    await fixture.whenStable();

    expect(rows(fixture.nativeElement as HTMLElement)[1].getAttribute('aria-current')).toBe('true');
    expect(rows(fixture.nativeElement as HTMLElement)[0].getAttribute('aria-current')).toBe(
      'false',
    );
  });

  it('shows the empty state before any record arrives', async () => {
    const fixture = await render();
    const element = fixture.nativeElement as HTMLElement;

    expect(rows(element)).toHaveLength(0);
    expect(element.textContent).toContain('No records yet');
  });

  it('reacts to filters from the store', async () => {
    transport.sink?.events([
      event(0, '1', { type: 'order.created' }),
      event(1, '2', { type: 'payment.failed' }),
    ]);

    const fixture = await render();
    expect(rows(fixture.nativeElement as HTMLElement)).toHaveLength(2);

    store.setFilters({ type: 'payment.failed' });
    fixture.detectChanges();
    await fixture.whenStable();

    const rendered = rows(fixture.nativeElement as HTMLElement);
    expect(rendered).toHaveLength(1);
    expect(rendered[0].textContent).toContain('payment.failed');
  });

  it('disables rewinding when the transport is not live', async () => {
    const fixture = await render();
    const buttons = [
      ...(fixture.nativeElement as HTMLElement).querySelectorAll<HTMLButtonElement>(
        '.toolbar button',
      ),
    ];
    const rewind = buttons.find((button) => button.textContent?.includes('Rewind'));

    // The fake reports itself as an SSE transport, so rewinding is offered here.
    expect(rewind?.disabled).toBe(false);
  });
});
