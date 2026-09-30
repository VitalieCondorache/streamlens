import { createFrameBatcher, type Scheduler } from './frame-batcher';

/** Deterministic scheduler: nothing runs until the test says so. */
const manualScheduler = () => {
  const queue: (() => void)[] = [];
  const scheduler: Scheduler = (run) => {
    queue.push(run);
    return () => {
      const index = queue.indexOf(run);
      if (index >= 0) queue.splice(index, 1);
    };
  };

  return {
    scheduler,
    run() {
      queue.splice(0).forEach((task) => task());
    },
    scheduled: () => queue.length,
  };
};

describe('createFrameBatcher', () => {
  it('coalesces everything pushed before the frame into a single flush', () => {
    const clock = manualScheduler();
    const flushed: string[][] = [];
    const batcher = createFrameBatcher<string>((items) => flushed.push([...items]), {
      scheduler: clock.scheduler,
    });

    batcher.push(['a']);
    batcher.push(['b', 'c']);

    expect(flushed).toEqual([]);
    expect(clock.scheduled()).toBe(1);

    clock.run();

    expect(flushed).toEqual([['a', 'b', 'c']]);
    expect(batcher.pending()).toBe(0);
  });

  it('schedules again after a flush', () => {
    const clock = manualScheduler();
    const flushed: string[][] = [];
    const batcher = createFrameBatcher<string>((items) => flushed.push([...items]), {
      scheduler: clock.scheduler,
    });

    batcher.push(['a']);
    clock.run();
    batcher.push(['b']);
    clock.run();

    expect(flushed).toEqual([['a'], ['b']]);
  });

  it('ignores empty pushes and does no work when idle', () => {
    const clock = manualScheduler();
    const flushed: string[][] = [];
    const batcher = createFrameBatcher<string>((items) => flushed.push([...items]), {
      scheduler: clock.scheduler,
    });

    batcher.push([]);
    expect(clock.scheduled()).toBe(0);
    clock.run();
    expect(flushed).toEqual([]);
  });

  it('flushes immediately on demand (teardown path)', () => {
    const clock = manualScheduler();
    const flushed: string[][] = [];
    const batcher = createFrameBatcher<string>((items) => flushed.push([...items]), {
      scheduler: clock.scheduler,
    });

    batcher.push(['a']);
    batcher.flushNow();

    expect(flushed).toEqual([['a']]);
  });

  it('caps how many items a single flush may carry', () => {
    const clock = manualScheduler();
    const flushed: number[][] = [];
    const batcher = createFrameBatcher<number>((items) => flushed.push([...items]), {
      scheduler: clock.scheduler,
      maxItemsPerFlush: 2,
    });

    batcher.push([1, 2, 3, 4]);
    clock.run();

    // The newest items win: a stalled client must not delay the latest state.
    expect(flushed).toEqual([[3, 4]]);
  });

  it('drops queued items once disposed', () => {
    const clock = manualScheduler();
    const flushed: string[][] = [];
    const batcher = createFrameBatcher<string>((items) => flushed.push([...items]), {
      scheduler: clock.scheduler,
    });

    batcher.push(['a']);
    batcher.dispose();
    batcher.push(['b']);
    clock.run();

    expect(flushed).toEqual([]);
  });
});
