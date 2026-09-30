/**
 * Coalesces bursts of updates into one signal write.
 *
 * A busy topic delivers hundreds of records per second; writing each one into a
 * signal would schedule hundreds of change-detection runs. Collecting them and
 * flushing once per animation frame keeps the UI at 60fps regardless of the rate.
 *
 * The scheduler is injectable so tests can run it deterministically.
 */
export type Scheduler = (run: () => void) => () => void;

const animationFrameScheduler: Scheduler = (run) => {
  const handle = requestAnimationFrame(run);
  return () => cancelAnimationFrame(handle);
};

const timeoutScheduler =
  (delayMs = 16): Scheduler =>
  (run) => {
    const handle = setTimeout(run, delayMs);
    return () => clearTimeout(handle);
  };

export const defaultScheduler: Scheduler =
  typeof requestAnimationFrame === 'function' ? animationFrameScheduler : timeoutScheduler();

export interface FrameBatcher<T> {
  /** Queues items; they are handed to `flush` on the next scheduled frame. */
  push(items: readonly T[]): void;
  /** Flushes immediately (used on teardown so nothing is silently lost). */
  flushNow(): void;
  /** Pending item count, exposed for tests and diagnostics. */
  pending(): number;
  dispose(): void;
}

export const createFrameBatcher = <T>(
  flush: (items: readonly T[]) => void,
  options: { scheduler?: Scheduler; maxItemsPerFlush?: number } = {},
): FrameBatcher<T> => {
  const scheduler = options.scheduler ?? defaultScheduler;
  const maxItemsPerFlush = options.maxItemsPerFlush ?? Number.POSITIVE_INFINITY;

  let pending: T[] = [];
  let cancel: (() => void) | null = null;
  let disposed = false;

  const run = () => {
    cancel = null;
    if (pending.length === 0) return;
    const items = pending.length > maxItemsPerFlush ? pending.slice(-maxItemsPerFlush) : pending;
    pending = [];
    flush(items);
  };

  return {
    push(items) {
      if (disposed || items.length === 0) return;
      pending.push(...items);
      if (cancel === null) cancel = scheduler(run);
    },
    flushNow() {
      cancel?.();
      run();
    },
    pending: () => pending.length,
    dispose() {
      disposed = true;
      cancel?.();
      cancel = null;
      pending = [];
    },
  };
};
