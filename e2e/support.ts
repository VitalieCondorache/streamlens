import type { Page } from '@playwright/test';

/**
 * Console errors that are expected in replay mode.
 *
 * The app probes `/api/*` on purpose and falls back to the recording when nothing
 * answers, so those failed requests are the feature working, not a defect.
 */
const EXPECTED = [/\/api\//];

declare global {
  interface Window {
    /** Filled by the init script that `collectConsoleErrors` installs. */
    streamlensErrors?: string[];
  }
}

/**
 * Collects browser errors for the duration of a test, from inside the page.
 *
 * Reading them in the page rather than through Playwright's console events is deliberate:
 * the same failure is formatted differently per engine, and only some engines put the
 * message where a listener can see it. Angular reports a failed probe as
 * `console.error('ERROR', response)` — Chromium and WebKit inline the response text, while
 * Firefox logs the literal string "ERROR Error" and keeps the response inside the argument.
 * Formatting it here means the filter below sees `Http failure response for /api/lag` in
 * every engine, which keeps the assertion a real one: a swallowed Firefox-only defect
 * would still fail the suite.
 *
 * Unit tests render through jsdom, which happily ignores template errors that a real engine
 * would throw. Asserting on this list turns "the page looked fine" into "no uncaught
 * exception and no unexpected console error".
 */
export const collectConsoleErrors = async (page: Page): Promise<() => Promise<string[]>> => {
  await page.addInitScript(() => {
    const errors: string[] = [];
    const format = (value: unknown) =>
      value instanceof Error ? `${value.name}: ${value.message}` : String(value);

    // The original is kept and called: this observes the console, it does not hide it
    // from whatever else is listening.
    const original = console.error.bind(console);
    console.error = (...args: unknown[]) => {
      errors.push(args.map(format).join(' '));
      original(...args);
    };

    window.addEventListener('error', (event) => errors.push(`pageerror: ${event.message}`));
    window.addEventListener('unhandledrejection', (event) =>
      errors.push(`rejection: ${format(event.reason)}`),
    );
    Object.defineProperty(window, 'streamlensErrors', { value: errors });
  });

  return async () =>
    (await page.evaluate(() => window.streamlensErrors ?? [])).filter(
      (text) => !EXPECTED.some((pattern) => pattern.test(text)),
    );
};

/** Waits for the live tail to hold at least `count` rendered rows. */
export const waitForRows = async (page: Page, count: number): Promise<void> => {
  await page.waitForFunction(
    (expected) => document.querySelectorAll('app-virtual-list button.record').length >= expected,
    count,
    { timeout: 30_000 },
  );
};

/** The "Rows shown" counter of the tail page, as a number. */
export const rowsShown = async (page: Page): Promise<number> => {
  const text = await page
    .locator('app-stat-card', { hasText: 'Rows shown' })
    .locator('.stat__value')
    .innerText();
  return Number(text.replace(/[^\d]/g, ''));
};
