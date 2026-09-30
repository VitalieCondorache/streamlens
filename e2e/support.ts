import type { ConsoleMessage, Page } from '@playwright/test';

/**
 * Console errors that are expected in replay mode.
 *
 * The app probes `/api/*` on purpose and falls back to the recording when nothing
 * answers, so those failed requests are the feature working, not a defect.
 */
const EXPECTED = [/\/api\//];

/**
 * Collects browser errors for the duration of a test.
 *
 * Unit tests render through jsdom, which happily ignores template errors that a real
 * engine would throw. Asserting on this list turns "the page looked fine" into
 * "no uncaught exception and no unexpected console error".
 */
export const collectConsoleErrors = (page: Page): string[] => {
  const errors: string[] = [];

  page.on('console', (message: ConsoleMessage) => {
    if (message.type() !== 'error') return;
    const text = `${message.text()} ${message.location().url}`;
    if (EXPECTED.some((pattern) => pattern.test(text))) return;
    errors.push(text);
  });

  page.on('pageerror', (error) => errors.push(`pageerror: ${error.message}`));

  return errors;
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
