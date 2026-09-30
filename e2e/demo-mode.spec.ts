import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';

import { collectConsoleErrors, rowsShown, waitForRows } from './support';

/**
 * The suite runs against the production bundle with no BFF, so every page must work from
 * the recorded session. Selectors lean on the accessibility tree (roles, labels) wherever
 * the markup carries real semantics — which means a broken aria label fails the suite
 * instead of silently passing.
 */
const goto = async (page: Page, path: string) => {
  await page.goto(path, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('app-status-pill')).toContainText('Recorded session');
};

const rows = (page: Page) => page.locator('app-virtual-list button.record');

const sections = (page: Page) =>
  page.getByRole('navigation', { name: 'Sections' }).getByRole('link');

/** Axe against the densest pages: a violation here fails the build. */
const accessibilityViolations = async (page: Page) => {
  const results = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa']).analyze();
  return results.violations.map((violation) => ({
    id: violation.id,
    impact: violation.impact,
    nodes: violation.nodes.map((node) => node.target.join(' ')),
  }));
};

test.describe('replay mode — no backend', () => {
  test('reports the recorded session instead of failing', async ({ page }) => {
    await goto(page, '/');

    await expect(page.getByRole('heading', { level: 1, name: 'Stream overview' })).toBeVisible();
    await expect(
      page.getByText(/Replaying \d+ records captured from localhost:9092/),
    ).toBeVisible();
    // The footer doubles as the session/group readout, so assert it through its landmark role.
    await expect(page.getByRole('contentinfo')).toContainText('replay:recorded-session');
    await expect(page.getByText(/docker compose up -d/)).toBeVisible();
  });

  test('streams records into the live tail', async ({ page }) => {
    await goto(page, '/tail');
    await waitForRows(page, 6);

    // Every row carries real Kafka coordinates: topic, partition and offset.
    await expect(rows(page).first()).toContainText(/streamlens\.(orders|telemetry)\/p\d+@\d+/);

    const before = await rowsShown(page);
    await expect.poll(() => rowsShown(page), { timeout: 20_000 }).toBeGreaterThan(before);
  });

  test('pauses ingest, counts what it dropped, and resumes', async ({ page }) => {
    await goto(page, '/tail');
    await waitForRows(page, 6);

    await page.getByRole('button', { name: 'Pause ingest' }).click();
    await expect(page.getByRole('status')).toContainText('Ingest is paused');

    const before = await rowsShown(page);
    // The only fixed wait in the suite: asserting that the view stops growing. A recording
    // that reaches its end starts a new pass, which legitimately resets the view — so the
    // assertion is "never grows", not "stays identical".
    await page.waitForTimeout(1_200);
    expect(await rowsShown(page)).toBeLessThanOrEqual(before);

    // Records keep arriving and are discarded, which is what pause means here.
    await expect(page.getByRole('status')).toContainText(/[1-9]\d* dropped so far/, {
      timeout: 15_000,
    });

    await page.getByRole('button', { name: 'Resume ingest' }).click();
    await waitForRows(page, 3);
  });

  test('filters by event type', async ({ page }) => {
    await goto(page, '/tail');
    await waitForRows(page, 12);

    await page.getByLabel('Event type').selectOption('payment.failed');
    await waitForRows(page, 1);

    const types = await rows(page).locator('.record__type').allInnerTexts();
    expect(types.length).toBeGreaterThan(0);
    expect(types.every((type) => type.trim() === 'payment.failed')).toBe(true);
  });

  test('renders only the visible window of a long list', async ({ page }) => {
    await goto(page, '/tail');

    // Wait until the buffer holds clearly more records than the window can show.
    await expect.poll(() => rowsShown(page), { timeout: 30_000 }).toBeGreaterThan(45);
    const total = await rowsShown(page);
    const rendered = await rows(page).count();

    expect(rendered).toBeLessThan(total);
    expect(rendered).toBeLessThanOrEqual(32);

    // Pause first. While ingest runs, follow mode keeps pulling the viewport back to the
    // newest row, so a scroll-to-the-end assertion would race the next batch.
    await page.getByRole('button', { name: 'Pause ingest' }).click();
    await expect(page.getByRole('status')).toContainText('Ingest is paused');

    // Scrolling to the end reaches records far from the top without growing the DOM.
    await page.locator('.viewport').evaluate((element) => {
      element.scrollTop = element.scrollHeight;
    });
    await expect(rows(page).first().locator('.record__index')).not.toHaveText('0');
    expect(await rows(page).count()).toBeLessThanOrEqual(32);
  });

  test('holds the reader position while new records are prepended', async ({ page }) => {
    await goto(page, '/tail');
    await waitForRows(page, 20);

    // Stop following, then park the viewport in the middle of the buffer.
    await page.getByRole('button', { name: 'Following newest' }).click();
    await expect(page.getByRole('button', { name: 'Holding position' })).toBeVisible();
    await page.locator('.viewport').evaluate((element) => {
      element.scrollTop = 220;
    });

    /** Where a record sits inside the viewport, found by its Kafka coordinates. */
    const offsetOf = (coordinates: string) =>
      page.evaluate((wanted) => {
        const list = document.querySelector('.viewport');
        if (!list) return null;
        const top = list.getBoundingClientRect().top;
        const row = [...list.querySelectorAll('button.record')].find(
          (candidate) =>
            candidate.querySelector('.record__where')?.textContent?.replace(/\s+/g, '') === wanted,
        );
        return row ? Math.round(row.getBoundingClientRect().top - top) : null;
      }, coordinates);

    const anchor = (await rows(page).nth(1).locator('.record__where').innerText()).replace(
      /\s+/g,
      '',
    );
    const before = await offsetOf(anchor);
    expect(before).not.toBeNull();

    // Records keep arriving at the top; the buffer grows while the anchor must not move.
    const shown = await rowsShown(page);
    await expect.poll(() => rowsShown(page), { timeout: 20_000 }).toBeGreaterThan(shown + 3);

    const after = await offsetOf(anchor);
    expect(after).not.toBeNull();
    expect(Math.abs((after ?? 0) - (before ?? 0))).toBeLessThanOrEqual(2);
  });

  test('opens the record inspector for a row', async ({ page }) => {
    await goto(page, '/tail');
    await waitForRows(page, 4);

    // Clicking a row while records are being prepended is a race in every engine, and the
    // one WebKit loses; the inspector is what is under test here, so freeze the list first.
    await page.getByRole('button', { name: 'Pause ingest' }).click();
    await expect(page.getByRole('status')).toContainText('Ingest is paused');

    const target = rows(page).nth(1);
    const key = (await target.locator('.record__key').innerText()).trim();
    await target.click();

    const inspector = page.getByRole('complementary');
    await expect(inspector).toContainText('Record inspector');
    await expect(inspector).toContainText('Pipeline delay');
    await expect(inspector).toContainText(key);
    await expect(inspector).toContainText(/orderId|deviceId/);
  });
});

test.describe('the rest of the product', () => {
  test('verifies partition predictions against the recording', async ({ page }) => {
    await goto(page, '/key-router');

    const verify = page.getByRole('button', { name: 'Check the recording' });
    await expect(verify).toBeVisible();
    await verify.click();

    await expect(page.locator('app-stat-card', { hasText: 'Agreement' })).toContainText(
      'matches the recording',
    );
    await expect(page.locator('.verdict--ok').first()).toBeVisible();
    await expect(page.locator('.verdict--bad')).toHaveCount(0);
  });

  test('shows the recorded topology and the resume positions', async ({ page }) => {
    await goto(page, '/partitions');

    await expect(page.getByRole('heading', { level: 3, name: 'streamlens.orders' })).toBeVisible();
    // Header row plus the six partitions of the topic.
    await expect(page.locator('table').first().getByRole('row')).toHaveCount(7);
    await expect(
      page.getByRole('heading', { level: 3, name: 'streamlens.telemetry' }),
    ).toBeVisible();
    await expect(page.locator('table').nth(1).getByRole('row')).toHaveCount(4);

    await expect(page.getByRole('heading', { name: 'Resume positions' })).toBeVisible();
    await expect(page.locator('.positions li').first()).toBeVisible();
  });

  test('keeps the chosen theme across a reload', async ({ page }) => {
    await goto(page, '/overview');
    const root = page.locator('html');

    await expect(root).toHaveAttribute('data-theme', 'dark');
    await page.getByRole('button', { name: 'Toggle colour theme' }).click();
    await expect(root).toHaveAttribute('data-theme', 'light');

    await page.reload();
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
    expect(await page.evaluate(() => localStorage.getItem('streamlens.theme'))).toBe('light');
  });

  test('navigates every lazy route without a runtime error', async ({ page }) => {
    const errors = await collectConsoleErrors(page);
    await goto(page, '/overview');

    for (const name of [
      'Live tail',
      'Partitions & lag',
      'Key router',
      'How it works',
      'Overview',
    ]) {
      await sections(page).filter({ hasText: name }).click();
      await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
    }

    expect(await errors()).toEqual([]);
  });

  test('has no detectable WCAG A/AA violations on the shell and the tail', async ({ page }) => {
    await goto(page, '/overview');
    expect(await accessibilityViolations(page)).toEqual([]);

    await sections(page).filter({ hasText: 'Live tail' }).click();
    await waitForRows(page, 3);
    expect(await accessibilityViolations(page)).toEqual([]);
  });
});
