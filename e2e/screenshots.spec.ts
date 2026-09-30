import { expect, test, type Page } from '@playwright/test';

import { waitForRows } from './support';

/**
 * README screenshots, produced by the app itself.
 *
 *   npm run screenshots        against the built bundle, replaying the recording
 *   npm run screenshots:live   against the dev server and a real broker (Docker up)
 *
 * Keeping them generated means the documentation cannot drift from the UI: re-run the
 * command and commit the diff.
 */
const OUT = 'docs/screenshots';
const LIVE = process.env.E2E_LIVE_URL;

const open = async (page: Page, path: string) => {
  await page.goto(`${LIVE ?? ''}${path}`, { waitUntil: 'domcontentloaded' });
  // Live mode proves the whole stack; replay mode proves it works without one.
  await expect(page.locator('app-status-pill')).toContainText(
    LIVE ? 'Live from broker' : 'Recorded session',
  );
};

const settle = (page: Page, ms = 1_500) => page.waitForTimeout(ms);

test.describe('documentation screenshots', () => {
  test.skip(process.env.SCREENSHOTS !== '1', 'set SCREENSHOTS=1 (npm run screenshots)');

  test('overview', async ({ page }) => {
    await open(page, '/overview');
    // Wait for data, not just the shell: the first sparkline sample and the notice feed.
    await expect(page.locator('app-sparkline svg polyline')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Transport notices' })).toBeVisible();
    await settle(page);
    await page.screenshot({ path: `${OUT}/01-overview.png`, fullPage: true });
  });

  test('live tail', async ({ page }) => {
    await open(page, '/tail');
    await waitForRows(page, 18);
    await settle(page);
    await page.screenshot({ path: `${OUT}/02-live-tail.png`, fullPage: true });
  });

  test('record inspector', async ({ page }) => {
    await open(page, '/tail');
    await waitForRows(page, 5);
    await page.locator('app-virtual-list button.record').nth(1).click();
    await expect(page.getByRole('complementary')).toContainText('Record inspector');
    await settle(page);
    await page.screenshot({ path: `${OUT}/03-inspector.png`, fullPage: true });
  });

  test('partitions and lag', async ({ page }) => {
    await open(page, '/partitions');
    await expect(page.getByRole('heading', { name: 'Resume positions' })).toBeVisible();
    await settle(page);
    await page.screenshot({ path: `${OUT}/04-partitions.png`, fullPage: true });
  });

  test('key router', async ({ page }) => {
    await open(page, '/key-router');
    await page
      .getByRole('button', { name: LIVE ? 'Ask the broker' : 'Check the recording' })
      .click();
    await expect(page.locator('.verdict--ok').first()).toBeVisible();
    await settle(page);
    await page.screenshot({ path: `${OUT}/05-key-router.png`, fullPage: true });
  });

  test('architecture', async ({ page }) => {
    await open(page, '/architecture');
    await settle(page, 600);
    await page.screenshot({ path: `${OUT}/06-architecture.png`, fullPage: true });
  });
});
