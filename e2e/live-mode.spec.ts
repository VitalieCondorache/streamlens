import { expect, test, type Page } from '@playwright/test';

import { rowsShown, waitForRows } from './support';

/**
 * The live path: browser ↔ BFF ↔ broker.
 *
 *   1. docker compose up -d          (Kafka 4.1 + the bridge, producing traffic)
 *   2. npm start                     (dev server proxying /api to the BFF)
 *   3. npm run test:e2e:live
 *
 * This is the only suite that can prove the claims the README makes about Kafka
 * itself: that committed offsets *are* the resume point, that rewinding rewrites
 * them, and that the browser's murmur2 agrees with the broker. It needs the stack,
 * so it is skipped unless E2E_LIVE_URL points at a dev server.
 */
const LIVE = process.env.E2E_LIVE_URL;

const rows = (page: Page) => page.locator('app-virtual-list button.record');
const live = (page: Page) =>
  expect(page.locator('app-status-pill')).toContainText('Live from broker');

test.describe('live stack', () => {
  test.skip(!LIVE, 'set E2E_LIVE_URL (npm run test:e2e:live) to run against a real broker');

  test('streams records from the broker with their Kafka coordinates', async ({ page }) => {
    await page.goto('/tail', { waitUntil: 'domcontentloaded' });
    await live(page);
    await waitForRows(page, 10);

    await expect(rows(page).first()).toContainText(/streamlens\.(orders|telemetry)\/p\d+@\d+/);

    const before = await rowsShown(page);
    await expect.poll(() => rowsShown(page), { timeout: 30_000 }).toBeGreaterThan(before);
  });

  test('shows log ends and lag the broker really tracks', async ({ page }) => {
    await page.goto('/partitions', { waitUntil: 'domcontentloaded' });
    await live(page);

    // This hint is only rendered when /api/lag answered.
    await expect(page.getByText('records behind the log end')).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Resume positions' })).toBeVisible();

    // Columns: Partition, In buffer, Distribution, Log start, Log end, Committed, Lag.
    const logEnd = await page.locator('table tbody tr').first().locator('td').nth(3).innerText();
    expect(Number(logEnd)).toBeGreaterThan(0);
  });

  test('agrees with the broker on where every key lands', async ({ page }) => {
    await page.goto('/key-router', { waitUntil: 'domcontentloaded' });
    await live(page);

    await page.getByRole('button', { name: 'Ask the broker' }).click();
    await expect(page.locator('.verdict--ok').first()).toBeVisible({ timeout: 30_000 });

    const agreement = await page
      .locator('app-stat-card', { hasText: 'Agreement' })
      .locator('.stat__value')
      .innerText();
    const [matched, compared] = agreement.split('/').map((part) => Number(part.trim()));

    expect(compared).toBeGreaterThan(0);
    expect(matched).toBe(compared);
    // The broker's own partition count is what the prediction used (options are not
    // "visible" to Playwright, so this counts them instead of asserting visibility).
    await expect(page.getByText(/broker default \(\d+\)/)).toHaveCount(1);
  });

  test('keeps streaming after a rewind from the tail', async ({ page }) => {
    await page.goto('/tail', { waitUntil: 'domcontentloaded' });
    await waitForRows(page, 10);

    await page.getByLabel('Rewind topic').selectOption('streamlens.orders');
    await page.getByRole('button', { name: 'Rewind', exact: true }).click();

    // The button is renamed while the offsets are rewritten, so this waits for it to come back…
    await expect(page.getByRole('button', { name: 'Rewind', exact: true })).toBeEnabled({
      timeout: 45_000,
    });
    // …and then the stream has to come back on top of the rewound offsets.
    await expect.poll(() => rowsShown(page), { timeout: 45_000 }).toBeGreaterThan(0);
    await expect(page.locator('app-status-pill')).toContainText('Live from broker', {
      timeout: 30_000,
    });
  });
});

test.describe('live endpoints', () => {
  test.skip(!LIVE, 'set E2E_LIVE_URL (npm run test:e2e:live) to run against a real broker');

  test('rewinding a group rewrites its committed offsets', async ({ request }) => {
    const session = `e2e${Date.now()}`;
    const groupId = `streamlens-ui-${session}`;

    const replayed = await request.post(`${LIVE}/api/replay`, {
      data: { sessionId: session, count: 20, topic: 'streamlens.orders' },
    });
    expect(replayed.ok()).toBeTruthy();

    const rewind = await replayed.json();
    expect(rewind).toMatchObject({ groupId, topic: 'streamlens.orders', reconnect: true });
    expect(rewind.partitions).toHaveLength(6);

    // The broker now holds offsets for a group that has no members: that is the resume point.
    const lag = await (await request.get(`${LIVE}/api/lag?group=${groupId}`)).json();
    expect(lag.groupId).toBe(groupId);

    const orders = lag.partitions.filter(
      (entry: { topic: string }) => entry.topic === 'streamlens.orders',
    );
    expect(orders).toHaveLength(6);
    for (const entry of orders) {
      expect(entry.committedOffset).not.toBeNull();
      expect(Number(entry.committedOffset)).toBeLessThanOrEqual(Number(entry.nextOffset));
    }
    expect(Number(lag.totalLag)).toBeGreaterThanOrEqual(0);
  });

  test('asks the broker for a partition per key', async ({ request }) => {
    const keys = ['ord-1000', 'ord-1100', 'scan-berlin-1-1'];

    const response = await request.post(`${LIVE}/api/partition-probe`, {
      data: { keys, topic: 'streamlens.orders' },
    });
    expect(response.ok()).toBeTruthy();

    const probe = await response.json();
    expect(probe.keyCount).toBe(keys.length);
    expect(probe.probes).toHaveLength(keys.length);
    for (const entry of probe.probes) {
      expect(entry.partition).toBeGreaterThanOrEqual(0);
      expect(entry.partition).toBeLessThan(probe.partitions);
    }
  });

  test('refuses a probe without keys', async ({ request }) => {
    const response = await request.post(`${LIVE}/api/partition-probe`, { data: { keys: [] } });

    expect(response.status()).toBe(400);
    expect((await response.json()).error).toBe('missing_keys');
  });

  test('reports the topology it owns, never auto-created by the broker', async ({ request }) => {
    const topics = await (await request.get(`${LIVE}/api/topics`)).json();

    const orders = topics.topics.find(
      (topic: { name: string }) => topic.name === 'streamlens.orders',
    );
    const telemetry = topics.topics.find(
      (topic: { name: string }) => topic.name === 'streamlens.telemetry',
    );
    expect(orders.partitions).toHaveLength(6);
    expect(telemetry.partitions).toHaveLength(3);
  });
});
