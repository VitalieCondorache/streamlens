import { defineConfig, devices } from '@playwright/test';

const PORT = Number(process.env.E2E_PORT ?? 4173);
const baseURL = `http://127.0.0.1:${PORT}`;

/**
 * End-to-end suite.
 *
 * It runs the *built* bundle (see `npm run test:e2e`) served as a static site, with no
 * `/api` backend on the origin. That is deliberate: the app then takes its recorded-session
 * path, so the suite is deterministic — no Kafka, no Docker, no network — and it still
 * exercises the lazy routes, the virtual list and the defer blocks in a real browser.
 *
 * Live mode against Docker is covered by `npm run bff:smoke` plus the unit tests; a page
 * object for it can be added the same way, pointing at the dev server instead.
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  timeout: 45_000,
  expect: { timeout: 10_000 },
  reporter: process.env.CI
    ? [['github'], ['list'], ['html', { outputFolder: 'playwright-report', open: 'never' }]]
    : [['list'], ['html', { outputFolder: 'playwright-report', open: 'never' }]],

  use: {
    baseURL,
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 2,
    // The app follows the OS preference, so pin it: the tests and the README
    // screenshots then always describe the same theme.
    colorScheme: 'dark',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'off',
  },

  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        viewport: { width: 1440, height: 900 },
        deviceScaleFactor: 2,
      },
    },
  ],

  webServer: {
    command: 'node e2e/static-server.mjs',
    url: baseURL,
    reuseExistingServer: !process.env.CI,
    timeout: 30_000,
    stdout: 'pipe',
  },
});
