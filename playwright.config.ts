import { defineConfig, devices } from '@playwright/test';

const PORT = Number(process.env.E2E_PORT ?? 4173);
const STATIC_URL = `http://127.0.0.1:${PORT}`;
/**
 * Set by `npm run test:e2e:live`: points the suite at the dev server, which proxies
 * `/api` to the BFF, so the browser talks to a real broker. Everything else runs
 * against the built bundle served as a static site, with no `/api` backend on the
 * origin — deliberately, because the app then takes its recorded-session path and the
 * suite is deterministic: no Kafka, no Docker, no network.
 */
const LIVE_URL = process.env.E2E_LIVE_URL;
const baseURL = LIVE_URL ?? STATIC_URL;

const desktop = (device: (typeof devices)[string]) => ({
  ...device,
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 2,
});

/**
 * Engines to run on. Chromium and WebKit are the two a laptop can always launch; CI adds
 * Firefox (`E2E_BROWSERS=chromium,firefox,webkit`), because some macOS setups refuse to
 * start the bundled unsigned Nightly at all — Gatekeeper kills it before it can read its
 * profile. SSE, the virtual list and the scroll anchoring are exactly the places where one
 * engine hides a difference, so the coverage is worth the extra minute.
 */
const ENGINES = (process.env.E2E_BROWSERS ?? 'chromium,webkit')
  .split(',')
  .map((name) => name.trim())
  .filter(Boolean);

const DEVICES: Record<string, (typeof devices)[string]> = {
  chromium: devices['Desktop Chrome'],
  firefox: devices['Desktop Firefox'],
  webkit: devices['Desktop Safari'],
};

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

  // The same suite on more than one engine.
  projects: ENGINES.map((name) => {
    const device = DEVICES[name];
    if (!device) throw new Error(`unknown engine in E2E_BROWSERS: ${name}`);
    return { name, use: desktop(device) };
  }),

  // Nothing to boot when the suite runs against the live stack.
  webServer: LIVE_URL
    ? undefined
    : {
        command: 'node e2e/static-server.mjs',
        url: STATIC_URL,
        reuseExistingServer: !process.env.CI,
        timeout: 30_000,
        stdout: 'pipe',
      },
});
