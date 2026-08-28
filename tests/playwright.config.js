import { defineConfig, devices } from '@playwright/test';

// The @subset suite (run against okteto.shared.yaml) doesn't use BASE_URL at
// all — it hits catalog directly over in-cluster DNS, since shared has no
// ingress-exposed frontend. The @full suite (run against a personal
// namespace, via okteto.personal.yaml) uses it. Note this topology has no
// blanket baggage header here (unlike the single-service-diverted demo):
// frontend/api/rent-api are always fully personal, never split between
// shared/personal, so there's no HTTP-level divert routing to steer with a
// header. The baggage header only matters for Kafka message tagging in the
// rent/return flow, set per-request where that's tested, not globally here.
const BASE_URL = `https://movies-${process.env.OKTETO_NAMESPACE}.${process.env.OKTETO_DOMAIN}`;

/**
 * @see https://playwright.dev/docs/test-configuration
 */
const config = defineConfig({
  testDir: './tests',
  /* Run tests in files in parallel */
  fullyParallel: true,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* Retry on CI only */
  retries: process.env.CI ? 0 : 0,
  /* Opt out of parallel tests on CI. */
  workers: process.env.CI ? 1 : undefined,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: [['list'], ['html', { open: 'never' }]],
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    baseURL: BASE_URL,

    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: 'on-first-retry',
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});

export default config;