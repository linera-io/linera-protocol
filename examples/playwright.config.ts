import * as playwright from '@playwright/test';
import { devices } from '@playwright/test';

export default function defineConfig({ port, applicationId }) {
  process.env.LINERA_APPLICATION_ID = applicationId;

  return playwright.defineConfig({
    testDir: '.',
    testMatch: '*/tests/**/*.spec.ts',
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 1 : undefined,
    reporter: 'html',
    use: {
      trace: 'on-first-retry',
      baseURL: `http://localhost:${port}`,
    },
    projects: [
      {
        name: 'chromium',
        use: { ...devices['Desktop Chrome'] },
      },
    ],
    webServer: [
      {
        command: `pnpm dev --port ${port}`,
        url: `http://localhost:${port}`,
        reuseExistingServer: !process.env.CI,
        env: {
          LINERA_APPLICATION_ID: applicationId,
        },
      },
    ],
  });
}
