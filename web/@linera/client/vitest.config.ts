import { defineConfig } from 'vitest/config'
export default defineConfig({
  envPrefix: ['LINERA_'],
  server: {
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
  },
  test: {
    browser: {
      provider: 'playwright',
      enabled: true,
      headless: true,
      // at least one instance is required
      instances: [
        { browser: 'chromium' },
      ],
    },
  }
})
