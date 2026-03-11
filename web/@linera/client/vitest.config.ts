import { defineConfig } from 'vitest/config'
export default defineConfig({
  envPrefix: ['LINERA_'],
  // Required for SharedArrayBuffer support in the browser, which the
  // wasm32-web-unknown target needs for shared memory and atomics.
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
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
