import { defineConfig } from 'vite';

// https://vitejs.dev/config/
export default defineConfig({
  base: '/hosted/counter/',
  server: {
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
  },
  build: {
    rollupOptions: {
      external: ['@linera/client'],
    },
  },
  esbuild: {
    supported: {
      'top-level-await': true,
    },
  },
  optimizeDeps: {
    exclude: [
      '@linera/client',
    ],
  },
})
