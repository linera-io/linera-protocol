import { defineConfig } from 'vite';
import path from 'path';

// https://vitejs.dev/config/
export default defineConfig({
  base: '',
  envPrefix: 'LINERA_',
  server: {
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
    fs: {
      allow: [
        path.resolve(__dirname, '../linera-web'),
      ],
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
