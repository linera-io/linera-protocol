import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';

// The `@linera/client` WASM runs multi-threaded, which requires the page to be
// cross-origin isolated (SharedArrayBuffer). These headers enable that.
const crossOriginIsolation = {
  'Cross-Origin-Embedder-Policy': 'require-corp',
  'Cross-Origin-Opener-Policy': 'same-origin',
};

export default defineConfig({
  base: '',
  plugins: [react()],
  server: {
    headers: crossOriginIsolation,
    fs: {
      // Allow Vite to serve the workspace-linked `@linera/*` packages and their
      // prebuilt WASM from `../web`.
      allow: [path.resolve(__dirname, '.'), path.resolve(__dirname, '../web')],
    },
  },
  preview: { headers: crossOriginIsolation },
  esbuild: { supported: { 'top-level-await': true } },
  optimizeDeps: {
    esbuildOptions: { target: 'esnext', supported: { 'top-level-await': true } },
  },
  build: { target: 'esnext' },
});
