import { defineConfig } from 'vite';
import path from 'path';

const checkEnvironment = envs => {
  const missing = envs.filter(env => !(env in process.env));
  if (missing.length > 0)
    throw new Error(`required environment variables missing: ${missing.toString()}`);

  return {
    name: 'checkEnvironment',
  };
}

// https://vitejs.dev/config/
export default defineConfig({
  base: '',
  envPrefix: 'LINERA_',
  plugins: [
    checkEnvironment([
      'LINERA_FAUCET_URL',
      'LINERA_APPLICATION_ID',
    ]),
  ],
  server: {
    headers: {
      'Cross-Origin-Embedder-Policy': 'require-corp',
      'Cross-Origin-Opener-Policy': 'same-origin',
    },
    fs: {
      allow: [
        path.resolve(__dirname, '.'),
        path.resolve(__dirname, '../web'),
      ],
    },
  },
  esbuild: {
    supported: {
      'top-level-await': true,
    },
  },
})
