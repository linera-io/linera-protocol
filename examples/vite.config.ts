import { defineConfig, loadEnv } from 'vite';
import path from 'path';

const checkEnvironment = required => {
  return {
    name: 'checkEnvironment',
    config(config, { mode }) {
      const available = {...process.env, ...loadEnv(mode, process.cwd(), config.envPrefix)};
      const missing = required.filter(env => !available[env]);
      if (missing.length > 0)
        throw new Error(`required environment variables missing: ${missing.toString()}`);
    }
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
