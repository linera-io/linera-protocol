import defineConfig from '../playwright.config';

export default defineConfig({
  port: 5173,
  applicationId: process.env.LINERA_COUNTER_APPLICATION_ID,
});
