import defineConfig from '../playwright.config';

export default defineConfig({
  port: 5174,
  applicationId: process.env.LINERA_FUNGIBLE_APPLICATION_ID,
});
