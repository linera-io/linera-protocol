import { test, expect } from '@playwright/test';

test.describe('Fungible App', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('send button is initially disabled', async ({ page }) => {
    const submitButton = page.locator('form#transfer input[type="submit"]');
    await expect(submitButton).toBeDisabled();
  });

  test('copy account button is clickable', async ({ page }) => {
    const copyButton = page.locator('#copy-account');
    await expect(copyButton).toBeEnabled();
  });
});

test.describe('Fungible App - Integration', () => {
  test.skip(
    !process.env.LINERA_APPLICATION_ID,
    'Requires LINERA_APPLICATION_ID to be set'
  );

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('initializes wallet and displays owner', async ({ page }) => {
    const owner = page.locator('#owner');
    await expect(owner).not.toHaveText('requesting owner…', { timeout: 30000 });
    const ownerText = await owner.textContent();
    expect(ownerText).toMatch(/^0x[0-9a-f]{40,64}$/i);
  });

  test('claims chain and displays chain ID', async ({ page }) => {
    const chainId = page.locator('#chain-id');
    await expect(chainId).not.toHaveText('requesting a new microchain…', {
      timeout: 30000,
    });
    const chainIdText = await chainId.textContent();
    expect(chainIdText).toMatch(/^[0-9a-f]{64}$/i);
  });

  test('displays account ID after initialization', async ({ page }) => {
    const accountId = page.locator('#account-id');
    await expect(accountId).not.toHaveText('loading account…', {
      timeout: 30000,
    });
    const accountIdText = await accountId.textContent();
    expect(accountIdText).toMatch(/^0x[0-9a-f]+@[0-9a-f]{64}$/i);
  });

  test('enables send button after initialization', async ({ page }) => {
    const submitButton = page.locator('form#transfer input[type="submit"]');
    await expect(submitButton).toBeEnabled({ timeout: 30000 });
  });

  test('displays balance after initialization', async ({ page }) => {
    const balance = page.locator('#balance');
    await expect(balance).not.toHaveText('retrieving funds…', {
      timeout: 30000,
    });
    const balanceText = await balance.textContent();
    expect(balanceText).toMatch(/^\d+\.\d{2}$/);
  });

  test('displays ticker symbol', async ({ page }) => {
    const ticker = page.locator('#ticker-symbol');
    await expect(ticker).not.toBeEmpty({ timeout: 30000 });
  });

  test('shows error for invalid recipient', async ({ page }) => {
    const submitButton = page.locator('form#transfer input[type="submit"]');
    await expect(submitButton).toBeEnabled({ timeout: 30000 });

    await page.fill('input[name="recipient"]', 'invalid-address');
    await page.fill('input[name="amount"]', '1');
    await page.click('input[type="submit"]');

    const errors = page.locator('#errors li');
    await expect(errors).toBeVisible({ timeout: 10000 });
  });
});
