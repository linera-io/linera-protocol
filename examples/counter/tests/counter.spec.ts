import { test, expect } from '@playwright/test';

test.describe('Counter App', () => {
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
    await expect(chainId).not.toHaveText('requesting chain…', { timeout: 30000 });
    const chainIdText = await chainId.textContent();
    expect(chainIdText).toMatch(/^[0-9a-f]{64}$/i);
  });

  test('increments counter when button is clicked', async ({ page }) => {
    const chainId = page.locator('#chain-id');
    await expect(chainId).not.toHaveText('requesting chain…', { timeout: 30000 });

    const countSpan = page.locator('#count');
    const initialCount = await countSpan.textContent() || '0';

    const button = page.locator('#increment-btn');
    await button.click();

    await expect(countSpan).not.toHaveText(initialCount!, { timeout: 10000 });
    const newCount = parseInt(await countSpan.textContent() || '0', 10);
    expect(newCount).toBeGreaterThan(parseInt(initialCount, 10));
  });
});
