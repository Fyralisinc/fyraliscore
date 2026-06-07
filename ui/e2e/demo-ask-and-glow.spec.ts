import { test, expect } from "@playwright/test";

test("focused review Ask strip answers without removing the card", async ({ page }) => {
  await page.goto("/today");
  await expect(page.getByTestId("focused-review-delta-primary-001")).toBeVisible();

  const beforeCount = await page.locator('[data-testid^="focused-review-"]').count();
  await page.getByTestId("ask-input-delta-primary-001").fill("Why now?");
  await page.getByTestId("ask-submit-delta-primary-001").click();

  await expect(page.getByTestId("ask-answer-delta-primary-001")).toBeVisible();
  await expect(page.locator('[data-testid^="focused-review-"]')).toHaveCount(beforeCount);
});

test("header Ask affordance is visible in the current demo surface", async ({ page }) => {
  await page.goto("/today");
  await expect(page.getByTestId("header-ask")).toBeVisible();
  await expect(page.getByTestId("header-ask")).toContainText("Ask Fyralis");
});
