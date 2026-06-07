import { test, expect } from "@playwright/test";

test("evidence drawer opens and closes from focused review", async ({ page }) => {
  await page.goto("/today");

  await page.getByTestId("focused-review-evidence-link-delta-primary-001").click();
  await expect(page.getByTestId("evidence-drawer")).toBeVisible();
  await expect(page.getByText("Sync failure alert #441")).toBeVisible();

  await page.keyboard.press("Escape");
  await expect(page.getByTestId("evidence-drawer")).toHaveCount(0);
});
