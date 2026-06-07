import { test, expect } from "@playwright/test";

test("Acme deal review shows evidence-backed resolution detail", async ({ page }) => {
  await page.goto("/today");
  await page.getByTestId("rail-row-delta-other-acme-deal-reality").click();

  await expect(
    page.getByTestId("focused-review-delta-other-acme-deal-reality"),
  ).toBeVisible();
  const review = page.getByTestId("focused-review-delta-other-acme-deal-reality");
  await expect(review.getByText("Commit → Best Case")).toBeVisible();
  await expect(review.getByText(/buying group is not aligned/i)).toBeVisible();
  await expect(page.getByTestId("resolution-thread-rt-acme-deal-reality")).toBeVisible();
  await expect(review.getByText("Approve SSO-safe buyer language")).toBeVisible();
  await expect(review.getByText("CFO + security call appears this week.")).toBeVisible();
});
