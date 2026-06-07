import { test, expect } from "@playwright/test";

test("legacy /demo route boots Pelago and lands in Today", async ({ page }) => {
  const pageErrors: string[] = [];
  const failedResponses: string[] = [];

  page.on("pageerror", (err) => pageErrors.push(`${err.name}: ${err.message}`));
  page.on("response", (resp) => {
    if (resp.status() >= 500) failedResponses.push(`${resp.status()} ${resp.url()}`);
  });

  await page.goto("/demo");
  await expect(page).toHaveURL(/\/today$/);
  await expect(page.getByTestId("today-page")).toBeVisible();
  await expect(page.getByTestId("briefing-header")).toBeVisible();
  await expect(page.getByTestId("review-rail")).toBeVisible();
  await expect(page.getByTestId("focused-review-delta-primary-001")).toBeVisible();

  expect(pageErrors, "no uncaught page errors").toHaveLength(0);
  expect(failedResponses, "no 5xx responses").toHaveLength(0);
});
