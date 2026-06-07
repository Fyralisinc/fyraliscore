import { test, expect } from "@playwright/test";

test("accepting the focused change calls the Today apply endpoint", async ({ page }) => {
  let applied = false;
  await page.route("**/api/today/deltas/delta-primary-001/apply", async (route) => {
    applied = true;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        status: "applied",
        deltaId: "delta-primary-001",
        resultMessage: "Applied in test",
        ledgerEventId: "ledger-test",
      }),
    });
  });

  await page.goto("/today");
  await page.getByTestId("focused-accept-delta-primary-001").click();

  await expect.poll(() => applied).toBe(true);
  await expect(page.getByTestId("today-toast")).toContainText("Applied in test");
});
