import { test, expect } from "@playwright/test";

test.describe("Ledger page", () => {
  test("loads the v2 ledger workspace", async ({ page }) => {
    await page.goto("/ledger");

    await expect(page.getByRole("heading", { level: 1, name: "Ledger" })).toBeVisible();
    await expect(page.getByText(/What changed, what resolved/i)).toBeVisible();
    await expect(page.getByRole("region", { name: "Memory river" })).toBeVisible();
    await expect(
      page.getByRole("heading", {
        level: 2,
        name: "Customer Reliability Escalation",
      }),
    ).toBeVisible();
  });

  test("switches between primary modes", async ({ page }) => {
    await page.goto("/ledger");

    await page.getByRole("tab", { name: "Accuracy" }).click();
    await expect(page.getByRole("heading", { level: 2, name: "Accuracy" })).toBeVisible();
    await expect(page.getByText("Calibrated accuracy")).toBeVisible();

    await page.getByRole("tab", { name: "Audit" }).click();
    await expect(page.getByRole("heading", { level: 2, name: "Audit log" })).toBeVisible();
  });

  test("deep-links to audit mode and filters rows", async ({ page }) => {
    await page.goto("/ledger?mode=audit");

    await page
      .getByPlaceholder("Search event, actor, or chain...")
      .fill("pricing");

    await expect(page.getByText(/Pricing Ownership Delay/).first()).toBeVisible();
    await expect(page.getByText(/Customer Reliability Escalation/)).not.toBeVisible();
  });
});
