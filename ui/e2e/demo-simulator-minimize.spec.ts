import { test, expect } from "@playwright/test";

const BASE = "http://127.0.0.1:5173";

test("signal simulator starts collapsed and minimize button works", async ({ page }) => {
  await page.goto(`${BASE}/demo`);
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.getByTestId("start-truss").click();
  await page.waitForURL(`${BASE}/`);
  await page.waitForTimeout(2500);

  // On first load the panel should NOT cover the cockpit — only the side
  // handle is visible.
  await expect(page.getByTestId("sim-open-handle")).toBeVisible();
  await expect(page.getByTestId("signal-simulator")).toBeHidden();

  // Open the panel via the handle.
  await page.getByTestId("sim-open-handle").click();
  await expect(page.getByTestId("signal-simulator")).toBeVisible();
  await expect(page.getByTestId("sim-minimize")).toBeVisible();

  // The Minimize button has a visible label, not just an X.
  const minimizeText = await page.getByTestId("sim-minimize").innerText();
  expect(minimizeText.toLowerCase()).toContain("minimize");

  // Click Minimize: panel collapses, handle returns.
  await page.getByTestId("sim-minimize").click();
  await expect(page.getByTestId("signal-simulator")).toBeHidden();
  await expect(page.getByTestId("sim-open-handle")).toBeVisible();
});
