import { test, expect } from "@playwright/test";

// Wave 3b/3c — visual smoke. Captures every primary route at three
// breakpoints into ui/test-results/visual-smoke/ for human review.

const routes = [
  { name: "today", path: "/" },
  { name: "model", path: "/model" },
  { name: "forecasts", path: "/forecasts" },
  { name: "ledger", path: "/ledger" },
];

const viewports = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "tablet", width: 768, height: 1024 },
  { name: "mobile", width: 375, height: 812 },
];

for (const vp of viewports) {
  for (const r of routes) {
    test(`${vp.name} :: ${r.name}`, async ({ page }) => {
      await page.setViewportSize({ width: vp.width, height: vp.height });
      await page.goto(r.path);
      await page.waitForLoadState("networkidle");
      // Let any post-mount animations settle.
      await page.waitForTimeout(400);
      await page.screenshot({
        path: `test-results/visual-smoke/${vp.name}-${r.name}.png`,
        fullPage: true,
      });
      // Sanity: page rendered something below the shell. The sidebar
      // is always present; we want the main column to have content.
      await expect(page.locator("main")).toBeVisible();
    });
  }
}
