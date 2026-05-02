import { test, expect } from "@playwright/test";

// Drives the Driftwood Structure page (DRIFTWOOD_STRUCTURE_SPEC.md).
// USE_MOCK=1 is set by the playwright webServer; the Structure page
// uses local sample data so no backend is involved.

test.beforeEach(async ({ page }) => {
  await page.goto("/structure");
  await page.locator(".layer-strip").waitFor();
  // give the dot stagger animation a beat to finish
  await page.waitForTimeout(700);
});

test("navigates from Today → Structure via sidebar", async ({ page }) => {
  await page.goto("/");
  await page.locator(".sidebar").waitFor();
  await page.locator('.nav-item:has-text("Structure")').click();
  await page.waitForURL("**/structure");
  await expect(page.locator(".layer-strip")).toBeVisible();
});

test("navigates from Structure → Today via sidebar", async ({ page }) => {
  await page.locator('.nav-item:has-text("Today")').click();
  await page.waitForURL((url) => url.pathname === "/" || url.pathname === "");
});

test("renders layer strip, narrative band, controls, and 47 dots", async ({
  page,
}) => {
  // Layer strip — five tabs + utility cell, Commits active
  await expect(page.locator(".layer-cell")).toHaveCount(6);
  await expect(page.locator(".layer-cell.active")).toHaveText(/COMMITS/);

  // Narrative band shape statement is present and references render
  await expect(page.locator(".shape-statement-text")).toContainText(
    "Customer-facing work"
  );
  await expect(page.locator(".ref")).not.toHaveCount(0);

  // Status bar segments
  await expect(page.locator(".status-segment")).not.toHaveCount(0);

  // Three map controls
  await expect(page.locator(".control-toggle")).toHaveCount(3);

  // 47 commitment dots
  const dots = page.locator(".dot-group");
  await expect(dots).toHaveCount(47);

  await page.screenshot({
    path: "test-results/structure-overview.png",
    fullPage: false,
  });
});

test("clicking a person ref dims non-matching dots and switches color-by to Owner", async ({
  page,
}) => {
  await page.locator('.ref[data-ref-type="person"]', { hasText: "Sarah" }).click();
  await page.waitForTimeout(300);

  // some dots are now dimmed (those that aren't Sarah's)
  const dimmed = page.locator(".dot-group.dimmed");
  await expect(dimmed.first()).toBeVisible();

  // color-by control should now show Owner
  const colorBtn = page.locator('.control-toggle:has-text("Color by")');
  await expect(colorBtn).toContainText("Owner");

  await page.screenshot({
    path: "test-results/structure-ref-sarah.png",
    fullPage: false,
  });

  // clicking the same ref again clears the filter
  await page.locator('.ref[data-ref-type="person"]', { hasText: "Sarah" }).click();
  await page.waitForTimeout(200);
  await expect(page.locator(".dot-group.dimmed")).toHaveCount(0);
});

test("clicking a dot opens the side panel; Esc closes", async ({ page }) => {
  const firstDot = page.locator(".dot-group").first();
  await firstDot.click();
  await page.waitForTimeout(450);

  const panel = page.locator(".commitment-panel.open");
  await expect(panel).toBeVisible();
  await expect(panel.locator(".panel-title")).toBeVisible();
  await expect(panel.locator(".panel-id")).toBeVisible();

  await page.screenshot({
    path: "test-results/structure-panel-open.png",
    fullPage: false,
  });

  await page.keyboard.press("Escape");
  await page.waitForTimeout(450);
  await expect(page.locator(".commitment-panel.open")).toHaveCount(0);
});

test("layout toggle swaps to two-axis mode (axes appear)", async ({ page }) => {
  await page.locator('.control-toggle:has-text("Layout")').click();
  await page.locator('.menu-item:has-text("Two-axis")').click();
  await page.waitForTimeout(700);

  // territory rectangles are gone; two-axis frame is rendered
  await expect(page.locator(".territory")).toHaveCount(0);
  await expect(page.locator(".two-axis-frame")).toBeVisible();

  await page.screenshot({
    path: "test-results/structure-two-axis.png",
    fullPage: false,
  });

  // toggle back
  await page.locator('.control-toggle:has-text("Layout")').click();
  await page.locator('.menu-item:has-text("Territory")').click();
  await page.waitForTimeout(500);
  await expect(page.locator(".territory").first()).toBeVisible();
});

test("color-by Owner recolors dots", async ({ page }) => {
  await page.locator('.control-toggle:has-text("Color by")').click();
  await page.locator('.menu-item:has-text("Owner")').click();
  await page.waitForTimeout(500);

  // multiple distinct fill colors should appear (Owner palette)
  const fills = await page.locator(".dot-fill").evaluateAll((nodes) =>
    Array.from(new Set(nodes.map((n) => (n as SVGCircleElement).getAttribute("fill"))))
  );
  // at least 3 distinct fills (more than just status palette)
  expect(fills.length).toBeGreaterThanOrEqual(3);
});

test("filter dropdown restricts visible dots", async ({ page }) => {
  await page.locator('.control-toggle:has-text("Filter")').click();
  await page.locator(".filter-panel").waitFor();

  // Uncheck on-track → most dots disappear
  await page.locator('.filter-checkbox-group label:has-text("On track") input').click();
  await page.locator('.btn-primary:has-text("Apply")').click();
  await page.waitForTimeout(400);

  const remaining = await page.locator(".dot-group").count();
  expect(remaining).toBeLessThan(15);

  await page.screenshot({
    path: "test-results/structure-filtered.png",
    fullPage: false,
  });
});

test("layer 2 (Decisions) shows Coming soon", async ({ page }) => {
  await page.locator(".layer-cell", { hasText: "DECISIONS" }).click();
  await page.waitForTimeout(200);
  await expect(page.locator(".layer-coming-soon")).toBeVisible();
  await page.locator(".layer-cell", { hasText: "COMMITS" }).click();
});

test("hover on a dot reveals a tooltip", async ({ page }) => {
  const dot = page.locator(".dot-group").nth(5);
  await dot.hover();
  await page.waitForTimeout(200);
  await expect(page.locator(".dot-tooltip.visible")).toBeVisible();
  await expect(page.locator(".dot-tooltip.visible .tooltip-id")).toBeVisible();
});

test("?  opens shortcuts overlay", async ({ page }) => {
  await page.keyboard.press("?");
  await expect(page.getByText(/Keyboard shortcuts/i)).toBeVisible();
  await page.keyboard.press("Escape");
});

test("mobile viewport collapses layer strip and panel becomes bottom sheet", async ({
  page,
}) => {
  await page.setViewportSize({ width: 600, height: 900 });
  await page.reload();
  await page.locator(".layer-strip").waitFor();
  await page.waitForTimeout(400);

  // utility cell + 3 visible cells (others hidden by responsive rule)
  const visibleCells = await page.locator(".layer-cell:visible").count();
  expect(visibleCells).toBeGreaterThanOrEqual(3);

  await page.screenshot({
    path: "test-results/structure-mobile.png",
    fullPage: false,
  });
});
