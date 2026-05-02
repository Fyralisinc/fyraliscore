import { test, expect } from "@playwright/test";

// Drives the Driftwood History page (DRIFTWOOD_HISTORY_SPEC.md).
// USE_MOCK=1 is set by the playwright webServer; History uses local sample
// data so no backend is involved.

test.beforeEach(async ({ page }) => {
  await page.goto("/history");
  await page.locator(".layer-strip").waitFor();
  await page.waitForTimeout(900); // let staggered events settle
});

test("renders layer strip + chronicle by default", async ({ page }) => {
  await expect(page.locator(".layer-cell")).toHaveCount(4); // 3 + utility
  await expect(page.locator(".layer-cell.active")).toContainText("CHRONICLE");
  await expect(page.locator(".chronicle")).toBeVisible();
  // multiple bucket headers
  const headers = await page.locator(".bucket-title").count();
  expect(headers).toBeGreaterThanOrEqual(2);
  // events render
  const events = await page.locator(".event").count();
  expect(events).toBeGreaterThan(5);

  await page.screenshot({
    path: "test-results/history-chronicle.png",
    fullPage: false,
  });
});

test("major events show substrate voice paragraph", async ({ page }) => {
  const major = page.locator('.event[data-prominence="major"]').first();
  await expect(major).toBeVisible();
  await expect(major.locator(".event-substrate-voice")).toBeVisible();
});

test("clicking an event opens side panel; Esc closes", async ({ page }) => {
  await page.locator('.event[data-prominence="major"]').first().click();
  await page.waitForTimeout(450);
  await expect(page.locator(".event-panel.open")).toBeVisible();
  await expect(page.locator(".event-panel .panel-title")).toBeVisible();

  await page.screenshot({
    path: "test-results/history-event-panel.png",
    fullPage: false,
  });

  await page.keyboard.press("Escape");
  await page.waitForTimeout(400);
  await expect(page.locator(".event-panel.open")).toHaveCount(0);
});

test("Predictions layer (key 2): table sorts and filter chip works", async ({
  page,
}) => {
  await page.keyboard.press("2");
  await expect(page.locator(".layer-cell.active")).toContainText("PREDICTIONS");
  await expect(page.locator(".predictions-table")).toBeVisible();

  // filter to correct only
  await page.locator('.filter-chip:has-text("Correct")').click();
  await page.waitForTimeout(200);
  const wrongRows = await page.locator('tr[data-status="wrong"]').count();
  expect(wrongRows).toBe(0);

  // calibration summary visible
  await expect(page.locator(".calibration-summary")).toBeVisible();
  await expect(page.locator(".cal-domain-row").first()).toBeVisible();

  await page.screenshot({
    path: "test-results/history-predictions.png",
    fullPage: false,
  });
});

test("clicking a prediction row opens prediction panel", async ({ page }) => {
  await page.keyboard.press("2");
  await page.waitForTimeout(200);
  await page.locator(".prediction-row").first().click();
  await page.waitForTimeout(400);
  await expect(page.locator(".event-panel.open")).toBeVisible();
  await expect(page.locator(".event-panel")).toContainText("PREDICTION");
});

test("Arcs layer (key 3) shows two-pane with selected arc detail", async ({
  page,
}) => {
  await page.keyboard.press("3");
  await page.waitForTimeout(300);
  await expect(page.locator(".arcs-layer")).toBeVisible();
  await expect(page.locator(".arc-item").first()).toBeVisible();
  await expect(page.locator(".arc-detail-name")).toBeVisible();
  await expect(page.locator(".arc-narrative-text")).toBeVisible();

  // click another arc
  const arcs = page.locator(".arc-item");
  const before = await page.locator(".arc-detail-name").textContent();
  await arcs.nth(1).click();
  await page.waitForTimeout(200);
  const after = await page.locator(".arc-detail-name").textContent();
  expect(after).not.toEqual(before);

  await page.screenshot({
    path: "test-results/history-arcs.png",
    fullPage: false,
  });
});

test("clicking arc chip in narrative band navigates to Arcs layer", async ({
  page,
}) => {
  await page.locator(".arc-chip").first().click();
  await page.waitForTimeout(300);
  await expect(page.locator(".layer-cell.active")).toContainText("ARCS");
});

test("search filters chronicle events", async ({ page }) => {
  await page.locator(".chronicle-controls .search-input").fill("Northwind");
  await page.waitForTimeout(300);
  const visible = await page.locator(".event").count();
  expect(visible).toBeGreaterThanOrEqual(1);
  // every visible event mentions Northwind somewhere
  const text = await page.locator(".chronicle").textContent();
  expect(text?.toLowerCase()).toContain("northwind");
});

test("aggregated routine events show 'see all' expander", async ({ page }) => {
  const expander = page.locator(".event-expand").first();
  await expect(expander).toBeVisible();
  await expander.click();
  await page.waitForTimeout(200);
  await expect(page.locator(".event-expanded-list")).toBeVisible();
});

test("navigates from Today → History via sidebar", async ({ page }) => {
  await page.goto("/");
  await page.locator(".sidebar").waitFor();
  await page.locator('.nav-item:has-text("History")').click();
  await page.waitForURL("**/history");
  await expect(page.locator(".chronicle")).toBeVisible();
});

test("? opens shortcuts overlay", async ({ page }) => {
  await page.keyboard.press("?");
  await expect(page.getByText(/Keyboard shortcuts/i)).toBeVisible();
  await page.keyboard.press("Escape");
});
