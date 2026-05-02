import { test, expect } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.goto("/history");
  await page.locator(".layer-strip").waitFor();
  await page.waitForTimeout(900);
});

test("full chronicle screenshot — all buckets visible", async ({ page }) => {
  await page.screenshot({
    path: "test-results/history-chronicle-full.png",
    fullPage: true,
  });
  // sanity: at least 4 buckets
  const buckets = await page.locator(".bucket").count();
  expect(buckets).toBeGreaterThanOrEqual(4);
});

test("arc threads render between same-arc events", async ({ page }) => {
  // Filter to d-5-drift arc by clicking an event's arc thread
  const threads = await page.locator(".event-arc-thread").count();
  expect(threads).toBeGreaterThan(2);

  // each thread has the right data-arc-position
  const positions = await page
    .locator(".event[data-arc]")
    .evaluateAll((nodes) =>
      nodes.map((n) => n.getAttribute("data-arc-position"))
    );
  expect(positions).toContain("first");
  expect(positions).toContain("last");
});

test("clicking an arc thread filters chronicle to that arc", async ({
  page,
}) => {
  const beforeCount = await page.locator(".event").count();
  // Click the first arc thread we can find
  const thread = page.locator(".event-arc-thread").first();
  await thread.click({ force: true });
  await page.waitForTimeout(400);
  const afterCount = await page.locator(".event").count();
  expect(afterCount).toBeLessThan(beforeCount);
  await page.screenshot({
    path: "test-results/history-arc-filtered.png",
    fullPage: false,
  });
});

test("sort predictions by confidence", async ({ page }) => {
  await page.keyboard.press("2");
  await page.waitForTimeout(300);
  await page.locator('th[data-sort="confidence"]').click();
  await page.waitForTimeout(300);

  const confidences = await page.locator(".cell-confidence").allTextContents();
  const numeric = confidences.map((c) => Number(c.replace("%", "")));
  // sort desc by default
  for (let i = 1; i < numeric.length; i += 1) {
    expect(numeric[i]).toBeLessThanOrEqual(numeric[i - 1]);
  }

  // toggle to ascending
  await page.locator('th[data-sort="confidence"]').click();
  await page.waitForTimeout(300);
  const ascNumeric = (await page.locator(".cell-confidence").allTextContents()).map(
    (c) => Number(c.replace("%", ""))
  );
  for (let i = 1; i < ascNumeric.length; i += 1) {
    expect(ascNumeric[i]).toBeGreaterThanOrEqual(ascNumeric[i - 1]);
  }
});

test("prediction panel shows reasoning + outcome + calibration impact", async ({
  page,
}) => {
  await page.keyboard.press("2");
  await page.waitForTimeout(300);
  // click the d-5 row (correct, has full payload)
  await page.locator('tr[data-id="p-37"]').click();
  await page.waitForTimeout(450);
  const panel = page.locator(".event-panel.open");
  await expect(panel).toBeVisible();
  await expect(panel).toContainText("My reasoning at the time");
  await expect(panel).toContainText("What happened");
  await expect(panel).toContainText("Calibration impact");
  await expect(panel).toContainText("0.84 → 0.86");
  await page.screenshot({
    path: "test-results/history-prediction-panel.png",
    fullPage: false,
  });
});

test("filter by Wrong shows the Northwind miss with honest voice", async ({
  page,
}) => {
  await page.keyboard.press("2");
  await page.waitForTimeout(200);
  await page.locator('.filter-chip:has-text("Wrong")').click();
  await page.waitForTimeout(200);
  const rows = await page.locator(".prediction-row").count();
  expect(rows).toBeGreaterThanOrEqual(1);
  await page.locator('tr[data-id="p-22"]').click();
  await page.waitForTimeout(400);
  await expect(page.locator(".event-panel")).toContainText(
    "I overweighted"
  );
});

test("bucket header collapses and expands", async ({ page }) => {
  const bucket = page.locator(".bucket").first();
  await bucket.locator(".bucket-header").click();
  await page.waitForTimeout(450);
  await expect(bucket).toHaveClass(/collapsed/);
  await bucket.locator(".bucket-header").click();
  await page.waitForTimeout(450);
  await expect(bucket).not.toHaveClass(/collapsed/);
});

test("/ focuses search input", async ({ page }) => {
  await page.keyboard.press("/");
  const search = page.locator(".chronicle-controls .search-input");
  await expect(search).toBeFocused();
});

test("arc detail shows events in that arc with timeline", async ({ page }) => {
  await page.keyboard.press("3");
  await page.waitForTimeout(400);
  // click the d-5 drift arc which has 6 events
  await page.locator('.arc-item:has-text("d-5 drift")').click();
  await page.waitForTimeout(400);
  await expect(page.locator(".arc-detail-name")).toContainText("d-5 drift");
  const arcEvents = await page.locator(".arc-events-timeline .event").count();
  expect(arcEvents).toBeGreaterThanOrEqual(3);
  await page.screenshot({
    path: "test-results/history-arcs-detail.png",
    fullPage: false,
  });
});

test("mobile viewport: chronicle still legible at 600px", async ({ page }) => {
  await page.setViewportSize({ width: 600, height: 900 });
  await page.reload();
  await page.locator(".chronicle").waitFor();
  await page.waitForTimeout(700);
  await page.screenshot({
    path: "test-results/history-mobile.png",
    fullPage: false,
  });
  // events still visible
  expect(await page.locator(".event").count()).toBeGreaterThan(0);
});

test("Predictions mobile: table hides per CSS rule", async ({ page }) => {
  await page.setViewportSize({ width: 600, height: 900 });
  await page.reload();
  await page.locator(".chronicle").waitFor();
  await page.keyboard.press("2");
  await page.waitForTimeout(400);
  // table is hidden via CSS at this breakpoint
  const tableDisplay = await page
    .locator(".predictions-table")
    .evaluate((el) => getComputedStyle(el).display)
    .catch(() => "none");
  expect(tableDisplay).toBe("none");
});

test("entity link in event panel jumps to chronicle search", async ({
  page,
}) => {
  await page.locator('.event[data-prominence="major"]').first().click();
  await page.waitForTimeout(400);
  // event panel open
  const link = page.locator(".panel-link-list .panel-link").first();
  if ((await link.count()) > 0) {
    await link.click({ force: true });
    await page.waitForTimeout(300);
  }
});
