import { test, expect } from "@playwright/test";

// Drives the Fyralis Today page against the in-process mock backend
// (USE_MOCK=1 — see playwright.config.ts). Smoke-tests the surfaces
// called out by FYRALIS_TODAY_SPEC.md §11 acceptance criteria.

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.locator(".page-h1").waitFor();
});

test("renders sidebar, signal strip, page header, seven cards, ask zone", async ({
  page,
}) => {
  await expect(page.locator(".brand-wordmark")).toContainText("Fyralis");
  await expect(page.locator(".page-h1")).toContainText("Saturday, April 25");
  await expect(page.locator(".pill")).toContainText("tense");

  // Four signal-strip metrics
  const labels = await page.locator(".signal-label").allTextContents();
  expect(labels.map((s) => s.trim())).toEqual(
    expect.arrayContaining(["ARR", "Runway", "Commitments", "My calibration"])
  );

  // Seven cards
  await expect(page.locator("article.card")).toHaveCount(7);

  // Ask zone visible
  await expect(
    page.getByPlaceholder(/What did we decide about pricing/)
  ).toBeVisible();
});

test("J/K navigates between cards and Enter expands one", async ({ page }) => {
  // First card is auto-focused after the 100ms delay.
  await page.waitForTimeout(150);
  const cards = page.locator("article.card");
  await expect(cards.nth(0)).toHaveClass(/focused/);
  await page.keyboard.press("j");
  await expect(cards.nth(1)).toHaveClass(/focused/);
  await page.keyboard.press("k");
  await expect(cards.nth(0)).toHaveClass(/focused/);
  await page.keyboard.press("Enter");
  await expect(cards.nth(0)).toHaveClass(/expanded/);
});

test("filter strip restricts visible cards (1/2/3)", async ({ page }) => {
  await page.keyboard.press("3");
  // Only strategic cards render
  await expect(
    page.locator('article.card[data-kind="strategic"]')
  ).toHaveCount(3);
  await expect(
    page.locator('article.card[data-kind="operational"]')
  ).toHaveCount(0);
  await page.keyboard.press("1");
  await expect(page.locator("article.card")).toHaveCount(7);
});

test("? opens shortcuts overlay; Esc closes", async ({ page }) => {
  await page.keyboard.press("?");
  await expect(page.getByText(/Keyboard shortcuts/)).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByText(/Keyboard shortcuts/)).toHaveCount(0);
});

test("/ focuses the ask field", async ({ page }) => {
  const ask = page.getByPlaceholder(/What did we decide about pricing/);
  await ask.evaluate((el: HTMLInputElement) => el.blur());
  await page.keyboard.press("/");
  await expect(ask).toBeFocused();
});

test("acting on a card sweeps it away", async ({ page }) => {
  await page.waitForTimeout(150);
  const before = await page.locator("article.card").count();
  await page.keyboard.press("a");
  await page.waitForTimeout(750);
  const after = await page.locator("article.card").count();
  expect(after).toBe(before - 1);
});

test("routed coda toggles open/closed", async ({ page }) => {
  const coda = page.locator(".routed-coda");
  await expect(coda).toBeVisible();
  await coda.click();
  await expect(coda).toHaveClass(/expanded/);
  await coda.click();
  await expect(coda).not.toHaveClass(/expanded/);
});

test("375px mobile viewport: cockpit collapses to one column", async ({
  page,
}) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.reload();
  await page.locator(".page-h1").waitFor();
  const cols = await page
    .locator(".cockpit")
    .evaluate((el) => getComputedStyle(el).gridTemplateColumns);
  expect(cols.split(" ").length).toBe(1);
});
