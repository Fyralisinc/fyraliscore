import { test, expect } from "@playwright/test";

// These specs drive the CEO view against the in-process mock backend
// (USE_MOCK=1 — see playwright.config.ts). They cover the three surfaces
// the exit gate calls out: greeting + query grid + cards, plus the
// interaction patterns from design doc §11.

test.beforeEach(async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("greeting").waitFor();
});

test("no console errors on load", async ({ page }) => {
  const errors: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") errors.push(msg.text());
  });
  await page.goto("/", { waitUntil: "networkidle" });
  await page.getByTestId("greeting").waitFor();
  // Font preconnect warnings from some browsers show up as warnings, not
  // errors. Anything that makes it through here is a real problem.
  expect(errors).toEqual([]);
});

test("renders greeting, query grid (6 chips), three cards, close line", async ({
  page,
}) => {
  await expect(page.getByText("Company OS")).toBeVisible();
  await expect(page.locator(".view-label")).toContainText("ceo · rachin");
  await expect(page.getByTestId("greeting")).toContainText(
    "structurally unsafe"
  );
  const chips = page.locator('[data-testid="query-grid"] button.q');
  await expect(chips).toHaveCount(6);
  await expect(page.getByTestId("card-observation")).toBeVisible();
  await expect(page.getByTestId("card-decision")).toBeVisible();
  await expect(page.getByTestId("card-question")).toBeVisible();
  await expect(page.getByTestId("close-line")).toContainText(
    "That's the signal"
  );
});

test("expanding a card opens the drawer, Esc closes it", async ({ page }) => {
  const card = page.getByTestId("card-observation");
  await card.click();
  await expect(card).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(card).toHaveAttribute("aria-expanded", "false");
});

test("tapping a query chip renders a turn", async ({ page }) => {
  await page
    .getByRole("button", { name: /Show me why Acme became unsafe/ })
    .click();
  await expect(page.getByTestId("turn")).toBeVisible();
  await expect(page.getByTestId("turn")).toContainText("Show me why Acme");
});

test("tapping a card verb dispatches a query and collapses the card", async ({
  page,
}) => {
  const card = page.getByTestId("card-observation");
  await card.click();
  await page.getByRole("button", { name: /Full reasoning/ }).click();
  await expect(page.getByTestId("turn")).toBeVisible();
  await expect(card).toHaveAttribute("aria-expanded", "false");
});

test("/ focuses the ground input, Enter submits", async ({ page }) => {
  await page.keyboard.press("/");
  await expect(page.locator("#ground-input")).toBeFocused();
  await page.keyboard.type("What did I miss yesterday?");
  await page.keyboard.press("Enter");
  await expect(page.getByTestId("turn")).toBeVisible();
});

test("Done on a turn dismisses it with the collapse animation", async ({
  page,
}) => {
  await page
    .getByRole("button", { name: /Show me why Acme became unsafe/ })
    .click();
  const turn = page.getByTestId("turn");
  await expect(turn).toBeVisible();
  await turn.getByRole("button", { name: "Done" }).click();
  await expect(turn).toHaveCount(0, { timeout: 2000 });
});

test("375px mobile viewport: grid collapses to one column", async ({
  page,
}) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.reload();
  await page.getByTestId("greeting").waitFor();
  const grid = page.getByTestId("query-grid");
  const cols = await grid.evaluate(
    (el) => getComputedStyle(el).gridTemplateColumns
  );
  // At this width the queries grid collapses to a single column.
  expect(cols.split(" ").length).toBe(1);
});
