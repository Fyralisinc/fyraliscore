import { test, expect } from "@playwright/test";

// Driftwood Today card revision — exercises the probe-driven expanded
// card model (DRIFTWOOD_TODAY_CARD_REVISION.md §14 acceptance).
//
// Drives the in-process mock backend (USE_MOCK=1, see
// playwright.config.ts). The mock decorates fixture cards with probe
// chips + <probe> markup and serves /api/v1/cards/{id}/probe with
// canned responses.

test.beforeEach(async ({ page, request }) => {
  // Reset mock conversation state so probe-id dedupe and exchange
  // ordering don't leak between tests.
  await request.post("/api/__test__/reset-conversations").catch(() => {});
  await page.goto("/");
  await page.locator(".page-h1").waitFor();
});

test("expanded card renders probe chips and Ask field, hides legacy detail sections", async ({
  page,
}) => {
  const card = page.locator("article.card").first();
  await card.click();
  await expect(card).toHaveClass(/expanded/);

  // Probe row label appears with chips.
  await expect(card.locator(".probe-row-label")).toContainText(
    /What do you want to understand/i
  );
  await expect(card.locator(".probe-chip")).not.toHaveCount(0);

  // In-card Ask field present, with the spec placeholder.
  await expect(
    card.locator(".card-ask-input")
  ).toHaveAttribute("placeholder", /Or ask anything/);

  // Legacy push sections are not pre-rendered.
  await expect(card.locator(".detail-label")).toHaveCount(0);
  await expect(card.locator(".paths .path")).toHaveCount(0);
});

test("clicking a probe chip creates an exchange and removes the chip", async ({
  page,
}) => {
  const card = page.locator("article.card").first();
  await card.click();
  await expect(card).toHaveClass(/expanded/);

  const firstChip = card.locator(".probe-chip").first();
  const chipText = (await firstChip.textContent())?.trim() ?? "";
  await firstChip.click();

  // Exchange appears with the probe header reflecting the chip.
  const exchange = card.locator(".exchange").first();
  await exchange.waitFor();
  await expect(exchange.locator(".probe-action")).toContainText("You probed");
  await expect(exchange.locator(".probe-text")).toContainText(chipText);

  // The chip should be gone from the row (used chips don't reappear).
  await expect(
    card.locator(".probe-chip", { hasText: chipText })
  ).toHaveCount(0);

  // Response body rendered with HTML.
  await expect(exchange.locator(".exchange-response")).not.toBeEmpty();
});

test("clicking a probable phrase opens an exchange and marks the phrase probed", async ({
  page,
}) => {
  const card = page.locator("article.card").first();
  await card.click();

  const phrase = card.locator(".card-detail-inner [data-probe-id]").first();
  await phrase.click();

  const exchange = card.locator(".exchange").first();
  await exchange.waitFor();
  await expect(exchange.locator(".probe-action")).toContainText("You clicked");

  // After the response settles, the phrase carries the .probed class.
  await expect(phrase).toHaveClass(/probed/);
});

test("Ask field submission appends an exchange with the user's question", async ({
  page,
}) => {
  const card = page.locator("article.card").first();
  await card.click();

  const ask = card.locator(".card-ask-input");
  await ask.fill("What if I deferred this for two weeks?");
  await ask.press("Enter");

  const exchange = card.locator(".exchange").first();
  await exchange.waitFor();
  await expect(exchange.locator(".probe-action")).toContainText("You asked");
  await expect(exchange.locator(".probe-text")).toContainText(
    "deferred this for two weeks"
  );
  // Field clears after submit.
  await expect(ask).toHaveValue("");
});

test("sticky footer keeps action buttons reachable after probing", async ({
  page,
}) => {
  const card = page.locator("article.card").first();
  await card.click();

  const footer = card.locator(".card-footer.sticky");
  await expect(footer).toBeVisible();
  // Primary action (Act) still present.
  await expect(footer.locator(".card-action.primary")).toBeVisible();

  // Probe twice to grow the conversation, then re-check footer.
  await card.locator(".probe-chip").nth(0).click();
  await card.locator(".exchange").first().waitFor();
  await expect(footer.locator(".card-action.primary")).toBeVisible();
});
