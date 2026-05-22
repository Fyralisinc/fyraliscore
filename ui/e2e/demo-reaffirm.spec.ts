import { test, expect } from "@playwright/test";

const BASE = "http://127.0.0.1:5173";

test("clicking Reaffirm path on a card commits the action", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(`${e.name}: ${e.message}`));

  await page.goto(`${BASE}/demo`);
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.getByTestId("start-truss").click();
  await page.waitForURL(`${BASE}/`);
  await page.waitForTimeout(3500);

  // How many cards do we start with?
  const beforeCount = await page.locator("article.card").count();
  expect(beforeCount).toBeGreaterThan(0);
  console.log(`cards before reaffirm: ${beforeCount}`);

  // Capture the topmost card's id, expand it, then click "Reaffirm".
  const topCard = page.locator("article.card").first();
  const topCardId = await topCard.getAttribute("data-id");
  console.log(`reaffirming card: ${topCardId}`);

  // Expand. Click on the expand CTA inside the footer.
  await topCard.locator(".expand-cta").click();
  await page.waitForTimeout(500);

  // Find the Reaffirm path within this expanded card.
  const reaffirm = topCard.locator(".path", { hasText: "Reaffirm" }).first();
  const reaffirmCount = await reaffirm.count();
  console.log(`reaffirm buttons in topcard: ${reaffirmCount}`);
  if (reaffirmCount === 0) {
    // Some recs render as Adopt/Reject/Revisit instead of Reaffirm.
    const fallback = topCard.locator(".path").first();
    await expect(fallback).toBeVisible({ timeout: 5000 });
    await fallback.click();
  } else {
    await reaffirm.click();
  }

  // Toast should appear immediately (well before the 600ms dismissal animation).
  const toast = page.getByTestId("triage-toast");
  await expect(toast).toBeVisible({ timeout: 1500 });
  const toastText = await toast.innerText();
  console.log(`toast: ${toastText.slice(0, 200)}`);
  expect(toastText.toLowerCase()).toMatch(/reaffirm|acted/);

  // Card should disappear from the feed (animated dismiss).
  await page.waitForTimeout(2000);
  const afterCount = await page.locator("article.card").count();
  console.log(`cards after reaffirm: ${afterCount}`);

  expect(errors, "no console errors").toHaveLength(0);
  expect(afterCount, "topmost card was acted on and removed").toBeLessThan(beforeCount);
});
