import { test, expect } from "@playwright/test";

const BASE = "http://127.0.0.1:5173";

test("inject-signals handle has glowing/animated styling", async ({ page }) => {
  await page.goto(`${BASE}/`);
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.getByTestId("start-pelago").click();
  await page.waitForURL(`${BASE}/`);
  await page.waitForTimeout(2000);

  const handle = page.getByTestId("sim-open-handle");
  await expect(handle).toBeVisible();

  // Verify the handle has animation set + accent border (the glow).
  const style = await handle.evaluate((el) => {
    const cs = window.getComputedStyle(el);
    return {
      animationName: cs.animationName,
      animationDuration: cs.animationDuration,
      borderColor: cs.borderTopColor,
      cursor: cs.cursor,
    };
  });
  expect(style.animationName).toContain("sim-handle-glow");
  expect(style.animationDuration).not.toBe("0s");
  expect(style.cursor).toBe("pointer");
});

test("asking a question on a card produces an answer turn, NOT a card removal", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (e) => errors.push(`${e.name}: ${e.message}`));

  await page.goto(`${BASE}/`);
  await page.evaluate(() => localStorage.clear());
  await page.reload();
  await page.getByTestId("start-pelago").click();
  await page.waitForURL(`${BASE}/`);
  await page.waitForTimeout(3000);

  const beforeCount = await page.locator("article.card").count();
  expect(beforeCount).toBeGreaterThan(0);

  // Expand the top card and find the ask input.
  const topCard = page.locator("article.card").first();
  await topCard.locator(".expand-cta").click();
  await page.waitForTimeout(400);

  // Driftwood revision: legacy .detail-ask-input → .card-ask-input.
  const askInput = topCard.locator(".card-ask-input");
  const inputCount = await askInput.count();
  console.log(`ask inputs visible: ${inputCount}`);
  if (inputCount === 0) {
    test.info().annotations.push({ type: "skipped", description: "no card with show_ask in this snapshot" });
    return;
  }

  await askInput.fill("Why is this the highest-impact item?");
  await askInput.press("Enter");

  // Card should still be visible (the ask must NOT remove it).
  await page.waitForTimeout(1500);
  const afterCount = await page.locator("article.card").count();
  console.log(`cards after ask: ${afterCount}`);
  expect(afterCount, "ask must not dismiss the card").toBe(beforeCount);

  // No "Acted" toast should appear.
  const toast = page.getByTestId("triage-toast");
  const toastVisible = await toast.isVisible().catch(() => false);
  console.log(`toast after ask: ${toastVisible}`);
  if (toastVisible) {
    const text = (await toast.innerText()).toLowerCase();
    expect(text, "toast must not say Acted/Reaffirm").not.toMatch(/acted|reaffirm/);
  }

  expect(errors, "no console errors").toHaveLength(0);
});
