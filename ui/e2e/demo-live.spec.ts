/**
 * Live end-to-end demo flow against a real running stack.
 *
 * Assumes the gateway is at http://127.0.0.1:8000 and Vite dev is at
 * http://127.0.0.1:5173 with the /api proxy enabled. Captures console
 * errors + failed responses so we can see exactly what the browser
 * encounters when a user picks a company.
 */
import { test, expect } from "@playwright/test";

const BASE = "http://127.0.0.1:5173";

test("pick Truss → cockpit renders without errors", async ({ page }) => {
  const consoleErrors: string[] = [];
  const failedResponses: { url: string; status: number; body: string }[] = [];
  const pageErrors: string[] = [];

  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });
  page.on("pageerror", (err) => pageErrors.push(`${err.name}: ${err.message}`));
  page.on("response", async (resp) => {
    if (resp.status() >= 400) {
      let body = "";
      try { body = (await resp.text()).slice(0, 300); } catch {}
      failedResponses.push({ url: resp.url(), status: resp.status(), body });
    }
  });

  // 1. Picker page renders
  await page.goto(`${BASE}/`);
  await expect(page.locator("h1.demo-picker-title")).toBeVisible({ timeout: 10000 });

  // 2. Wait for companies to load and the Start button to appear
  const startBtn = page.getByTestId("start-pelago");
  await expect(startBtn).toBeVisible({ timeout: 10000 });

  // 3. Click Start
  await startBtn.click();

  // 4. We expect to land at / (the cockpit). Wait for either a card to
  //    render or 30s, whichever comes first.
  await page.waitForURL(`${BASE}/`, { timeout: 30000 });

  // Give the cockpit a few seconds to fetch /v1/today, /v1/recommendations,
  // and the simulator suggested-signals.
  await page.waitForTimeout(5000);

  // Capture what's actually on screen
  const visibleCards = await page.locator(".card, article.card").count();
  const offlineBanner = await page.locator(".offline-banner").isVisible().catch(() => false);
  const headlineText = await page.locator("body").innerText();

  console.log("==== POST-CLICK STATE ====");
  console.log(`URL: ${page.url()}`);
  console.log(`offlineBanner visible: ${offlineBanner}`);
  console.log(`visible card-shaped elements: ${visibleCards}`);
  console.log(`page text (first 500ch): ${headlineText.slice(0, 500)}`);
  console.log(`pageErrors: ${pageErrors.length}`);
  pageErrors.forEach((e) => console.log("  ! " + e));
  console.log(`consoleErrors: ${consoleErrors.length}`);
  consoleErrors.slice(0, 8).forEach((e) => console.log("  ! " + e));
  console.log(`failedResponses: ${failedResponses.length}`);
  failedResponses.slice(0, 8).forEach((r) =>
    console.log(`  ! ${r.status} ${r.url}\n     ${r.body}`)
  );

  // The actual assertion: at least one recommendation card should be
  // visible OR an empty-state should be visible (NOT a stack trace).
  const hasContent =
    visibleCards > 0 ||
    headlineText.includes("Nothing requires your attention") ||
    headlineText.toLowerCase().includes("loading");

  expect(hasContent, "cockpit should render content, not stack trace").toBe(true);
  expect(pageErrors, "no uncaught page errors").toHaveLength(0);
  expect(failedResponses.filter((r) => r.status >= 500), "no 5xx responses")
    .toHaveLength(0);
});
