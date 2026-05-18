// One-off screenshot script. Captures two states of the Today (v2)
// page: (1) default (primary auto-expanded), and (2) after clicking a
// later compact card. Writes to ui/test-results/today-redesign*.png.
//
//   USE_MOCK=1 npm run dev -- --port 5173
//   node scripts/screenshot-today.mjs
//
// Safe to delete after the redesign session.

import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";

const TARGET = process.env.URL ?? "http://localhost:5173/today";
const OUT_DIR = new URL("../test-results/", import.meta.url).pathname;

await mkdir(OUT_DIR, { recursive: true });

const browser = await chromium.launch();
const ctx = await browser.newContext({
  viewport: { width: 1440, height: 1024 },
  deviceScaleFactor: 2,
});
const page = await ctx.newPage();
await page.goto(TARGET, { waitUntil: "load" });
await page.waitForSelector('[data-testid^="focused-review-"]', { timeout: 15_000 });
await page.waitForTimeout(500);

await page.screenshot({ path: `${OUT_DIR}today-redesign.png`, fullPage: true });
console.log(`Saved: ${OUT_DIR}today-redesign.png`);

// Capture the "secondary state" — collapse the primary, click the
// third compact card if available so we can see compact cards above
// and below an expanded card.
const compactCards = await page.locator('[data-testid^="compact-card-"]').all();
if (compactCards.length >= 3) {
  const target = compactCards[2];
  const id = await target.getAttribute("data-testid");
  console.log(`Clicking ${id} to switch focus`);
  await page.click(`[data-testid^="compact-row-"]:nth-of-type(1)`).catch(() => {});
  await target.locator(`[data-testid^="compact-row-"]`).click();
  await page.waitForTimeout(500);
  await page.screenshot({
    path: `${OUT_DIR}today-redesign-mid.png`,
    fullPage: true,
  });
  console.log(`Saved: ${OUT_DIR}today-redesign-mid.png`);
}

await browser.close();
