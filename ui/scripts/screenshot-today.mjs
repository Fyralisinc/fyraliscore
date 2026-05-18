// Capture two states of the Today page: (1) at top of scroll (sticky
// action bar visible at viewport bottom) and (2) mid-scroll (action
// bar still visible). Writes to ui/test-results/.
//
//   USE_MOCK=1 npm run dev -- --port 5180
//   node scripts/screenshot-today.mjs
//
// Safe to delete after the redesign session.

import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";

const TARGET = process.env.URL ?? "http://localhost:5180";
const OUT_DIR = new URL("../test-results/", import.meta.url).pathname;

await mkdir(OUT_DIR, { recursive: true });

const browser = await chromium.launch();
const ctx = await browser.newContext({
  viewport: { width: 1440, height: 900 },
  deviceScaleFactor: 2,
});
const page = await ctx.newPage();

await page.goto(`${TARGET}/today`, { waitUntil: "load" });
await page.waitForSelector('[data-testid^="focused-review-"]', { timeout: 15_000 });
await page.waitForTimeout(500);

// Viewport screenshot (not fullPage) so the sticky action bar shows
// pinned to the viewport bottom — fullPage captures expand the
// viewport and break sticky positioning.
await page.screenshot({
  path: `${OUT_DIR}today-viewport-top.png`,
  fullPage: false,
});
console.log(`Saved: ${OUT_DIR}today-viewport-top.png`);

// Scroll down to verify the action bar stays sticky.
await page.evaluate(() => window.scrollTo({ top: 800, behavior: "instant" }));
await page.waitForTimeout(300);
await page.screenshot({
  path: `${OUT_DIR}today-viewport-mid.png`,
  fullPage: false,
});
console.log(`Saved: ${OUT_DIR}today-viewport-mid.png`);

await browser.close();
