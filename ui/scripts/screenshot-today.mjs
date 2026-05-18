// One-off screenshot script for the combined Today layout.
//
//   USE_MOCK=1 npm run dev -- --port 5173    # or live backend
//   node scripts/screenshot-today.mjs
//
// Safe to delete after the redesign session.

import { chromium } from "playwright";
import { mkdir } from "node:fs/promises";

const TARGET = process.env.URL ?? "http://localhost:5173";
const OUT_DIR = new URL("../test-results/", import.meta.url).pathname;

await mkdir(OUT_DIR, { recursive: true });

const browser = await chromium.launch();
const ctx = await browser.newContext({
  viewport: { width: 1440, height: 1024 },
  deviceScaleFactor: 2,
});
const page = await ctx.newPage();

await page.goto(`${TARGET}/today`, { waitUntil: "load" });
await page.waitForSelector('[data-testid^="focused-review-"]', { timeout: 15_000 });
await page.waitForTimeout(500);
await page.screenshot({ path: `${OUT_DIR}today-combined.png`, fullPage: true });
console.log(`Saved: ${OUT_DIR}today-combined.png`);

await browser.close();
