// Deep visual interaction smoke test for the spec-aligned UI.
// Hits the live Vite + Gateway stack (localhost:5173 / :8000) and
// exercises every page + the cross-page interactions. Screenshots go
// to /tmp/fy-smoke/. Console errors and failed requests are reported
// to stdout.
//
// Run from ui/:  node scripts/spec-smoke.mjs

import { chromium } from "@playwright/test";
import { mkdirSync } from "node:fs";

const OUT = "/tmp/fy-smoke";
const BASE = process.env.FY_BASE ?? "http://localhost:5173";

mkdirSync(OUT, { recursive: true });

const issues = [];
function note(level, msg) {
  const line = `[${level}] ${msg}`;
  console.log(line);
  if (level === "FAIL" || level === "WARN") issues.push(line);
}

async function shot(page, name) {
  const path = `${OUT}/${name}.png`;
  await page.screenshot({ path, fullPage: true });
  console.log(`  saved ${path}`);
}

async function withPage(name, fn) {
  console.log(`\n=== ${name} ===`);
  await fn();
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 1,
  });
  const page = await ctx.newPage();

  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") {
      const txt = msg.text();
      // React hydration / source-map noise is not a test failure.
      if (txt.includes("Download the React DevTools")) return;
      consoleErrors.push(txt);
    }
  });

  const failedRequests = [];
  page.on("requestfailed", (req) => {
    failedRequests.push(`${req.method()} ${req.url()} :: ${req.failure()?.errorText}`);
  });

  // ── 1. Today ──────────────────────────────────────────────────────
  await withPage("Today", async () => {
    await page.goto(`${BASE}/`, { waitUntil: "networkidle" });

    // Page heading + compression sentence
    await page.waitForSelector("h1.fx-pageheader__title", { timeout: 8000 });
    const heading = await page.textContent("h1.fx-pageheader__title");
    if (heading?.trim() === "Today") {
      note("OK", `heading is "Today"`);
    } else {
      note("FAIL", `heading is "${heading}" (expected "Today")`);
    }

    // Requires authority section
    const authority = await page.locator("text=Requires your authority").count();
    note(authority > 0 ? "OK" : "FAIL", `Requires-authority section: ${authority}`);

    // At least one Decision Delta row with "Proposed Change" label
    const proposed = await page.locator("text=PROPOSED CHANGE").count();
    note(proposed > 0 ? "OK" : "FAIL", `PROPOSED CHANGE labels: ${proposed}`);

    // The escalation delta from the fixture
    const escalation = await page.locator("text=/Escalate customer risk/i").first();
    if (await escalation.count() > 0) {
      note("OK", "found escalation delta");
    } else {
      note("FAIL", "escalation delta missing");
    }

    await shot(page, "01-today-list");

    // Click the first card body (not action buttons) to open inspector
    await page.locator(".fx-card").first().click();
    await page.waitForSelector('[data-testid="delta-inspector"]', { timeout: 4000 });
    note("OK", "delta inspector opened");

    // Inspector spec sections (8 of 10 should be present minimum)
    const wantSections = [
      "Current state",
      "Proposed state",
      "Why this surfaced",
      "Confidence",
      "Evidence trace",
      "Source coverage",
      "What Fyralis may be missing",
      "If accepted",
    ];
    for (const label of wantSections) {
      const c = await page.locator(`text="${label}"`).count();
      note(c > 0 ? "OK" : "WARN", `inspector section "${label}": ${c}`);
    }

    await shot(page, "02-today-inspector");

    // Action buttons exist
    for (const action of ["Accept change", "Delegate", "This looks wrong", "Add context", "Snooze"]) {
      const c = await page.locator(`[data-testid="delta-inspector"] button:has-text("${action}")`).count();
      note(c > 0 ? "OK" : "WARN", `action "${action}": ${c}`);
    }

    // Fire Delegate optimistic mutation, verify toast
    await page.locator('[data-testid="delta-inspector"] button:has-text("Delegate")').first().click();
    await page.waitForTimeout(400);
    const toastDelegated = await page.locator("text=/Delegated/i").count();
    note(toastDelegated > 0 ? "OK" : "WARN", `delegate toast: ${toastDelegated}`);

    // Close inspector by × button
    await page.locator(".fx-inspector__close").first().click();
    await page.waitForTimeout(200);
    const inspectorClosed = await page.locator('[data-testid="delta-inspector"]').count();
    note(inspectorClosed === 0 ? "OK" : "FAIL", `inspector closed: ${inspectorClosed === 0}`);
  });

  // ── 2. Model ──────────────────────────────────────────────────────
  await withPage("Model", async () => {
    await page.goto(`${BASE}/model`, { waitUntil: "networkidle" });
    await page.waitForSelector("h1.fx-pageheader__title", { timeout: 6000 });

    const heading = await page.textContent("h1.fx-pageheader__title");
    note(heading?.trim() === "Model" ? "OK" : "FAIL", `heading: ${heading}`);

    // 8-lens bar
    for (const lens of ["Company", "Commitments", "Decisions", "Customers", "Teams", "Risks", "Owners", "Predictions"]) {
      const c = await page.locator(`button.fx-lensbar__btn:has-text("${lens}")`).count();
      note(c > 0 ? "OK" : "FAIL", `lens "${lens}": ${c}`);
    }

    // At least one Operating Thread row
    const thread = await page.locator(".fx-thread__title").first();
    const threadCount = await page.locator(".fx-thread__title").count();
    note(threadCount > 0 ? "OK" : "FAIL", `thread rows: ${threadCount}`);

    // Causal ribbon cells (5 cells per row)
    const ribbonCells = await page.locator(".fx-thread__ribbon-cell").count();
    note(ribbonCells >= 5 ? "OK" : "FAIL", `ribbon cells: ${ribbonCells}`);

    // Status pill must appear at least once
    const pills = await page.locator(".fx-pill").count();
    note(pills >= 1 ? "OK" : "FAIL", `status pills: ${pills}`);

    // Recent changes strip
    const recent = await page.locator(".fx-recent").count();
    note(recent >= 1 ? "OK" : "WARN", `recent changes strip: ${recent}`);

    await shot(page, "03-model-board");

    // Switch lens
    await page.locator('button.fx-lensbar__btn:has-text("Commitments")').click();
    await page.waitForTimeout(200);
    const commitmentsActive = await page.locator('button.fx-lensbar__btn--active:has-text("Commitments")').count();
    note(commitmentsActive > 0 ? "OK" : "FAIL", `Commitments lens active: ${commitmentsActive}`);
    await shot(page, "04-model-lens-commitments");

    // Back to Company, open thread inspector
    await page.locator('button.fx-lensbar__btn:has-text("Company")').click();
    await page.waitForTimeout(200);
    await thread.click();
    await page.waitForSelector('[data-testid="thread-inspector"]', { timeout: 4000 });
    note("OK", "thread inspector opened");

    for (const label of [
      "Current reading",
      "Why this matters",
      "Causal spine",
      "What changed",
      "Hidden structure",
      "Accountability",
      "Confidence & evidence",
      "Source coverage",
    ]) {
      const c = await page.locator(`text="${label}"`).count();
      note(c > 0 ? "OK" : "WARN", `thread inspector "${label}": ${c}`);
    }
    await shot(page, "05-model-inspector");

    // Trace cause overlay
    await page.locator('[data-testid="thread-inspector"] button:has-text("Trace cause")').first().click();
    await page.waitForTimeout(300);
    const overlay = await page.locator(".fx-trace-overlay").count();
    note(overlay > 0 ? "OK" : "FAIL", `trace overlay opened: ${overlay}`);
    if (overlay > 0) {
      const stepCount = await page.locator(".fx-trace__step").count();
      note(stepCount >= 3 ? "OK" : "WARN", `trace steps rendered: ${stepCount}`);
      await shot(page, "06-model-trace-overlay");
      await page.locator('.fx-trace-overlay button:has-text("Close")').click();
      await page.waitForTimeout(200);
    }
  });

  // ── 3. Forecasts ─────────────────────────────────────────────────
  await withPage("Forecasts", async () => {
    await page.goto(`${BASE}/forecasts`, { waitUntil: "networkidle" });
    await page.waitForSelector("h1.fx-pageheader__title", { timeout: 6000 });

    const heading = await page.textContent("h1.fx-pageheader__title");
    note(heading?.trim() === "Forecasts" ? "OK" : "FAIL", `heading: ${heading}`);

    // 5 tabs
    for (const tab of ["Active", "Resolving soon", "Interventions available", "Resolved", "Accuracy"]) {
      const c = await page.locator(`button.fx-lensbar__btn:has-text("${tab}")`).count();
      note(c > 0 ? "OK" : "FAIL", `tab "${tab}": ${c}`);
    }

    // Forecast rows
    const forecastRows = await page.locator(".fx-forecast-row").count();
    note(forecastRows > 0 ? "OK" : "FAIL", `forecast rows: ${forecastRows}`);

    // Beacon renewal forecast
    const beacon = await page.locator("text=/Beacon renewal risk/i").count();
    note(beacon > 0 ? "OK" : "FAIL", `beacon forecast: ${beacon}`);

    await shot(page, "07-forecasts-active");

    // Open inspector
    await page.locator(".fx-forecast-row").first().click();
    await page.waitForSelector('[data-testid="forecast-inspector"]', { timeout: 4000 });
    note("OK", "forecast inspector opened");
    for (const label of ["Confidence", "Leading indicators", "Evidence trace", "Source coverage"]) {
      const c = await page.locator(`text="${label}"`).count();
      note(c > 0 ? "OK" : "WARN", `forecast inspector "${label}": ${c}`);
    }
    await shot(page, "08-forecasts-inspector");

    // Switch to Accuracy tab
    await page.locator('button.fx-lensbar__btn:has-text("Accuracy")').click();
    await page.waitForTimeout(200);
    const calibration = await page.locator("text=/Calibration over time/i").count();
    note(calibration > 0 ? "OK" : "WARN", `accuracy tab body: ${calibration}`);
    await shot(page, "09-forecasts-accuracy");
  });

  // ── 4. Ledger ────────────────────────────────────────────────────
  await withPage("Ledger", async () => {
    await page.goto(`${BASE}/ledger`, { waitUntil: "networkidle" });
    await page.waitForSelector("h1.fx-pageheader__title", { timeout: 6000 });

    const heading = await page.textContent("h1.fx-pageheader__title");
    note(heading?.trim() === "Ledger" ? "OK" : "FAIL", `heading: ${heading}`);

    // Category filters
    for (const cat of ["All", "Model updates", "Decision actions", "Contestations", "Forecasts", "Commitments", "Observations"]) {
      const c = await page.locator(`.fx-pill:has-text("${cat}")`).count();
      note(c > 0 ? "OK" : "WARN", `category filter "${cat}": ${c}`);
    }

    // Event rows
    const eventRows = await page.locator(".fx-ledger__event").count();
    note(eventRows > 0 ? "OK" : "FAIL", `ledger event rows: ${eventRows}`);

    // Day header (grouping)
    const dayHeaders = await page.locator(".fx-ledger__day").count();
    note(dayHeaders > 0 ? "OK" : "WARN", `day headers: ${dayHeaders}`);

    await shot(page, "10-ledger-timeline");

    // Click first event → inspector
    await page.locator(".fx-ledger__event").first().click();
    await page.waitForSelector('[data-testid="ledger-inspector"]', { timeout: 4000 });
    note("OK", "ledger inspector opened");
    await shot(page, "11-ledger-inspector");

    // Filter to Contestations
    await page.locator('.fx-pill:has-text("Contestations")').click();
    await page.waitForTimeout(400);
    const contestEvents = await page.locator(".fx-ledger__event").count();
    note(contestEvents > 0 ? "OK" : "WARN", `contestation events after filter: ${contestEvents}`);
    await shot(page, "12-ledger-filtered-contest");
  });

  // ── 5. Command palette ──────────────────────────────────────────
  await withPage("Command palette", async () => {
    await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
    await page.keyboard.down("Meta");
    await page.keyboard.press("k");
    await page.keyboard.up("Meta");
    await page.waitForTimeout(300);
    let paletteOpen = await page.locator(".fx-palette").count();
    if (paletteOpen === 0) {
      // Some test envs treat Meta differently; try Control.
      await page.keyboard.down("Control");
      await page.keyboard.press("k");
      await page.keyboard.up("Control");
      await page.waitForTimeout(300);
      paletteOpen = await page.locator(".fx-palette").count();
    }
    note(paletteOpen > 0 ? "OK" : "FAIL", `⌘K palette opened: ${paletteOpen > 0}`);
    if (paletteOpen > 0) {
      await shot(page, "13-palette-initial");
      await page.fill(".fx-palette__input", "beacon");
      await page.waitForTimeout(250);
      const items = await page.locator(".fx-palette__item").count();
      note(items > 0 ? "OK" : "WARN", `palette results for "beacon": ${items}`);
      await shot(page, "14-palette-search-beacon");

      // Press Enter → should navigate
      await page.keyboard.press("Enter");
      await page.waitForTimeout(500);
      const closedAfterEnter = (await page.locator(".fx-palette").count()) === 0;
      note(closedAfterEnter ? "OK" : "WARN", `palette closed after Enter: ${closedAfterEnter}`);
    }
  });

  // ── 6. Cross-page navigation via sidebar ────────────────────────
  await withPage("Sidebar navigation", async () => {
    await page.goto(`${BASE}/`, { waitUntil: "networkidle" });
    const navTargets = [
      ["Model", "/model"],
      ["Forecasts", "/forecasts"],
      ["Ledger", "/ledger"],
      ["Today", "/"],
    ];
    for (const [label, path] of navTargets) {
      await page.locator(`.fx-sidebar a:has-text("${label}")`).first().click();
      await page.waitForTimeout(350);
      const onPath = new URL(page.url()).pathname === path;
      note(onPath ? "OK" : "FAIL", `sidebar nav "${label}" → ${page.url()}`);
    }
  });

  // ── Final report ────────────────────────────────────────────────
  console.log("\n========================================");
  console.log("Console errors:", consoleErrors.length);
  for (const e of consoleErrors.slice(0, 10)) console.log("  ", e);
  console.log("Failed requests:", failedRequests.length);
  for (const r of failedRequests.slice(0, 10)) console.log("  ", r);
  console.log("Issues:", issues.length);
  for (const i of issues) console.log("  ", i);
  console.log("Screenshots in:", OUT);

  await browser.close();

  if (issues.length > 0 || consoleErrors.length > 0) {
    process.exitCode = 1;
  }
}

main().catch((err) => {
  console.error("smoke failed:", err);
  process.exitCode = 2;
});
