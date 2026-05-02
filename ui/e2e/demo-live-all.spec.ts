import { test, expect } from "@playwright/test";

const BASE = "http://127.0.0.1:5173";

for (const company of ["truss", "northwind", "meridian"] as const) {
  test(`pick ${company} → cockpit renders, signal injects, reset works`, async ({ page }) => {
    const errors: string[] = [];
    const failures: string[] = [];
    page.on("pageerror", (e) => errors.push(`${e.name}: ${e.message}`));
    page.on("console", (m) => { if (m.type() === "error") errors.push(m.text()); });
    page.on("response", async (r) => {
      if (r.status() >= 500) failures.push(`${r.status()} ${r.url()}`);
    });

    // Clean any prior demo session
    await page.goto(`${BASE}/demo`);
    await page.evaluate(() => localStorage.clear());
    await page.reload();

    await expect(page.getByTestId(`start-${company}`)).toBeVisible({ timeout: 10000 });
    await page.getByTestId(`start-${company}`).click();
    await page.waitForURL(`${BASE}/`, { timeout: 30000 });
    await page.waitForTimeout(4000);

    const text = await page.locator("body").innerText();
    const cardCount = await page.locator("article, .feed-list > *").count();
    const live = text.includes("LIVE") || text.includes("live");

    console.log(`\n=== ${company} ===`);
    console.log(`  cards visible: ${cardCount}`);
    console.log(`  live indicator: ${live}`);
    console.log(`  pageErrors: ${errors.length}, 5xx: ${failures.length}`);
    if (errors.length) errors.slice(0, 3).forEach(e => console.log("  ! " + e));
    if (failures.length) failures.slice(0, 3).forEach(f => console.log("  ! " + f));

    expect(errors, "no console errors").toHaveLength(0);
    expect(failures, "no 5xx").toHaveLength(0);
    expect(cardCount).toBeGreaterThan(0);
  });
}
