import { test } from "@playwright/test";
import path from "node:path";

// Produces the screenshots captured in BUILD-LOG as the visual exit-gate
// evidence. Covers the three surfaces Agent-UI owns:
//   1. Fresh home (greeting + grid + cards + close)
//   2. Observation card expanded (drawer + trace ledger)
//   3. Query turn rendered below the close line
// A 4th mobile capture at 375px demonstrates the responsive layout.
//
// Screenshots land in test-results/screenshots/ so BUILD-LOG can point
// at stable absolute paths.

const OUT = path.join(process.cwd(), "test-results", "screenshots");

test("home surface", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/");
  await page.getByTestId("greeting").waitFor();
  await page.waitForTimeout(500); // let animations settle
  await page.screenshot({
    path: path.join(OUT, "01-home.png"),
    fullPage: true,
  });
});

test("observation card expanded", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/");
  await page.getByTestId("greeting").waitFor();
  await page.getByTestId("card-observation").click();
  await page.waitForTimeout(500);
  await page.screenshot({
    path: path.join(OUT, "02-card-expanded.png"),
    fullPage: true,
  });
});

test("query turn rendered", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/");
  await page.getByTestId("greeting").waitFor();
  await page
    .getByRole("button", { name: /Show me why Acme became unsafe/ })
    .click();
  await page.getByTestId("turn").waitFor();
  await page.waitForTimeout(400);
  await page.screenshot({
    path: path.join(OUT, "03-turn.png"),
    fullPage: true,
  });
});

test("mobile 375px", async ({ page }) => {
  await page.setViewportSize({ width: 375, height: 812 });
  await page.goto("/");
  await page.getByTestId("greeting").waitFor();
  await page.waitForTimeout(500);
  await page.screenshot({
    path: path.join(OUT, "04-mobile-375.png"),
    fullPage: true,
  });
});
