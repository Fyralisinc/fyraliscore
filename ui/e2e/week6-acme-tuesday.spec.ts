/**
 * Week-6 demo e2e — boots the UI against the captured
 * `acme_tuesday_home.json` dump and verifies the three surfaces
 * render the live substrate output. Independent of the scenario
 * capture pipeline: Playwright intercepts /api/view/ceo/home with
 * the on-disk JSON so the test is hermetic once the fixture exists.
 *
 * Captures screenshots into ui/test-results/captures/week6-acme-tuesday/.
 *
 * Run:
 *   USE_MOCK=1 npm run test:e2e -- --grep @week6
 * (USE_MOCK=1 keeps the dev server alive; the route() below preempts
 * the mock handler for /home so the capture wins.)
 */
import { test, expect } from "@playwright/test";
import * as fs from "node:fs";
import * as path from "node:path";

const FIXTURE = path.join(
  process.cwd(),
  "..",
  "tests",
  "integration",
  "captures",
  "acme_tuesday_home.json",
);
const OUT = path.join(
  process.cwd(),
  "test-results",
  "captures",
  "week6-acme-tuesday",
);

test.describe("@week6 acme_tuesday — live substrate capture", () => {
  test.beforeEach(async ({ page }) => {
    if (!fs.existsSync(FIXTURE)) {
      test.skip(
        true,
        `fixture missing: ${FIXTURE} — run scripts/capture_scenario_home.py acme_tuesday`,
      );
    }
    const home = fs.readFileSync(FIXTURE, "utf-8");
    await page.route("**/api/view/ceo/home", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: home,
      });
    });
    await page.setViewportSize({ width: 1440, height: 900 });
  });

  test("greeting + cards + query grid render from captured substrate", async ({
    page,
  }) => {
    await page.goto("/");
    await page.getByTestId("greeting").waitFor();

    // Greeting is non-empty and references Acme (from the captured
    // substrate — specificity invariant).
    const greetingText = await page.getByTestId("greeting").textContent();
    expect(greetingText?.toLowerCase()).toContain("acme");
    // Voice rule guard — no exclamation marks on the surface.
    expect(greetingText).not.toContain("!");

    // Query grid — at least one chip exists.
    const chips = page.locator('[data-testid="query-grid"] button.q');
    const chipCount = await chips.count();
    expect(chipCount).toBeGreaterThan(0);

    // At least one card rendered.
    const cards = page.locator("[data-testid^='card-']");
    const cardCount = await cards.count();
    expect(cardCount).toBeGreaterThan(0);

    await page.waitForTimeout(400);
    fs.mkdirSync(OUT, { recursive: true });
    await page.screenshot({
      path: path.join(OUT, "01-home.png"),
      fullPage: true,
    });
  });

  test("expanded observation card captures evidence drawer", async ({
    page,
  }) => {
    await page.goto("/");
    await page.getByTestId("greeting").waitFor();
    const firstCard = page.locator("[data-testid^='card-']").first();
    await firstCard.click();
    await expect(firstCard).toHaveAttribute("aria-expanded", "true");
    await page.waitForTimeout(400);
    fs.mkdirSync(OUT, { recursive: true });
    await page.screenshot({
      path: path.join(OUT, "02-card-expanded.png"),
      fullPage: true,
    });
  });

  test("ask turn renders below the close line", async ({ page }) => {
    // Stub the ask response too so this test is hermetic.
    await page.route("**/api/view/ceo/ask", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          turn_id: "week6-turn",
          query_echo: "Show me why Acme became unsafe",
          response_html:
            "<div class=\"t-body\">Model m-2841 carried a falsifier that fired Saturday.</div>",
          verbs: [
            { id: "followup", label: "Follow up" },
            { id: "save", label: "Save" },
            { id: "done", label: "Done" },
          ],
          computed_at: "2026-04-22T00:00:00Z",
          latency_ms: 120,
        }),
      });
    });
    await page.goto("/");
    await page.getByTestId("greeting").waitFor();

    // Type a query and submit.
    await page.keyboard.press("/");
    await page.keyboard.type("Show me why Acme became unsafe");
    await page.keyboard.press("Enter");
    await page.getByTestId("turn").waitFor();
    await page.waitForTimeout(300);
    fs.mkdirSync(OUT, { recursive: true });
    await page.screenshot({
      path: path.join(OUT, "03-turn.png"),
      fullPage: true,
    });
  });

  test("mobile 375px responsive layout holds", async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto("/");
    await page.getByTestId("greeting").waitFor();

    // Query grid should collapse to one column at 375px (design system).
    const grid = page.getByTestId("query-grid");
    const cols = await grid.evaluate(
      (el) => getComputedStyle(el).gridTemplateColumns,
    );
    expect(cols.split(" ").length).toBe(1);

    // Top bar + greeting + at least one card still visible.
    await expect(page.getByTestId("greeting")).toBeVisible();
    const cards = page.locator("[data-testid^='card-']");
    const cardCount = await cards.count();
    expect(cardCount).toBeGreaterThan(0);

    await page.waitForTimeout(400);
    fs.mkdirSync(OUT, { recursive: true });
    await page.screenshot({
      path: path.join(OUT, "04-mobile-375.png"),
      fullPage: true,
    });
  });
});
