import { test, expect, Page } from "@playwright/test";
import {
  LEDGER_EVENTS_FIXTURE,
  LEDGER_SUMMARY_FIXTURE,
  SALESFORCE_ESCALATION_EVENT,
} from "../src/api/ledger-mock";

// Ledger e2e — drives /ledger against page.route() mocks so the suite
// is self-contained and never depends on the mock-server response shape
// (mock-server.ts intentionally serves the legacy History shape; the
// Ledger surface uses surface=ledger which mock-server doesn't yet
// handle — Phase 4 wires that in).

type ServeMode = "fixture" | "empty" | "error";

async function mockApi(page: Page, mode: ServeMode = "fixture") {
  await page.route(/\/api\/v1\/history(\?|\/|$)/, async (route) => {
    const url = new URL(route.request().url());
    if (mode === "error") {
      await route.fulfill({ status: 500, body: "server failure" });
      return;
    }
    if (url.pathname.endsWith("/summary")) {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(LEDGER_SUMMARY_FIXTURE),
      });
      return;
    }
    const typesRaw = url.searchParams.get("types");
    const types = typesRaw ? typesRaw.split(",") : null;
    const events =
      mode === "empty"
        ? []
        : types
          ? LEDGER_EVENTS_FIXTURE.filter((e) => types.includes(e.type))
          : LEDGER_EVENTS_FIXTURE;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ events, period: "30d", types }),
    });
  });
}

test.describe("Ledger page", () => {
  test("loads at /ledger with title and summary strip", async ({ page }) => {
    await mockApi(page);
    await page.goto("/ledger");
    await expect(
      page.getByRole("heading", { level: 1, name: "Ledger" })
    ).toBeVisible();
    await expect(
      page.getByText("The history of what changed, what was predicted, and how it resolved.")
    ).toBeVisible();
    await expect(
      page.getByText(LEDGER_SUMMARY_FIXTURE.events.value.toLocaleString())
    ).toBeVisible();
  });

  test("all 6 tabs visible and clickable", async ({ page }) => {
    await mockApi(page);
    await page.goto("/ledger");
    const tabs = [
      "All activity",
      "Model changes",
      "Predictions",
      "Actions",
      "Contestations",
      "Observations",
    ];
    for (const name of tabs) {
      await expect(page.getByRole("tab", { name })).toBeVisible();
    }
  });

  test("switching tab triggers API call with correct types param", async ({
    page,
  }) => {
    await mockApi(page);
    const requests: string[] = [];
    page.on("request", (r) => {
      const u = r.url();
      if (u.includes("/v1/history") && !u.includes("/summary")) {
        requests.push(u);
      }
    });
    await page.goto("/ledger");
    await page.getByTestId("ledger-timeline").waitFor();
    requests.length = 0;
    await page.getByRole("tab", { name: "Actions" }).click();
    await expect.poll(() =>
      requests.some((u) => u.includes("types=action_taken"))
    ).toBe(true);
  });

  test("clicking event opens inspector with correct event-type label color", async ({
    page,
  }) => {
    await mockApi(page);
    await page.goto("/ledger");
    await page
      .locator(`[data-event-id="${SALESFORCE_ESCALATION_EVENT.id}"]`)
      .click();
    await expect(page.getByTestId("ledger-inspector-title")).toContainText(
      SALESFORCE_ESCALATION_EVENT.title
    );
    const klass = page.getByTestId("ledger-inspector-class");
    await expect(klass).toContainText("ACTION TAKEN");
    await expect(klass).toHaveClass(
      /fy-ledger__inspector-class--action-taken/
    );
  });

  test("search input filters visible rows", async ({ page }) => {
    await mockApi(page);
    await page.goto("/ledger");
    await page.getByTestId("ledger-timeline").waitFor();
    const initialCount = await page
      .locator(".fy-ledger__event-row")
      .count();
    expect(initialCount).toBeGreaterThan(0);
    await page.getByTestId("ledger-search-input").fill("salesforce");
    // Every visible row should mention "salesforce" somewhere in its
    // body (title / summary / tags / actor). Search is multi-field so
    // we check the full row text rather than just the title.
    await expect.poll(async () => {
      const rowTexts = await page
        .locator(".fy-ledger__event-row")
        .allTextContents();
      if (rowTexts.length === 0) return false;
      return rowTexts.every((t) => t.toLowerCase().includes("salesforce"));
    }).toBe(true);
  });

  test("sticky date headers visible", async ({ page }) => {
    await mockApi(page);
    await page.goto("/ledger");
    const headers = page.getByTestId("ledger-day-header");
    await expect(headers.first()).toBeVisible();
    expect(await headers.count()).toBeGreaterThanOrEqual(2);
    await expect(headers.first()).toContainText(/Today/);
  });

  test("'View in model →' link is present in inspector", async ({ page }) => {
    await mockApi(page);
    await page.goto("/ledger");
    await page
      .locator(`[data-event-id="${SALESFORCE_ESCALATION_EVENT.id}"]`)
      .click();
    await expect(page.getByTestId("ledger-link-view-in-model")).toContainText(
      "View in model"
    );
    await expect(page.getByTestId("ledger-link-view-full-chain")).toContainText(
      "View full chain"
    );
  });

  test("empty state when route returns no events", async ({ page }) => {
    await mockApi(page, "empty");
    await page.goto("/ledger");
    await expect(
      page.locator(".fy-ledger__timeline-state[data-empty]")
    ).toBeVisible();
  });

  test("error state when route returns 500", async ({ page }) => {
    await mockApi(page, "error");
    await page.goto("/ledger");
    await expect(
      page.locator(".fy-ledger__timeline-state--error")
    ).toBeVisible();
    await expect(page.getByText(/Could not load ledger/i)).toBeVisible();
  });

  test("⌘K focuses search input", async ({ page }) => {
    await mockApi(page);
    await page.goto("/ledger");
    await page.getByTestId("ledger-timeline").waitFor();
    const input = page.getByTestId("ledger-search-input");
    const isMac = process.platform === "darwin";
    await page.keyboard.press(isMac ? "Meta+k" : "Control+k");
    await expect(input).toBeFocused();
  });
});
