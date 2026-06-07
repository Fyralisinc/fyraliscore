import { test, expect } from "@playwright/test";
import { FORECASTS_PAGE_FIXTURE } from "../src/api/forecasts-page-mock";

async function routeForecastsPage(page: import("@playwright/test").Page, body: unknown, status = 200) {
  await page.route("**/api/v1/forecasts/page*", async (route) => {
    await route.fulfill({
      status,
      contentType: "application/json",
      body: typeof body === "string" ? body : JSON.stringify(body),
    });
  });
}

test.describe("Forecasts page", () => {
  test("loads Horizon mode with cards and selected inspector", async ({ page }) => {
    await page.goto("/forecasts");

    await expect(page.locator(".fc-header__title")).toHaveText("Forecasts");
    await expect(page.getByRole("region", { name: "Forecast Horizon Matrix" })).toBeVisible();
    await expect(page.locator(".fc-card").first()).toBeVisible();
    await expect(page.locator(".fc-inspector__title")).toContainText(
      "Beacon renewal risk likely to increase",
    );
  });

  test("switches between Patterns, Scenarios, and Accuracy modes", async ({ page }) => {
    await page.goto("/forecasts");

    await page.getByRole("tab", { name: "Patterns" }).click();
    await expect(page.getByRole("region", { name: "Patterns mode" })).toBeVisible();
    await expect(page.locator(".fc-patterns__card").first()).toBeVisible();

    await page.getByRole("tab", { name: "Scenarios" }).click();
    await expect(page.getByRole("region", { name: "Scenarios mode" })).toBeVisible();
    await page.getByPlaceholder("What if we…").fill("What if we assign an owner today?");
    await page.getByRole("button", { name: "Run scenario" }).click();
    await expect(page.locator(".fc-scenario-card--draft")).toBeVisible();

    await page.getByRole("tab", { name: "Accuracy" }).click();
    await expect(page.getByRole("region", { name: "Accuracy mode" })).toBeVisible();
    await expect(page.getByText("Calibration by confidence bin")).toBeVisible();
  });

  test("selecting a forecast updates the inspector and URL", async ({ page }) => {
    await page.goto("/forecasts");

    await page.locator(".fc-card", { hasText: "Engineering capacity will exceed 90%" }).click();

    await expect(page.locator(".fc-inspector__title")).toContainText(
      "Engineering capacity will exceed 90%",
    );
    await expect(page).toHaveURL(/forecast=/);
  });

  test("empty state when the page payload has no active forecasts", async ({ page }) => {
    await routeForecastsPage(page, {
      ...FORECASTS_PAGE_FIXTURE,
      header: { ...FORECASTS_PAGE_FIXTURE.header, active_forecast_count: 0 },
      horizon: { ...FORECASTS_PAGE_FIXTURE.horizon, domains: [] },
      forecast_details_by_id: {},
      selected_forecast_id: null,
    });

    await page.goto("/forecasts");
    await expect(page.getByText("No active forecasts right now.")).toBeVisible();
  });

  test("error state when the page payload fails", async ({ page }) => {
    await routeForecastsPage(page, "boom", 500);

    await page.goto("/forecasts");
    await expect(page.getByText("Forecasts could not be loaded.")).toBeVisible();
  });
});
