import { test, expect } from "@playwright/test";

const routes = [
  { path: "/today", testId: "today-page", label: "Today" },
  { path: "/model", testId: "model-page", label: "Model" },
  { path: "/forecasts", selector: ".fc-page", label: "Forecasts" },
  { path: "/ledger", selector: ".lg-page", label: "Ledger" },
];

for (const route of routes) {
  test(`${route.label} renders inside the Pelago demo session`, async ({ page }) => {
    const pageErrors: string[] = [];
    const failedResponses: string[] = [];
    page.on("pageerror", (err) => pageErrors.push(`${err.name}: ${err.message}`));
    page.on("response", (resp) => {
      if (resp.status() >= 500) failedResponses.push(`${resp.status()} ${resp.url()}`);
    });

    await page.goto(route.path);
    if (route.testId) {
      await expect(page.getByTestId(route.testId)).toBeVisible();
    } else {
      await expect(page.locator(route.selector!)).toBeVisible();
    }

    expect(pageErrors, "no uncaught page errors").toHaveLength(0);
    expect(failedResponses, "no 5xx responses").toHaveLength(0);
  });
}
