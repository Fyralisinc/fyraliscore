import { test, expect, Page, Route } from "@playwright/test";

// Forecasts page Playwright suite. The mock-server.ts plugin doesn't
// implement /v1/forecasts/* endpoints yet, so each test stubs them
// with page.route() so the page can render end-to-end.

const SUMMARY = {
  active_count: 8,
  at_risk_arr: 3_840_000,
  high_confidence_count: 3,
  upcoming_resolutions_count_14d: 5,
  model_calibration: 0.72,
  calibration_delta: 0.03,
};

const PREDICTION_BEACON = {
  id: "pred-beacon-renewal",
  tenant_id: "11111111-1111-1111-1111-111111111111",
  status: "active",
  statement: "Beacon renewal at risk",
  rationale: "Two sync incidents this month; renewal call in 11 days.",
  category: "customer_risk",
  target_node_kind: "customer",
  target_node_id: "cust-beacon",
  target_label: "Beacon",
  confidence: 0.78,
  confidence_basis: "12 signals",
  falsification_condition: "VP Customer Success confirms exec brief landed.",
  key_drivers: [
    { title: "Salesforce sync failures", delta: "↑ 42%", tone: "negative" },
    { title: "Exec brief stalled", delta: "↑ 3 this week", tone: "negative" },
  ],
  impact: { arr_at_risk: 980000 },
  resolution_at: "2026-05-17T17:00:00Z",
  resolved_at: null,
  outcome: null,
  resolution_timeliness: null,
  created_at: "2026-05-15T09:18:00Z",
  updated_at: "2026-05-15T14:17:00Z",
};

const PREDICTION_ENG = {
  ...PREDICTION_BEACON,
  id: "pred-eng-capacity",
  statement: "Engineering capacity will exceed 90%",
  rationale: "Sustained utilization.",
  category: "capacity",
  target_label: "Engineering",
  confidence: 0.71,
  impact: { arr_at_risk: 0, capacity_pct: 92 },
  resolution_at: "2026-05-21T17:00:00Z",
};

const PREDICTION_Q3 = {
  ...PREDICTION_BEACON,
  id: "pred-q3-delivery",
  statement: "Q3 delivery commitments at risk",
  rationale: "Two enterprise commits depend on the same team.",
  category: "delivery",
  target_label: "Q3 release",
  confidence: 0.66,
  impact: { arr_at_risk: 1240000 },
  resolution_at: "2026-05-26T17:00:00Z",
};

const RESOLVED = {
  ...PREDICTION_BEACON,
  id: "pred-resolved-1",
  status: "resolved",
  statement: "Meridian will renew before May 1",
  outcome: "true",
  resolved_at: "2026-04-30T15:00:00Z",
  resolution_timeliness: "early",
};

const LIST = {
  items: [PREDICTION_BEACON, PREDICTION_ENG, PREDICTION_Q3],
  count: 3,
};

const RESOLVED_LIST = { items: [RESOLVED], count: 1 };

const ACCURACY = {
  bins: [
    { bin_label: "50-60", predicted_rate: 0.55, observed_hit_rate: 0.5, n_resolved: 4 },
    { bin_label: "60-70", predicted_rate: 0.65, observed_hit_rate: 0.6, n_resolved: 5 },
    { bin_label: "70-80", predicted_rate: 0.75, observed_hit_rate: 0.72, n_resolved: 11 },
    { bin_label: "80-90", predicted_rate: 0.85, observed_hit_rate: 0.83, n_resolved: 6 },
    { bin_label: "90-100", predicted_rate: 0.95, observed_hit_rate: null, n_resolved: 2 },
  ],
  recent_resolutions: [],
  calibration_summary: { value: 0.72, delta_vs_last_week: 0.03, n_resolved_total: 28 },
};

const RISK = {
  metric: "arr_at_risk",
  range_days: 90,
  buckets: [
    { bucket_start: "2026-05-15T00:00:00Z", bucket_end: "2026-05-22T00:00:00Z", value: 980000 },
    { bucket_start: "2026-05-22T00:00:00Z", bucket_end: "2026-05-29T00:00:00Z", value: 1240000 },
    { bucket_start: "2026-05-29T00:00:00Z", bucket_end: "2026-06-05T00:00:00Z", value: 740000 },
    { bucket_start: "2026-06-05T00:00:00Z", bucket_end: "2026-06-12T00:00:00Z", value: 320000 },
  ],
};

const UPCOMING = {
  items: [PREDICTION_BEACON, PREDICTION_ENG],
  count: 2,
  days: 14,
};

interface MockOptions {
  emptyActive?: boolean;
  errorList?: boolean;
}

async function installMocks(page: Page, opts: MockOptions = {}) {
  // Register the catch-all FIRST so the specific routes below (which
  // playwright resolves in reverse-registration order, i.e. most-recent
  // first) get the chance to intercept their specific paths before the
  // catch-all sees them.
  await page.route("**/api/v1/forecasts/**", async (route: Route) => {
    const req = route.request();
    const url = req.url();
    const method = req.method();
    if (method === "POST" && /\/v1\/forecasts\/?($|\?)/.test(url)) {
      let body: { statement?: string; category?: string; confidence?: number; resolution_at?: string } = {};
      try {
        body = JSON.parse(req.postData() ?? "{}");
      } catch {
        body = {};
      }
      return route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({
          ...PREDICTION_BEACON,
          id: `pred-new-${Date.now()}`,
          statement: body.statement ?? "scenario",
          category: body.category ?? "strategy",
          confidence: body.confidence ?? 0.6,
          resolution_at: body.resolution_at ?? "2026-06-01T17:00:00Z",
        }),
      });
    }
    const pathOnly = url.split("?")[0];
    const detailMatch = pathOnly.match(/\/v1\/forecasts\/([^/]+)$/);
    if (
      method === "GET" &&
      detailMatch &&
      detailMatch[1] !== "" &&
      detailMatch[1] !== "summary" &&
      detailMatch[1] !== "accuracy" &&
      detailMatch[1] !== "risk_exposure" &&
      detailMatch[1] !== "upcoming"
    ) {
      const id = decodeURIComponent(detailMatch[1]);
      const lookup: Record<string, unknown> = {
        [PREDICTION_BEACON.id]: PREDICTION_BEACON,
        [PREDICTION_ENG.id]: PREDICTION_ENG,
        [PREDICTION_Q3.id]: PREDICTION_Q3,
        [RESOLVED.id]: RESOLVED,
      };
      const prediction = lookup[id] ?? PREDICTION_BEACON;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ prediction, signals: [] }),
      });
    }
    if (method === "GET" && url.includes("/v1/forecasts")) {
      if (opts.errorList) {
        return route.fulfill({ status: 500, body: "error" });
      }
      const isResolved = url.includes("status=resolved");
      const payload = isResolved
        ? RESOLVED_LIST
        : opts.emptyActive
          ? { items: [], count: 0 }
          : LIST;
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(payload),
      });
    }
    return route.continue();
  });

  // Specific paths registered AFTER the catch-all so they win.
  await page.route("**/api/v1/forecasts/summary", (route: Route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(SUMMARY) })
  );
  await page.route("**/api/v1/forecasts/accuracy*", (route: Route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(ACCURACY) })
  );
  await page.route("**/api/v1/forecasts/risk_exposure*", (route: Route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(RISK) })
  );
  await page.route("**/api/v1/forecasts/upcoming*", (route: Route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(UPCOMING) })
  );
}

test.describe("Forecasts page", () => {
  test("loads the /forecasts route and renders header + predictions", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator(".fc-page-header__title")).toHaveText("Forecasts");
    await expect(page.locator('[data-testid="prediction-row"]').first()).toBeVisible();
    await expect(
      page.locator('[data-testid="prediction-row"]')
    ).toHaveCount(LIST.items.length);
  });

  test("tabs switch between Active, Resolved, Accuracy", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator('[data-testid="prediction-row"]').first()).toBeVisible();

    await page.locator('[data-testid="forecasts-tab-resolved"]').click();
    await expect(page.locator('[data-testid="resolved-list"]')).toBeVisible();
    await expect(
      page.locator('[data-testid="resolved-row"]')
    ).toHaveCount(RESOLVED_LIST.items.length);

    await page.locator('[data-testid="forecasts-tab-accuracy"]').click();
    await expect(page.locator('[data-testid="accuracy-panel"]')).toBeVisible();

    await page.locator('[data-testid="forecasts-tab-active"]').click();
    await expect(page.locator('[data-testid="prediction-row"]').first()).toBeVisible();
  });

  test("changing sort updates the order", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator('[data-testid="prediction-row"]').first()).toBeVisible();

    const sortRequest = page.waitForRequest((req) =>
      req.url().includes("/v1/forecasts/") &&
      req.url().includes("sort=highest_confidence")
    );
    await page.selectOption('select[aria-label="Sort predictions"]', "highest_confidence");
    await sortRequest;
  });

  test("clicking a prediction row opens inspector", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator('[data-testid="prediction-row"]').first()).toBeVisible();
    // The first row auto-selects on mount; click the second and check
    // the inspector title flips.
    await page.locator('[data-testid="prediction-row"]').nth(1).click();
    await expect(page.locator(".fy-inspector__title")).toContainText(
      /Engineering capacity will exceed 90%/
    );
  });

  test("View in model button navigates to /model (or attempts to)", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator('[data-testid="inspector-view-in-model"]')).toBeVisible();
    await page.locator('[data-testid="inspector-view-in-model"]').click();
    // Page may either land on /model or hit an auth redirect — both
    // confirm the click handler fired and the router took control.
    await expect(page).toHaveURL(/\/(model|demo)$/);
  });

  test("risk exposure chart renders SVG path", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator('[data-testid="risk-exposure-svg"]')).toBeVisible();
    await expect(page.locator('[data-testid="risk-exposure-line"]')).toBeVisible();
  });

  test("new scenario flow: open dialog, fill, submit, intercept POST", async ({ page }) => {
    await installMocks(page);
    await page.goto("/forecasts");
    await expect(page.locator('[data-testid="prediction-row"]').first()).toBeVisible();

    await page.getByRole("button", { name: /New scenario/i }).first().click();
    await expect(page.locator('[data-testid="new-scenario-dialog"]')).toBeVisible();

    await page
      .locator('[data-testid="new-scenario-statement"]')
      .fill("Pricing committee will ratify in 14 days");

    const postRequest = page.waitForRequest((req) =>
      req.method() === "POST" &&
      /\/v1\/forecasts\/?($|\?)/.test(req.url())
    );
    await page.locator('[data-testid="new-scenario-submit"]').click();
    const req = await postRequest;
    const body = JSON.parse(req.postData() ?? "{}");
    expect(body.statement).toMatch(/Pricing committee/);

    // Dialog should close after success.
    await expect(page.locator('[data-testid="new-scenario-dialog"]')).toBeHidden();
  });

  test("empty state when the list is empty", async ({ page }) => {
    await installMocks(page, { emptyActive: true });
    await page.goto("/forecasts");
    await expect(page.getByText(/No active predictions/i)).toBeVisible();
  });

  test("error state when the list endpoint returns 500", async ({ page }) => {
    await installMocks(page, { errorList: true });
    await page.goto("/forecasts");
    await expect(page.getByText(/Couldn't load predictions/i)).toBeVisible();
  });
});
