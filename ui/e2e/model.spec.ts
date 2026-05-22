// Playwright E2E for the Model page (v2). The page falls back to a
// spec-aligned fixture when the API is unavailable, so these tests
// don't need to seed the gateway — they exercise the state machine
// against the local fixture data.

import { test, expect, type Route } from "@playwright/test";

const BASE = "http://localhost:5173";

// Pre-seed localStorage so AutoDemoSession resolves immediately
// instead of hanging on /api/v1/demo/sessions/start (which mock-server
// doesn't implement). Without this, the page never reaches the model
// view and every test fails on "model-page not visible".
async function seedSession(page: import("@playwright/test").Page) {
  await page.addInitScript(() => {
    localStorage.setItem("demoAuthToken", "test-token");
    localStorage.setItem("demoSessionId", "test-session");
    localStorage.setItem("demoTenantId", "tnt-test");
    localStorage.setItem("demoCeoActorId", "actor-test");
    localStorage.setItem("demoCompanyId", "pelago");
  });
}

// Make sure API calls 404 fast (mock dev server doesn't implement
// /api/model/*) so the load layer falls back to the fixture without
// retries.
async function stubModelApiAsEmpty(page: import("@playwright/test").Page) {
  await seedSession(page);
  const fulfill = async (route: Route, body: unknown) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(body),
    });
  // Each endpoint returns an empty/sparse payload so the data loader's
  // sparseness check kicks in and the page renders the fixture.
  await page.route("**/api/model/overview*", (r) =>
    fulfill(r, {
      summary: {
        activeItemCount: 0,
        changedTodayCount: 0,
        blockedCount: 0,
        contestedCount: 0,
        lastUpdatedAt: new Date().toISOString(),
      },
      categories: [],
      relationshipBundles: [],
      mode: "impact",
      layoutHints: { categoryPositions: {} },
    }),
  );
  await page.route("**/api/model/categories/**/focus*", (r) =>
    fulfill(r, { category: null, topItems: [] }),
  );
  await page.route("**/api/model/relationships/**", (r) =>
    fulfill(r, { bundle: null, instances: [] }),
  );
  await page.route("**/api/model/items/**/trace*", (r) =>
    fulfill(r, { nodes: [], edges: [] }),
  );
  await page.route("**/api/model/items/**", (r) =>
    r.fulfill({ status: 404, body: "{}" }),
  );
}

test.describe("Model page (v2) — overview", () => {
  test.beforeEach(async ({ page }) => {
    await stubModelApiAsEmpty(page);
  });

  test("renders the page shell + 8 category modules", async ({ page }) => {
    await page.goto(`${BASE}/model`);
    await expect(page.getByTestId("model-page")).toBeVisible();
    await expect(page.getByTestId("model-header")).toBeVisible();
    await expect(page.getByTestId("model-modebar")).toBeVisible();

    const ids = [
      "goals", "commitments", "decisions", "risks",
      "customers", "people", "systems", "finance",
    ];
    for (const id of ids) {
      await expect(page.getByTestId(`category-${id}`)).toBeVisible();
    }
  });

  test("renders top relationship bundles with verbs", async ({ page }) => {
    await page.goto(`${BASE}/model`);
    // Impact mode shows at least the canonical Decisions → Commitments
    // blocks bundle.
    const blocksBundle = page.getByTestId("bundle-decisions__blocks__commitments");
    await expect(blocksBundle).toBeVisible();
    // And the Commitments → Customers affects bundle.
    await expect(
      page.getByTestId("bundle-commitments__affects__customers"),
    ).toBeVisible();
  });

  test("mode bar switches the visible bundles", async ({ page }) => {
    await page.goto(`${BASE}/model`);
    await page.getByTestId("mode-ownership").click();
    // Ownership mode shows the people→commitments owns bundle.
    await expect(page.getByTestId("bundle-people__owns__commitments")).toBeVisible();
    // And the Decisions→Commitments blocks bundle is NOT in ownership.
    await expect(
      page.getByTestId("bundle-decisions__blocks__commitments"),
    ).toHaveCount(0);

    await page.getByTestId("mode-evidence").click();
    // Evidence mode shows the systems→risks evidences bundle.
    await expect(
      page.getByTestId("bundle-systems__evidences__risks"),
    ).toBeVisible();
  });
});

test.describe("Model page (v2) — state transitions", () => {
  test.beforeEach(async ({ page }) => {
    await stubModelApiAsEmpty(page);
    await page.goto(`${BASE}/model`);
  });

  test("category click enters CategoryZoom and reveals top items", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await expect(page.getByTestId("categoryzoom-canvas")).toBeVisible();
    await expect(page.getByTestId("category-expanded-commitments")).toBeVisible();
    // Top items include the spec-aligned "Stabilize Salesforce sync".
    await expect(page.getByText("Stabilize Salesforce sync")).toBeVisible();
    // Back button returns to overview.
    await page.getByTestId("model-back").click();
    await expect(page.getByTestId("overview-canvas")).toBeVisible();
  });

  test("Esc returns to the previous state", async ({ page }) => {
    await page.getByTestId("category-decisions").click();
    await expect(page.getByTestId("categoryzoom-canvas")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("overview-canvas")).toBeVisible();
  });

  test("relationship click enters RelationshipZoom (corridor)", async ({ page }) => {
    await page.getByTestId("bundle-decisions__blocks__commitments").click();
    await expect(page.getByTestId("relationshipzoom-canvas")).toBeVisible();
    // Resolution opportunities appear for blocks bundles.
    await expect(page.getByText("Assign pricing owner")).toBeVisible();
    // Source-target instance "Pricing model has no owner" → "Launch DW pricing".
    await expect(page.getByText("Pricing model has no owner")).toBeVisible();
    await expect(page.getByText("Launch data warehouse pricing")).toBeVisible();
  });

  test("item micro-card click enters NodeZoom", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await expect(page.getByTestId("category-expanded-commitments")).toBeVisible();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await expect(page.getByTestId("nodezoom-canvas")).toBeVisible();
    await expect(page.getByTestId("node-central")).toBeVisible();
    // Floating toolbar appears.
    await expect(page.getByRole("button", { name: "Trace cause" })).toBeVisible();
    await expect(
      page.getByRole("button", { name: "Trace consequence" }),
    ).toBeVisible();
  });

  test("Trace consequence enters TraceView with a chain", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await page.getByRole("button", { name: "Trace consequence" }).click();
    await expect(page.getByTestId("trace-canvas")).toBeVisible();
    // The fixture chain runs to Board confidence.
    await expect(page.getByText("Board confidence")).toBeVisible();
    // Depth control is present.
    await expect(page.getByTestId("trace-depth")).toBeVisible();
    // Back returns to NodeZoom.
    await page.getByTestId("model-back").click();
    await expect(page.getByTestId("nodezoom-canvas")).toBeVisible();
  });

  test("Trace cause shows upstream chain", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await page.getByRole("button", { name: "Trace cause" }).click();
    await expect(page.getByTestId("trace-canvas")).toBeVisible();
    // Upstream chain includes the supporting observation source.
    await expect(page.getByText("Support tickets + CRM logs")).toBeVisible();
  });

  test("breadcrumb deep-link jumps back to overview", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await expect(page.getByTestId("nodezoom-canvas")).toBeVisible();
    // Click the root "Model" crumb. Use first() because the breadcrumb
    // root has the same accessible name as the page-level header.
    await page
      .locator(".fm-crumbs__link", { hasText: "Model" })
      .first()
      .click();
    await expect(page.getByTestId("overview-canvas")).toBeVisible();
  });
});

test.describe("Model page (v2) — search overlay", () => {
  test.beforeEach(async ({ page }) => {
    await stubModelApiAsEmpty(page);
    await page.goto(`${BASE}/model`);
  });

  test("Cmd/Ctrl+K opens the search overlay", async ({ page, browserName }) => {
    const mod = browserName === "webkit" ? "Meta" : "Control";
    await page.keyboard.press(`${mod}+KeyK`);
    await expect(page.getByTestId("search-overlay")).toBeVisible();
    await expect(page.getByTestId("search-overlay-input")).toBeFocused();
  });

  test("typing 'pricing' surfaces matching items", async ({ page }) => {
    await page.getByTestId("model-search").focus();
    await expect(page.getByTestId("search-overlay")).toBeVisible();
    await page.getByTestId("search-overlay-input").fill("pricing");
    await expect(
      page.getByRole("button", { name: /Pricing model has no owner/ }),
    ).toBeVisible();
  });

  test("clicking a search result enters NodeZoom", async ({ page }) => {
    await page.getByTestId("model-search").focus();
    await page.getByTestId("search-overlay-input").fill("stabilize");
    await page
      .getByRole("button", { name: /Stabilize Salesforce sync/ })
      .first()
      .click();
    await expect(page.getByTestId("nodezoom-canvas")).toBeVisible();
  });
});

test.describe("Model page (v2) — error + empty states", () => {
  test("renders gracefully when the gateway returns 500", async ({ page }) => {
    await seedSession(page);
    // Overview 500 → loader falls back to fixture, so the page still renders.
    await page.route("**/api/model/overview*", (r) =>
      r.fulfill({ status: 500, body: "boom" }),
    );
    await page.goto(`${BASE}/model`);
    await expect(page.getByTestId("model-page")).toBeVisible();
    // Fixture renders the 8 categories even on API failure.
    await expect(page.getByTestId("category-commitments")).toBeVisible();
  });
});

test.describe("Model page (v2) — design fix additions", () => {
  test.beforeEach(async ({ page }) => {
    await stubModelApiAsEmpty(page);
    await page.goto(`${BASE}/model`);
  });

  test("hovering a bundle reveals the inspect-preview tooltip", async ({ page }) => {
    const bundle = page.getByTestId("bundle-decisions__blocks__commitments");
    await bundle.hover();
    await expect(
      page.getByTestId("bundle-preview-decisions__blocks__commitments"),
    ).toBeVisible();
  });

  test("NodeZoom central card renders the relationship-count line", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await expect(page.getByTestId("node-central")).toBeVisible();
    // fixture has incoming "exposes" / "constrains" + outgoing "affects"
    // so at least one phrase should render.
    await expect(page.getByTestId("node-rel-counts")).toBeVisible();
  });

  test("Open full detail opens the slide-in sheet; Esc closes it", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await page
      .locator(".fm-toolbar__btn", { hasText: "Open full detail" })
      .click();
    await expect(page.getByTestId("full-detail-sheet")).toBeVisible();
    // sheet shows assertion, depends-on, supports/affects sections
    await expect(page.getByText("Supporting evidence")).toBeVisible();
    await expect(page.getByText("Supports / affects")).toBeVisible();
    // Esc closes it (sheet handles its own keyboard)
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("full-detail-sheet")).toHaveCount(0);
    // NodeZoom remains underneath
    await expect(page.getByTestId("nodezoom-canvas")).toBeVisible();
  });

  test("Full Detail close button dismisses the sheet (NodeZoom remains)", async ({ page }) => {
    await page.getByTestId("category-commitments").click();
    await page.getByTestId("micro-c-stabilize-sf").click();
    await page
      .locator(".fm-toolbar__btn", { hasText: "Open full detail" })
      .click();
    await expect(page.getByTestId("full-detail-sheet")).toBeVisible();
    await page.getByRole("button", { name: "Close" }).click();
    await expect(page.getByTestId("full-detail-sheet")).toHaveCount(0);
    await expect(page.getByTestId("nodezoom-canvas")).toBeVisible();
  });
});

test.describe("Model page (v2) — accessibility", () => {
  test.beforeEach(async ({ page }) => {
    await stubModelApiAsEmpty(page);
    await page.goto(`${BASE}/model`);
  });

  test("category modules carry ARIA labels with counts", async ({ page }) => {
    const ariaLabel = await page
      .getByTestId("category-commitments")
      .getAttribute("aria-label");
    expect(ariaLabel).toMatch(/Commitments/);
    expect(ariaLabel).toMatch(/active items/);
  });

  test("relationship labels are keyboard activatable", async ({ page }) => {
    const bundle = page.getByTestId("bundle-decisions__blocks__commitments");
    await bundle.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByTestId("relationshipzoom-canvas")).toBeVisible();
  });
});
