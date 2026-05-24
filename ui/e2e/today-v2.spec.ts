import { test, expect, type Route } from "@playwright/test";

// E2E for the Today (v2) page. Uses Playwright's `page.route()` to
// intercept the /api/v1/decision_deltas/* surface so we never touch
// mock-server.ts. The base webServer runs with USE_MOCK=1 so the
// other surfaces (Today legacy, /v1/today, etc.) are mocked.

const NOW = "2026-05-13T09:00:00Z";
function iso(daysAgo: number): string {
  return new Date(Date.parse(NOW) - daysAgo * 86_400_000).toISOString();
}

interface Delta {
  id: string;
  tenant_id: string;
  status: string;
  label: string;
  main_assertion: string;
  current_state: Record<string, unknown> | null;
  suggested_update: Record<string, unknown> | null;
  target_node_kind: string | null;
  target_node_id: string | null;
  confidence: number | null;
  confidence_basis: string | null;
  falsification_condition: string | null;
  consequence_preview: Record<string, unknown> | null;
  impact: Record<string, unknown> | null;
  category: string | null;
  source_recommendation_id: string | null;
  created_at: string;
  updated_at: string;
  accepted_at: string | null;
  accepted_by: string | null;
  resolution_target_at: string | null;
  evidence?: Array<Record<string, unknown>>;
  view?: Record<string, unknown>;
}

const TENANT = "tnt-fyralis-demo";

const E2E_DELTAS: Delta[] = [
  {
    id: "dd-1",
    tenant_id: TENANT,
    status: "proposed",
    label: "authority_required",
    main_assertion:
      "Salesforce sync escalation: three enterprise accounts have stalled past the renewal window.",
    current_state: { stage: "watching" },
    suggested_update: { stage: "escalate" },
    target_node_kind: "customer",
    target_node_id: null,
    confidence: 0.78,
    confidence_basis: "12 signals",
    falsification_condition: "If rep confirms calls, retract.",
    consequence_preview: { node_updates: 1 },
    impact: {
      arr_at_risk: 2_040_000,
      accounts_affected: 3,
      signals: 12,
      stale_days: 3,
      entity_refs: ["Beacon", "Northvale", "Conduit"],
      node_updates: 1,
      commitments_affected: 3,
      teams_notified: 2,
      why_this_matters: "ARR concentration risk.",
    },
    category: "customer_risk",
    source_recommendation_id: null,
    created_at: iso(3),
    updated_at: iso(3),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    evidence: [
      { id: "ev-1a", source: "crm", title: "Beacon: renewal call missed", ts: iso(3), trust_tier: "authoritative", weight: 0.9, ordinal: 0 },
      { id: "ev-1b", source: "crm", title: "Northvale: stage stuck",       ts: iso(4), trust_tier: "authoritative", weight: 0.9, ordinal: 1 },
    ],
    view: {
      severity: "critical",
      title: "Salesforce sync escalation",
      body: "Three enterprise accounts stalled past their renewal windows.",
      chips: ["Customer Risk", "Decision"],
      entity_refs: ["Beacon", "Northvale", "Conduit"],
      stale_days: 3,
      stale_label: "3 days",
      authority_required: true,
    },
  },
  {
    id: "dd-2",
    tenant_id: TENANT,
    status: "proposed",
    label: "authority_required",
    main_assertion:
      "Pricing decision: enterprise discount drift to 18% average.",
    current_state: { policy: "12%" },
    suggested_update: { policy: "tighten" },
    target_node_kind: "decision",
    target_node_id: null,
    confidence: 0.68,
    confidence_basis: "Quarterly average",
    falsification_condition: null,
    consequence_preview: null,
    impact: { stale_days: 42, entity_refs: ["Sales"] },
    category: "pricing",
    source_recommendation_id: null,
    created_at: iso(42),
    updated_at: iso(5),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    view: {
      severity: "high",
      title: "Pricing decision: enterprise discount drift",
      body: "Average enterprise discount has held at 18% for 42 days.",
      chips: ["Pricing"],
      entity_refs: ["Sales"],
      stale_days: 42,
      stale_label: "42 days",
      authority_required: true,
    },
  },
  // 4 delegatable so "Show 2 more" toggle has data to reveal
  {
    id: "dd-4",
    tenant_id: TENANT,
    status: "proposed",
    label: "recommended_update",
    main_assertion: "Support SLA slip.",
    current_state: null,
    suggested_update: null,
    target_node_kind: "commitment",
    target_node_id: null,
    confidence: 0.66,
    confidence_basis: null,
    falsification_condition: null,
    consequence_preview: null,
    impact: { stale_days: 14, entity_refs: ["Support"] },
    category: "delivery",
    source_recommendation_id: null,
    created_at: iso(14),
    updated_at: iso(1),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    view: {
      severity: "medium",
      title: "Support SLA slip",
      body: "Response time over SLA.",
      chips: ["Delivery"],
      entity_refs: ["Support"],
      stale_days: 14,
      stale_label: "14 days",
      owner: "Unassigned",
      authority_required: false,
    },
  },
  {
    id: "dd-5",
    tenant_id: TENANT,
    status: "proposed",
    label: "recommended_update",
    main_assertion: "Support backlog spike.",
    current_state: null,
    suggested_update: null,
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.74,
    confidence_basis: null,
    falsification_condition: null,
    consequence_preview: null,
    impact: { stale_days: 7, entity_refs: ["Support"] },
    category: "capacity",
    source_recommendation_id: null,
    created_at: iso(7),
    updated_at: iso(0),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    view: {
      severity: "medium",
      title: "Support backlog spike",
      body: "Backlog up 18 over last week.",
      chips: ["Capacity"],
      entity_refs: ["Support"],
      stale_days: 7,
      stale_label: "7 days",
      owner: "Head of Support",
      authority_required: false,
    },
  },
  {
    id: "dd-6",
    tenant_id: TENANT,
    status: "proposed",
    label: "recommended_update",
    main_assertion: "Attribution model gap.",
    current_state: null,
    suggested_update: null,
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.62,
    confidence_basis: null,
    falsification_condition: null,
    consequence_preview: null,
    impact: { stale_days: 21, entity_refs: ["Marketing"] },
    category: "strategy",
    source_recommendation_id: null,
    created_at: iso(21),
    updated_at: iso(2),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    view: {
      severity: "low",
      title: "Attribution model gap",
      body: "Two missing sources.",
      chips: ["Strategy"],
      entity_refs: ["Marketing"],
      stale_days: 21,
      stale_label: "21 days",
      owner: "VP Marketing",
      authority_required: false,
    },
  },
  {
    id: "dd-7",
    tenant_id: TENANT,
    status: "proposed",
    label: "recommended_update",
    main_assertion: "Checkout error rate climbing.",
    current_state: null,
    suggested_update: null,
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.69,
    confidence_basis: null,
    falsification_condition: null,
    consequence_preview: null,
    impact: { stale_days: 4, entity_refs: ["Platform"] },
    category: "delivery",
    source_recommendation_id: null,
    created_at: iso(4),
    updated_at: iso(0),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    view: {
      severity: "medium",
      title: "Checkout error rate climbing",
      body: "Up 0.4% week-over-week.",
      chips: ["Delivery"],
      entity_refs: ["Platform"],
      stale_days: 4,
      stale_label: "4 days",
      owner: "CTO",
      authority_required: false,
    },
  },
];

async function installDecisionDeltaRoutes(
  page: import("@playwright/test").Page,
  options: {
    listDeltas?: Delta[] | (() => Delta[]);
    status?: number;
    onAccept?: () => void;
    onDelegate?: () => void;
    onContest?: () => void;
  } = {}
) {
  const list = options.listDeltas ?? E2E_DELTAS;
  const status = options.status ?? 200;
  await page.route("**/api/v1/decision_deltas/**", async (route: Route) => {
    const req = route.request();
    const url = req.url();
    const method = req.method();
    if (status >= 500) {
      await route.fulfill({ status: 500, body: "boom" });
      return;
    }
    if (method === "GET" && /\/v1\/decision_deltas\/(\?|$)/.test(url)) {
      const items = typeof list === "function" ? list() : list;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ items, count: items.length }),
      });
      return;
    }
    // GET /v1/decision_deltas/{id}
    const detailMatch = url.match(/\/v1\/decision_deltas\/([^/?]+)$/);
    if (method === "GET" && detailMatch) {
      const id = decodeURIComponent(detailMatch[1]);
      const items = typeof list === "function" ? list() : list;
      const delta = items.find((d) => d.id === id) ?? items[0];
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(delta),
      });
      return;
    }
    if (method === "POST" && url.includes("/accept")) {
      options.onAccept?.();
      const items = typeof list === "function" ? list() : list;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ delta: items[0], triggered: { applied: true } }),
      });
      return;
    }
    if (method === "POST" && url.includes("/delegate")) {
      options.onDelegate?.();
      const items = typeof list === "function" ? list() : list;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ delta: items[0] }),
      });
      return;
    }
    if (method === "POST" && url.includes("/contest")) {
      options.onContest?.();
      const items = typeof list === "function" ? list() : list;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ delta: items[0] }),
      });
      return;
    }
    if (method === "POST" && url.includes("/add_context")) {
      const items = typeof list === "function" ? list() : list;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ delta: items[0] }),
      });
      return;
    }
    await route.continue();
  });
}

test.describe("Today v2", () => {
  test("renders sidebar, summary strip, authority queue, delegatable list, ask zone", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    // Sidebar — brand wordmark
    await expect(page.locator(".fy-sidebar__wordmark")).toContainText("Fyralis");
    // Summary strip: five cells
    await expect(page.locator(".fy-summary-cell")).toHaveCount(5);
    // Authority queue
    await expect(page.getByTestId("authority-section")).toBeVisible();
    await expect(page.locator(".ty-authority-row")).toHaveCount(2);
    // Delegatable list
    await expect(page.getByTestId("delegate-section")).toBeVisible();
    // Ask zone
    await expect(
      page.getByPlaceholder(/What did we decide about pricing/)
    ).toBeVisible();
  });

  test("clicking a row opens the inspector with the correct title and evidence", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await page.locator(".ty-authority-row").first().click();
    const inspector = page.getByTestId("today-inspector");
    await expect(inspector).toBeVisible();
    await expect(page.locator(".ty-inspector__title")).toContainText(
      "Salesforce sync escalation"
    );
    await expect(page.locator(".fy-evidence")).toHaveCount(2);
  });

  test("pressing J focuses next, K focuses prev", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    // First authority row is auto-focused.
    await expect(page.locator(".ty-authority-row").first()).toHaveClass(/focused/);
    await page.keyboard.press("j");
    await expect(page.locator(".ty-authority-row").nth(1)).toHaveClass(/focused/);
    await page.keyboard.press("k");
    await expect(page.locator(".ty-authority-row").first()).toHaveClass(/focused/);
  });

  test("pressing Enter opens inspector", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await expect(page.locator(".ty-authority-row").first()).toHaveClass(/focused/);
    await page.keyboard.press("Enter");
    await expect(page.getByTestId("today-inspector")).toBeVisible();
  });

  test("pressing A triggers accept", async ({ page }) => {
    let called = false;
    await installDecisionDeltaRoutes(page, {
      onAccept: () => {
        called = true;
      },
    });
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await expect(page.locator(".ty-authority-row").first()).toHaveClass(/focused/);
    await page.keyboard.press("a");
    await expect.poll(() => called).toBe(true);
  });

  test("pressing D opens delegate dialog", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await expect(page.locator(".ty-authority-row").first()).toHaveClass(/focused/);
    await page.keyboard.press("d");
    await expect(page.getByTestId("delegate-dialog")).toBeVisible();
  });

  test("pressing C opens contest dialog", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await expect(page.locator(".ty-authority-row").first()).toHaveClass(/focused/);
    await page.keyboard.press("c");
    await expect(page.getByTestId("contest-dialog")).toBeVisible();
  });

  test("pressing ? opens shortcuts overlay", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await page.keyboard.press("?");
    await expect(page.getByText(/Keyboard shortcuts/)).toBeVisible();
  });

  test("pressing Esc closes inspector", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await page.locator(".ty-authority-row").first().click();
    await expect(page.getByTestId("today-inspector")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.getByTestId("today-inspector")).toHaveCount(0);
  });

  test("pressing / focuses Ask input", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    const ask = page.getByPlaceholder(/What did we decide about pricing/);
    await ask.evaluate((el: HTMLInputElement) => el.blur());
    await page.keyboard.press("/");
    await expect(ask).toBeFocused();
  });

  test("critical row has Deep Garnet left edge", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    const criticalEdge = page
      .locator(".ty-authority-row--critical .ty-authority-row__edge")
      .first();
    await expect(criticalEdge).toBeVisible();
    const color = await criticalEdge.evaluate(
      (el) => getComputedStyle(el).backgroundColor
    );
    // Deep Garnet #7F2F29 => rgb(127, 47, 41)
    expect(color).toBe("rgb(127, 47, 41)");
  });

  test("Show 2 more toggle reveals hidden delegatable rows", async ({ page }) => {
    await installDecisionDeltaRoutes(page);
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    // 4 delegatable but only 4 shown initially — none hidden.
    const visibleBefore = await page.locator(".ty-delegate-row").count();
    expect(visibleBefore).toBeLessThanOrEqual(4);
    // We have exactly 4 delegatable in this fixture, so no show-more
    // toggle. Add one more to force the hidden state.
    const expanded = [
      ...E2E_DELTAS,
      {
        ...E2E_DELTAS[E2E_DELTAS.length - 1],
        id: "dd-extra",
        view: {
          ...(E2E_DELTAS[E2E_DELTAS.length - 1].view as Record<string, unknown>),
          title: "Extra delegatable item",
        },
      },
    ];
    await page.unroute("**/api/v1/decision_deltas/**");
    await installDecisionDeltaRoutes(page, { listDeltas: expanded });
    await page.reload();
    await page.getByTestId("today-page").waitFor();
    const toggle = page.getByTestId("show-more");
    await expect(toggle).toBeVisible();
    await toggle.click();
    await expect(page.locator(".ty-delegate-row")).toHaveCount(5);
  });

  test("empty state when route returns []", async ({ page }) => {
    await installDecisionDeltaRoutes(page, { listDeltas: [] });
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await expect(page.getByTestId("today-empty")).toBeVisible();
  });

  test("error state when route returns 500", async ({ page }) => {
    await installDecisionDeltaRoutes(page, { status: 500 });
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await expect(page.getByTestId("today-error")).toBeVisible();
  });

  test("inspector Escalate now button calls accept", async ({ page }) => {
    let called = false;
    await installDecisionDeltaRoutes(page, {
      onAccept: () => {
        called = true;
      },
    });
    await page.goto("/");
    await page.getByTestId("today-page").waitFor();
    await page.locator(".ty-authority-row").first().click();
    await page.getByTestId("inspector-accept").click();
    await expect.poll(() => called).toBe(true);
  });
});
