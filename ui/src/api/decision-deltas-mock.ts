// Mock fixture for the Decision Deltas surface. Shape mirrors
// decision-deltas-types.ts. Content matches the seven-delta scenario
// in the Today page screenshot: 3 authority-required + 4 delegatable.

import type {
  AddContextBody,
  ContestBody,
  DecisionDelta,
  DelegateBody,
  ListDeltasParams,
  ListDeltasResponse,
  MutationResponse,
} from "./decision-deltas-types";

const TENANT = "tnt-fyralis-demo";
const NOW_ISO = "2026-05-13T09:00:00Z";

function iso(daysAgo: number): string {
  const t = Date.parse(NOW_ISO) - daysAgo * 86_400_000;
  return new Date(t).toISOString();
}

export const DELTAS_FIXTURE: DecisionDelta[] = [
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
    confidence_basis: "12 corroborating signals across three accounts",
    falsification_condition:
      "If account-success rep confirms scheduled renewal calls in writing, retract.",
    consequence_preview: {
      node_updates: 1,
      commitments_affected: 3,
      teams_notified: 2,
    },
    impact: {
      arr_at_risk: 2_040_000,
      accounts_affected: 3,
      signals: 12,
      stale_days: 3,
      entity_refs: ["Beacon", "Northvale", "Conduit"],
      node_updates: 1,
      commitments_affected: 3,
      teams_notified: 2,
      why_this_matters:
        "ARR concentration in these three accounts represents 22% of FY revenue. Renewal windows close in 11 days; missing them costs the model six quarters of forecast accuracy.",
    },
    category: "customer_risk",
    source_recommendation_id: null,
    created_at: iso(3),
    updated_at: iso(3),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: iso(-11),
    evidence: [
      { id: "ev-1a", source: "crm",     title: "Beacon: renewal call missed",       ts: iso(3),  trust_tier: "authoritative", excerpt: null, weight: 0.9, ordinal: 0 },
      { id: "ev-1b", source: "crm",     title: "Northvale: stage stuck 14 days",    ts: iso(4),  trust_tier: "authoritative", excerpt: null, weight: 0.9, ordinal: 1 },
      { id: "ev-1c", source: "crm",     title: "Conduit: champion unresponsive",    ts: iso(5),  trust_tier: "attested",      excerpt: null, weight: 0.7, ordinal: 2 },
      { id: "ev-1d", source: "fyralis", title: "Sustained cluster pattern",         ts: iso(2),  trust_tier: "inferential",   excerpt: null, weight: 0.6, ordinal: 3 },
      { id: "ev-1e", source: "fyralis", title: "Reasoning trace: renewal triage",   ts: iso(1),  trust_tier: "inferential",   excerpt: null, weight: 0.6, ordinal: 4 },
    ],
    view: {
      severity: "critical",
      title: "Salesforce sync escalation",
      body: "Three enterprise accounts stalled past their renewal windows. Each is solvable; the cluster needs a decision.",
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
      "Pricing decision: enterprise tier discount policy has drifted to 18% average vs the 12% target.",
    current_state: { policy: "12%" },
    suggested_update: { policy: "tighten to 14%" },
    target_node_kind: "decision",
    target_node_id: null,
    confidence: 0.68,
    confidence_basis: "Quarterly average across 42 closed deals",
    falsification_condition:
      "If two of the recent discounts were strategic exceptions sanctioned by you, retract.",
    consequence_preview: {
      node_updates: 1,
      commitments_affected: 0,
      teams_notified: 1,
    },
    impact: {
      arr_at_risk: 820_000,
      accounts_affected: 42,
      signals: 8,
      stale_days: 42,
      entity_refs: ["Sales", "Finance"],
      why_this_matters:
        "Sustained 42 days of drift suggests the policy isn't bite-sized for the field. Either you tighten enforcement or you adjust the target.",
    },
    category: "pricing",
    source_recommendation_id: null,
    created_at: iso(42),
    updated_at: iso(5),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    evidence: [
      { id: "ev-2a", source: "finance", title: "Q1 closed-won discount roll-up", ts: iso(7),  trust_tier: "authoritative", excerpt: null, weight: 0.9, ordinal: 0 },
      { id: "ev-2b", source: "crm",     title: "Six deals > 20% discount",        ts: iso(10), trust_tier: "authoritative", excerpt: null, weight: 0.8, ordinal: 1 },
    ],
    view: {
      severity: "high",
      title: "Pricing decision: enterprise discount drift",
      body: "Average enterprise discount has held at 18% for 42 days against a 12% target. Either tighten or adjust.",
      chips: ["Pricing", "Decision"],
      entity_refs: ["Sales", "Finance"],
      stale_days: 42,
      stale_label: "42 days",
      authority_required: true,
    },
  },
  {
    id: "dd-3",
    tenant_id: TENANT,
    status: "proposed",
    label: "authority_required",
    main_assertion:
      "Engineering capacity has run at 92% for 5 sustained days — the warning band before delivery slips.",
    current_state: { utilization: "92%" },
    suggested_update: { utilization: "rebalance" },
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.71,
    confidence_basis: "Five consecutive days above 90% threshold",
    falsification_condition:
      "If the spike is explained by the platform migration sprint and ends Friday, retract.",
    consequence_preview: { node_updates: 1 },
    impact: {
      arr_at_risk: 0,
      accounts_affected: 0,
      signals: 14,
      stale_days: 5,
      entity_refs: ["Platform", "Growth", "Infra"],
      why_this_matters:
        "Sustained over-utilization compounds: every additional day above 90% costs roughly one engineer-week of throughput downstream.",
    },
    category: "capacity",
    source_recommendation_id: null,
    created_at: iso(5),
    updated_at: iso(0),
    accepted_at: null,
    accepted_by: null,
    resolution_target_at: null,
    evidence: [
      { id: "ev-3a", source: "linear",  title: "Sprint velocity dropped 18%",     ts: iso(2), trust_tier: "authoritative", excerpt: null, weight: 0.8, ordinal: 0 },
      { id: "ev-3b", source: "github",  title: "PR review SLA missed 9 times",    ts: iso(1), trust_tier: "authoritative", excerpt: null, weight: 0.8, ordinal: 1 },
    ],
    view: {
      severity: "high",
      title: "Engineering 92% sustained — capacity warning",
      body: "Engineering capacity at 92% for five sustained days. Throughput risk compounds past day seven.",
      chips: ["Capacity", "Delivery"],
      entity_refs: ["Platform", "Growth", "Infra"],
      stale_days: 5,
      stale_label: "Sustained 5 days",
      authority_required: true,
    },
  },
  // -------- Delegatable (4) --------
  {
    id: "dd-4",
    tenant_id: TENANT,
    status: "proposed",
    label: "recommended_update",
    main_assertion:
      "Support response time has slipped above the 4-hour SLA for the second week running.",
    current_state: { sla: "5h12m" },
    suggested_update: { sla: "restore 4h" },
    target_node_kind: "commitment",
    target_node_id: null,
    confidence: 0.66,
    confidence_basis: "Two-week trailing average",
    falsification_condition: null,
    consequence_preview: null,
    impact: {
      stale_days: 14,
      entity_refs: ["Support"],
    },
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
      body: "Average response time has run at 5h12m for two weeks against a 4h commitment.",
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
    main_assertion:
      "Backlog of unanswered support tickets has grown to 47, up 18 over last week.",
    current_state: { backlog: 47 },
    suggested_update: { backlog: "reduce to <20" },
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.74,
    confidence_basis: "Weekly count up 62%",
    falsification_condition: null,
    consequence_preview: null,
    impact: {
      stale_days: 7,
      entity_refs: ["Support"],
    },
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
      body: "Unanswered ticket count has climbed to 47 — up 18 in seven days.",
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
    main_assertion:
      "Marketing attribution model is missing two newly added campaign sources.",
    current_state: { sources: 6 },
    suggested_update: { sources: 8 },
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.62,
    confidence_basis: "Manual audit",
    falsification_condition: null,
    consequence_preview: null,
    impact: {
      stale_days: 21,
      entity_refs: ["Marketing"],
    },
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
      body: "Two newly added campaign sources are missing from the marketing attribution model.",
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
    main_assertion:
      "Production error rate climbed 0.4% week-over-week on the checkout flow.",
    current_state: { rate: "1.6%" },
    suggested_update: { rate: "<1.0%" },
    target_node_kind: "resource",
    target_node_id: null,
    confidence: 0.69,
    confidence_basis: "Seven-day rolling window",
    falsification_condition: null,
    consequence_preview: null,
    impact: {
      stale_days: 4,
      entity_refs: ["Platform", "Checkout"],
    },
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
      body: "Production error rate on the checkout flow is up 0.4% week-over-week.",
      chips: ["Delivery"],
      entity_refs: ["Platform", "Checkout"],
      stale_days: 4,
      stale_label: "4 days",
      owner: "CTO",
      authority_required: false,
    },
  },
];

export function mockListDeltas(
  params?: ListDeltasParams
): ListDeltasResponse {
  let items = DELTAS_FIXTURE.slice();
  if (params?.status) {
    const wanted = Array.isArray(params.status)
      ? new Set(params.status)
      : new Set([params.status]);
    items = items.filter((d) => wanted.has(d.status));
  }
  if (params?.category) {
    items = items.filter((d) => d.category === params.category);
  }
  if (params?.limit != null) {
    items = items.slice(0, params.limit);
  }
  return { items, count: items.length };
}

export function mockGetDelta(id: string): DecisionDelta | null {
  return DELTAS_FIXTURE.find((d) => d.id === id) ?? null;
}

function mutate(id: string, patch: Partial<DecisionDelta>): MutationResponse {
  const found = mockGetDelta(id);
  if (!found) {
    return { delta: { ...DELTAS_FIXTURE[0], id }, triggered: {} };
  }
  return { delta: { ...found, ...patch }, triggered: {} };
}

export function mockAcceptDelta(id: string): MutationResponse {
  const r = mutate(id, {
    status: "accepted",
    accepted_at: new Date().toISOString(),
  });
  return { ...r, triggered: { applied: true } };
}

export function mockDelegateDelta(
  id: string,
  body: DelegateBody
): MutationResponse {
  return mutate(id, { status: "delegated", impact: { ...(mockGetDelta(id)?.impact ?? {}), delegation: { owner_id: body.owner_id, note: body.note ?? null, at: new Date().toISOString() } } });
}

export function mockContestDelta(
  id: string,
  body: ContestBody
): MutationResponse {
  return mutate(id, { status: "contested", impact: { ...(mockGetDelta(id)?.impact ?? {}), contest: { by: "ceo", reason: body.reason, at: new Date().toISOString() } } });
}

export function mockAddContext(
  id: string,
  body: AddContextBody
): MutationResponse {
  const prev = mockGetDelta(id);
  const notes = prev?.impact?.context_notes ?? [];
  return mutate(id, {
    impact: {
      ...(prev?.impact ?? {}),
      context_notes: [...notes, { by: "ceo", note: body.note, at: new Date().toISOString() }],
    },
  });
}

// Export shape used by mock-server.ts when wiring (deferred — agent's
// report calls out the registration line). Caller registers all routes
// in one call rather than per-endpoint helpers.
export function registerDecisionDeltasMocks(): {
  list: (params?: ListDeltasParams) => ListDeltasResponse;
  get: (id: string) => DecisionDelta | null;
  accept: (id: string) => MutationResponse;
  delegate: (id: string, body: DelegateBody) => MutationResponse;
  contest: (id: string, body: ContestBody) => MutationResponse;
  addContext: (id: string, body: AddContextBody) => MutationResponse;
} {
  return {
    list: mockListDeltas,
    get: mockGetDelta,
    accept: mockAcceptDelta,
    delegate: mockDelegateDelta,
    contest: mockContestDelta,
    addContext: mockAddContext,
  };
}
