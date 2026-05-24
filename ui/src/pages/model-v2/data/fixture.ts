// Spec-aligned Model page fixture.
//
// The data here is the authoritative demo state for the Model page —
// it matches the worked examples in the spec (Stabilize Salesforce
// sync, Pricing model has no owner, Beacon renewal, Northvale,
// Conduit, etc.) so the page reads like the spec end-to-end even
// before the backend has full coverage of the 8-category model.
//
// Real tenants override these via the backend (services/gateway/
// model_page_routes.py). The page composes: try API → if any category
// is empty or fewer than 3 bundles, layer the fixture in by category.
// See `merge.ts`.

import type {
  CategoryFocus,
  CategoryId,
  ItemDetail,
  ModelCategory,
  ModelItemSummary,
  ModelOverview,
  RelationshipBundle,
  RelationshipFocus,
  RelationshipInstance,
  Trace,
} from "../types";

// ---------------------------------------------------------------------
// Atomic claims. Each ID is human-readable so debugging and routing is
// transparent. Real tenants use UUIDs; the fixture uses kebab-case ids
// — both kinds round-trip through encodeURIComponent the same.
// ---------------------------------------------------------------------

const items: Record<string, ModelItemSummary> = {
  // Goals & Priorities
  "g-revenue-target": {
    id: "g-revenue-target",
    categoryId: "goals",
    assertion: "Hit $24M ARR by end of Q3.",
    shortLabel: "Hit $24M ARR by end of Q3",
    status: "watch",
    relationshipHint: "served by 5 commitments",
    impactMetric: "$24M",
  },
  "g-anchor-retention": {
    id: "g-anchor-retention",
    categoryId: "goals",
    assertion: "Retain every anchor account through Q3.",
    shortLabel: "Retain every anchor account through Q3",
    status: "at_risk",
    relationshipHint: "threatened by 2 risks",
    impactMetric: "3 anchors",
  },
  "g-platform-readiness": {
    id: "g-platform-readiness",
    categoryId: "goals",
    assertion: "Platform is enterprise-ready by September.",
    shortLabel: "Platform is enterprise-ready by September",
    status: "watch",
    relationshipHint: "blocked by capacity decision",
  },

  // Commitments
  "c-stabilize-sf": {
    id: "c-stabilize-sf",
    categoryId: "commitments",
    assertion: "Stabilize Salesforce sync.",
    shortLabel: "Stabilize Salesforce sync",
    status: "at_risk",
    relationshipHint: "serves 3 renewals · $2.04M",
    impactMetric: "$2.04M",
    owner: "VP Engineering",
  },
  "c-launch-dw-pricing": {
    id: "c-launch-dw-pricing",
    categoryId: "commitments",
    assertion: "Launch data warehouse pricing.",
    shortLabel: "Launch data warehouse pricing",
    status: "blocked",
    relationshipHint: "blocked by pricing decision",
    impactMetric: "$720K opportunity",
    owner: "Unassigned",
  },
  "c-q3-roadmap": {
    id: "c-q3-roadmap",
    categoryId: "commitments",
    assertion: "Ship Q3 platform roadmap.",
    shortLabel: "Ship Q3 platform roadmap",
    status: "watch",
    relationshipHint: "constrained by capacity",
    owner: "Product",
  },
  "c-anchor-reporting": {
    id: "c-anchor-reporting",
    categoryId: "commitments",
    assertion: "Protect anchor renewal reporting.",
    shortLabel: "Protect anchor renewal reporting",
    status: "watch",
    relationshipHint: "serves Beacon renewal",
    owner: "Customer Success",
  },

  // Decisions
  "d-pricing-owner": {
    id: "d-pricing-owner",
    categoryId: "decisions",
    assertion: "Pricing model has no owner.",
    shortLabel: "Pricing model has no owner",
    status: "critical",
    relationshipHint: "blocks 2 commitments · 42 days open",
    impactMetric: "42d",
  },
  "d-q3-scope": {
    id: "d-q3-scope",
    categoryId: "decisions",
    assertion: "Q3 scope tradeoff unresolved.",
    shortLabel: "Q3 scope tradeoff unresolved",
    status: "at_risk",
    relationshipHint: "blocks 2 commitments",
  },
  "d-packaging": {
    id: "d-packaging",
    categoryId: "decisions",
    assertion: "Enterprise packaging approach undecided.",
    shortLabel: "Enterprise packaging approach undecided",
    status: "watch",
    relationshipHint: "blocks 1 commitment",
  },
  "d-conversation-ai": {
    id: "d-conversation-ai",
    categoryId: "decisions",
    assertion: "Conversation-AI re-scope unresolved.",
    shortLabel: "Conversation-AI re-scope unresolved",
    status: "watch",
    relationshipHint: "4 customer requests waiting",
  },

  // Risks & Constraints
  "r-sf-instability": {
    id: "r-sf-instability",
    categoryId: "risks",
    assertion: "Salesforce sync instability is affecting anchor accounts.",
    shortLabel: "Salesforce sync instability is affecting anchor accounts",
    status: "at_risk",
    relationshipHint: "exposes $2.04M ARR",
    impactMetric: "$2.04M",
  },
  "r-beacon-renewal": {
    id: "r-beacon-renewal",
    categoryId: "risks",
    assertion: "Beacon renewal is at elevated risk.",
    shortLabel: "Beacon renewal is at elevated risk",
    status: "critical",
    relationshipHint: "$840K ARR exposed",
    impactMetric: "$840K",
  },
  "r-capacity-shortfall": {
    id: "r-capacity-shortfall",
    categoryId: "risks",
    assertion: "Engineering Platform is operating above planned capacity.",
    shortLabel: "Engineering Platform above planned capacity",
    status: "at_risk",
    relationshipHint: "constrains 2 commitments",
    impactMetric: "92%",
  },
  "r-support-burden": {
    id: "r-support-burden",
    categoryId: "risks",
    assertion: "Anchor support burden is rising 18% week over week.",
    shortLabel: "Anchor support burden rising 18% wow",
    status: "watch",
    relationshipHint: "affects 3 customers",
  },

  // Customers & Revenue
  "cu-beacon": {
    id: "cu-beacon",
    categoryId: "customers",
    assertion: "Beacon renewal pending — anchor account.",
    shortLabel: "Beacon renewal pending",
    status: "at_risk",
    relationshipHint: "$840K ARR",
    impactMetric: "$840K",
  },
  "cu-northvale": {
    id: "cu-northvale",
    categoryId: "customers",
    assertion: "Northvale expansion stalled on sync reliability.",
    shortLabel: "Northvale expansion stalled",
    status: "watch",
    relationshipHint: "$620K ARR",
    impactMetric: "$620K",
  },
  "cu-conduit": {
    id: "cu-conduit",
    categoryId: "customers",
    assertion: "Conduit upgrade path is open.",
    shortLabel: "Conduit upgrade path open",
    status: "healthy",
    relationshipHint: "$580K ARR opportunity",
    impactMetric: "$580K",
  },
  "cu-port-stream": {
    id: "cu-port-stream",
    categoryId: "customers",
    assertion: "PortStream onboarding is slipping by two weeks.",
    shortLabel: "PortStream onboarding slipping 2w",
    status: "watch",
  },

  // People & Teams
  "p-pricing-gap": {
    id: "p-pricing-gap",
    categoryId: "people",
    assertion: "Pricing has no accountable owner.",
    shortLabel: "Pricing has no accountable owner",
    status: "critical",
    relationshipHint: "blocks 1 decision",
  },
  "p-vp-eng": {
    id: "p-vp-eng",
    categoryId: "people",
    assertion: "VP Engineering owns Salesforce sync stabilization.",
    shortLabel: "VP Engineering owns Salesforce stabilization",
    status: "healthy",
    relationshipHint: "owns 1 commitment",
    owner: "Maya R.",
  },
  "p-cs-head": {
    id: "p-cs-head",
    categoryId: "people",
    assertion: "Head of CS owns anchor renewal reporting.",
    shortLabel: "Head of CS owns anchor renewal reporting",
    status: "healthy",
    relationshipHint: "owns 1 commitment",
    owner: "Priya S.",
  },
  "p-gtm-gap": {
    id: "p-gtm-gap",
    categoryId: "people",
    assertion: "GTM lead for enterprise packaging is unassigned.",
    shortLabel: "GTM enterprise packaging unassigned",
    status: "at_risk",
    relationshipHint: "blocks 1 decision",
  },

  // Systems & Capacity
  "s-platform-capacity": {
    id: "s-platform-capacity",
    categoryId: "systems",
    assertion: "Platform Engineering capacity is at 92% of plan.",
    shortLabel: "Platform capacity at 92%",
    status: "at_risk",
    relationshipHint: "constrains 2 commitments",
    impactMetric: "92%",
  },
  "s-data-pipeline": {
    id: "s-data-pipeline",
    categoryId: "systems",
    assertion: "Data warehouse pipeline is stable but undersized.",
    shortLabel: "DW pipeline stable but undersized",
    status: "watch",
    relationshipHint: "limits DW pricing launch",
  },
  "s-support-tooling": {
    id: "s-support-tooling",
    categoryId: "systems",
    assertion: "Support tooling is fragmented across anchor segments.",
    shortLabel: "Support tooling fragmented",
    status: "watch",
    relationshipHint: "affects 3 customers",
  },

  // Finance & Capital
  "f-runway": {
    id: "f-runway",
    categoryId: "finance",
    assertion: "Runway is 11 months at current burn.",
    shortLabel: "Runway 11 months at current burn",
    status: "watch",
    impactMetric: "11mo",
  },
  "f-platform-budget": {
    id: "f-platform-budget",
    categoryId: "finance",
    assertion: "Platform budget funds 2 capacity expansions.",
    shortLabel: "Platform budget funds 2 expansions",
    status: "healthy",
    relationshipHint: "funds 2 systems",
  },
  "f-gtm-spend": {
    id: "f-gtm-spend",
    categoryId: "finance",
    assertion: "GTM spend is pacing 8% under plan.",
    shortLabel: "GTM spend pacing 8% under plan",
    status: "healthy",
  },
};

// Index by category for fast lookup.
function byCategory(cid: CategoryId): ModelItemSummary[] {
  return Object.values(items).filter((i) => i.categoryId === cid);
}

// ---------------------------------------------------------------------
// Categories — locked set of 8 (spec §3.2).
// ---------------------------------------------------------------------

const categoryMeta: Record<
  CategoryId,
  Pick<ModelCategory, "id" | "label" | "description" | "colorToken" | "position">
> = {
  goals: {
    id: "goals",
    label: "Goals & Priorities",
    description: "Strategic objectives currently in play.",
    colorToken: "moss",
    position: { x: 0.50, y: 0.10 },
  },
  decisions: {
    id: "decisions",
    label: "Decisions",
    description: "Open questions awaiting judgment.",
    colorToken: "iris",
    position: { x: 0.18, y: 0.36 },
  },
  commitments: {
    id: "commitments",
    label: "Commitments",
    description: "Promised work and present-tense execution claims.",
    colorToken: "lapis",
    position: { x: 0.50, y: 0.36 },
  },
  customers: {
    id: "customers",
    label: "Customers & Revenue",
    description: "Customers and revenue exposure.",
    colorToken: "teal",
    position: { x: 0.82, y: 0.36 },
  },
  people: {
    id: "people",
    label: "People & Teams",
    description: "Owners, contributors, and accountability gaps.",
    colorToken: "ochre",
    position: { x: 0.18, y: 0.66 },
  },
  risks: {
    id: "risks",
    label: "Risks & Constraints",
    description: "Active concerns and contested claims.",
    colorToken: "garnet",
    position: { x: 0.50, y: 0.66 },
  },
  systems: {
    id: "systems",
    label: "Systems & Capacity",
    description: "Operational systems and capacity.",
    colorToken: "blue",
    position: { x: 0.82, y: 0.66 },
  },
  finance: {
    id: "finance",
    label: "Finance & Capital",
    description: "Capital, runway, and funding posture.",
    colorToken: "gold",
    position: { x: 0.50, y: 0.92 },
  },
};

function buildCategory(cid: CategoryId): ModelCategory {
  const own = byCategory(cid);
  const meta = categoryMeta[cid];
  const dist: Record<string, number> = {};
  for (const i of own) dist[i.status] = (dist[i.status] ?? 0) + 1;
  const distribution: ModelCategory["statusDistribution"] = [
    "healthy",
    "watch",
    "at_risk",
    "critical",
    "contested",
  ].map((s) => ({ status: s as ModelCategory["statusDistribution"][number]["status"], count: dist[s] ?? 0 }));
  const contested = own.filter((i) => i.status === "contested").length;
  const blocked = own.filter((i) => i.status === "blocked").length;
  const atRisk = own.filter((i) => i.status === "at_risk" || i.status === "critical").length;
  return {
    ...meta,
    itemCount: own.length,
    changedTodayCount: cid === "decisions" || cid === "commitments" ? 3 : 1,
    contestedCount: contested,
    blockedCount: blocked,
    atRiskCount: atRisk,
    topItems: own.slice(0, 4),
    statusDistribution: distribution,
  };
}

// ---------------------------------------------------------------------
// Relationship bundles. Each mode emphasises a different subset (spec
// §3.5 / §7). The frontend renders 5–7 of the active mode's bundles.
// ---------------------------------------------------------------------

type BundleSeed = Omit<
  RelationshipBundle,
  "mode" | "label" | "visual" | "id"
> & {
  label: string;
  visual: RelationshipBundle["visual"];
  modes: RelationshipBundle["mode"][];
};

function bundleId(src: CategoryId, verb: string, tgt: CategoryId): string {
  return `${src}__${verb}__${tgt}`;
}

const bundleSeeds: BundleSeed[] = [
  {
    sourceCategoryId: "commitments",
    targetCategoryId: "customers",
    verb: "affects",
    label: "affects 3 customers · $2.04M",
    instanceCount: 3,
    severity: "high",
    impactLabel: "$2.04M ARR",
    impactValue: 2_040_000,
    topExample: {
      sourceShortLabel: "Stabilize Salesforce sync",
      targetShortLabel: "Beacon renewal",
    },
    visual: { colorToken: "moss", strength: "high", direction: "source_to_target", lineStyle: "solid" },
    modes: ["impact"],
  },
  {
    sourceCategoryId: "risks",
    targetCategoryId: "customers",
    verb: "exposes",
    label: "exposes $2.04M ARR",
    instanceCount: 2,
    severity: "high",
    impactLabel: "$2.04M ARR",
    impactValue: 2_040_000,
    topExample: {
      sourceShortLabel: "Salesforce sync instability",
      targetShortLabel: "3 anchor accounts",
    },
    visual: { colorToken: "garnet", strength: "high", direction: "source_to_target", lineStyle: "solid" },
    modes: ["impact"],
  },
  {
    sourceCategoryId: "systems",
    targetCategoryId: "customers",
    verb: "affects",
    label: "degrades customer impact",
    instanceCount: 2,
    severity: "medium",
    visual: { colorToken: "blue", strength: "medium", direction: "source_to_target", lineStyle: "solid" },
    modes: ["impact", "dependencies"],
  },
  {
    sourceCategoryId: "finance",
    targetCategoryId: "systems",
    verb: "funds",
    label: "funds 2 systems",
    instanceCount: 2,
    severity: "medium",
    visual: { colorToken: "gold", strength: "medium", direction: "source_to_target", lineStyle: "solid" },
    modes: ["impact", "dependencies"],
  },
  {
    sourceCategoryId: "goals",
    targetCategoryId: "commitments",
    verb: "serves",
    label: "served by 5 commitments",
    instanceCount: 5,
    severity: "high",
    visual: { colorToken: "moss", strength: "high", direction: "source_to_target", lineStyle: "solid" },
    modes: ["impact", "dependencies"],
  },
  {
    sourceCategoryId: "decisions",
    targetCategoryId: "commitments",
    verb: "blocks",
    label: "blocks 4 commitments",
    instanceCount: 4,
    severity: "high",
    impactLabel: "$890K blocked",
    impactValue: 890_000,
    topExample: {
      sourceShortLabel: "Pricing model has no owner",
      targetShortLabel: "Launch data warehouse pricing",
    },
    visual: { colorToken: "garnet", strength: "high", direction: "source_to_target", lineStyle: "solid" },
    modes: ["impact", "dependencies"],
  },
  {
    sourceCategoryId: "people",
    targetCategoryId: "decisions",
    verb: "owns",
    label: "3 owner gaps",
    instanceCount: 3,
    severity: "medium",
    visual: { colorToken: "ochre", strength: "medium", direction: "source_to_target", lineStyle: "dashed" },
    modes: ["impact", "ownership"],
  },
  {
    sourceCategoryId: "systems",
    targetCategoryId: "commitments",
    verb: "constrains",
    label: "constrains 6 commitments",
    instanceCount: 6,
    severity: "high",
    visual: { colorToken: "blue", strength: "high", direction: "source_to_target", lineStyle: "solid" },
    modes: ["dependencies"],
  },
  {
    sourceCategoryId: "people",
    targetCategoryId: "commitments",
    verb: "owns",
    label: "owns 12 commitments",
    instanceCount: 12,
    severity: "medium",
    visual: { colorToken: "ochre", strength: "medium", direction: "source_to_target", lineStyle: "solid" },
    modes: ["ownership"],
  },
  {
    sourceCategoryId: "people",
    targetCategoryId: "risks",
    verb: "owns",
    label: "owns 4 risks",
    instanceCount: 4,
    severity: "medium",
    visual: { colorToken: "ochre", strength: "medium", direction: "source_to_target", lineStyle: "solid" },
    modes: ["ownership"],
  },
  {
    sourceCategoryId: "people",
    targetCategoryId: "systems",
    verb: "owns",
    label: "2 system owner gaps",
    instanceCount: 2,
    severity: "medium",
    visual: { colorToken: "ochre", strength: "medium", direction: "source_to_target", lineStyle: "dashed" },
    modes: ["ownership"],
  },
  {
    sourceCategoryId: "risks",
    targetCategoryId: "commitments",
    verb: "constrains",
    label: "threatens 6 commitments",
    instanceCount: 6,
    severity: "high",
    visual: { colorToken: "coral", strength: "high", direction: "source_to_target", lineStyle: "solid" },
    modes: ["dependencies"],
  },
  // Evidence-mode bundles
  {
    sourceCategoryId: "systems",
    targetCategoryId: "risks",
    verb: "evidences",
    label: "evidences 3 risks",
    instanceCount: 3,
    severity: "medium",
    visual: { colorToken: "lapis", strength: "medium", direction: "source_to_target", lineStyle: "solid" },
    modes: ["evidence"],
  },
  {
    sourceCategoryId: "customers",
    targetCategoryId: "risks",
    verb: "evidences",
    label: "evidences 2 risks",
    instanceCount: 2,
    severity: "medium",
    visual: { colorToken: "lapis", strength: "medium", direction: "source_to_target", lineStyle: "solid" },
    modes: ["evidence"],
  },
  {
    sourceCategoryId: "customers",
    targetCategoryId: "decisions",
    verb: "evidences",
    label: "weak evidence on 2 decisions",
    instanceCount: 2,
    severity: "low",
    visual: { colorToken: "ochre", strength: "low", direction: "source_to_target", lineStyle: "dashed" },
    modes: ["evidence"],
  },
];

function bundlesForMode(mode: RelationshipBundle["mode"]): RelationshipBundle[] {
  return bundleSeeds
    .filter((b) => b.modes.includes(mode))
    .map((b) => ({
      id: bundleId(b.sourceCategoryId, b.verb, b.targetCategoryId),
      mode,
      sourceCategoryId: b.sourceCategoryId,
      targetCategoryId: b.targetCategoryId,
      verb: b.verb,
      label: b.label,
      instanceCount: b.instanceCount,
      severity: b.severity,
      impactLabel: b.impactLabel,
      impactValue: b.impactValue,
      topExample: b.topExample,
      visual: b.visual,
    }));
}

// ---------------------------------------------------------------------
// Relationship instance fixtures keyed by bundle id. Used for
// RelationshipZoom corridor.
// ---------------------------------------------------------------------

const instanceSeeds: Record<string, RelationshipInstance[]> = {
  // Decisions → Commitments: blocks 4
  [bundleId("decisions", "blocks", "commitments")]: [
    {
      id: "i-1",
      sourceItem: items["d-pricing-owner"],
      targetItem: items["c-launch-dw-pricing"],
      verb: "blocks",
      explanation: "Pricing model has no owner blocks Launch data warehouse pricing.",
      impactLabel: "$720K opportunity blocked",
    },
    {
      id: "i-2",
      sourceItem: items["d-q3-scope"],
      targetItem: items["c-q3-roadmap"],
      verb: "blocks",
      explanation: "Q3 scope tradeoff unresolved blocks Ship Q3 platform roadmap.",
      impactLabel: "2 commitments at risk",
    },
    {
      id: "i-3",
      sourceItem: items["d-packaging"],
      targetItem: items["c-q3-roadmap"],
      verb: "blocks",
      explanation: "Enterprise packaging undecided blocks Enterprise GTM launch.",
      impactLabel: "conversion risk",
    },
    {
      id: "i-4",
      sourceItem: items["d-conversation-ai"],
      targetItem: items["c-anchor-reporting"],
      verb: "blocks",
      explanation: "Conversation-AI re-scope unresolved blocks ICP scoring commitments.",
      impactLabel: "4 customer requests affected",
    },
  ],
  // Commitments → Customers: affects 3
  [bundleId("commitments", "affects", "customers")]: [
    {
      id: "ic-1",
      sourceItem: items["c-stabilize-sf"],
      targetItem: items["cu-beacon"],
      verb: "affects",
      explanation: "Stabilize Salesforce sync affects Beacon renewal.",
      impactLabel: "$840K ARR",
    },
    {
      id: "ic-2",
      sourceItem: items["c-stabilize-sf"],
      targetItem: items["cu-northvale"],
      verb: "affects",
      explanation: "Stabilize Salesforce sync affects Northvale expansion.",
      impactLabel: "$620K ARR",
    },
    {
      id: "ic-3",
      sourceItem: items["c-anchor-reporting"],
      targetItem: items["cu-conduit"],
      verb: "affects",
      explanation: "Anchor renewal reporting affects Conduit upgrade.",
      impactLabel: "$580K opportunity",
    },
  ],
  // Risks → Customers: exposes
  [bundleId("risks", "exposes", "customers")]: [
    {
      id: "ir-1",
      sourceItem: items["r-sf-instability"],
      targetItem: items["cu-beacon"],
      verb: "exposes",
      explanation: "Salesforce sync instability exposes Beacon renewal.",
      impactLabel: "$840K ARR exposed",
    },
    {
      id: "ir-2",
      sourceItem: items["r-beacon-renewal"],
      targetItem: items["cu-beacon"],
      verb: "exposes",
      explanation: "Beacon renewal risk exposes anchor revenue.",
      impactLabel: "$840K ARR",
    },
  ],
  // People → Decisions: 3 owner gaps
  [bundleId("people", "owns", "decisions")]: [
    {
      id: "io-1",
      sourceItem: items["p-pricing-gap"],
      targetItem: items["d-pricing-owner"],
      verb: "owns",
      explanation: "Pricing has no accountable owner — gap on Pricing decision.",
      impactLabel: "owner gap",
    },
    {
      id: "io-2",
      sourceItem: items["p-gtm-gap"],
      targetItem: items["d-packaging"],
      verb: "owns",
      explanation: "GTM enterprise packaging is unassigned — gap on packaging decision.",
      impactLabel: "owner gap",
    },
    {
      id: "io-3",
      sourceItem: items["p-gtm-gap"],
      targetItem: items["d-conversation-ai"],
      verb: "owns",
      explanation: "GTM lead missing — gap on Conversation-AI re-scope.",
      impactLabel: "owner gap",
    },
  ],
  // Goals → Commitments: serves
  [bundleId("goals", "serves", "commitments")]: [
    {
      id: "ig-1",
      sourceItem: items["g-revenue-target"],
      targetItem: items["c-stabilize-sf"],
      verb: "serves",
      explanation: "$24M ARR goal served by Stabilize Salesforce sync.",
    },
    {
      id: "ig-2",
      sourceItem: items["g-anchor-retention"],
      targetItem: items["c-anchor-reporting"],
      verb: "serves",
      explanation: "Anchor retention served by anchor renewal reporting.",
    },
    {
      id: "ig-3",
      sourceItem: items["g-platform-readiness"],
      targetItem: items["c-q3-roadmap"],
      verb: "serves",
      explanation: "Platform readiness served by Q3 roadmap.",
    },
  ],
  // Systems → Commitments: constrains
  [bundleId("systems", "constrains", "commitments")]: [
    {
      id: "is-1",
      sourceItem: items["s-platform-capacity"],
      targetItem: items["c-q3-roadmap"],
      verb: "constrains",
      explanation: "Platform capacity at 92% constrains Q3 roadmap.",
      impactLabel: "capacity bound",
    },
    {
      id: "is-2",
      sourceItem: items["s-data-pipeline"],
      targetItem: items["c-launch-dw-pricing"],
      verb: "limits",
      explanation: "DW pipeline undersized — limits DW pricing launch.",
    },
  ],
};

// ---------------------------------------------------------------------
// Trace fixtures keyed by item id. Used when the backend returns an
// empty trace (sparse tenant) or the item id is a fixture id.
// ---------------------------------------------------------------------

const traceConsequenceSeeds: Record<string, Trace> = {
  "c-stabilize-sf": {
    rootItemId: "c-stabilize-sf",
    direction: "consequence",
    nodes: [
      { id: "c-stabilize-sf", assertion: "Stabilize Salesforce sync.", shortLabel: "Stabilize Salesforce sync", kind: "commitment", step: 0 },
      { id: "cu-beacon", assertion: "Beacon renewal pending.", shortLabel: "Beacon renewal pending", kind: "customer", step: 1 },
      { id: "f-arr-risk", assertion: "$840K ARR at risk.", shortLabel: "$840K ARR at risk", kind: "outcome", step: 2 },
      { id: "g-revenue-target", assertion: "Q3 revenue target.", shortLabel: "Q3 revenue target", kind: "goal", step: 3 },
      { id: "outcome-board", assertion: "Board confidence depends on Q3 anchor outcomes.", shortLabel: "Board confidence", kind: "outcome", step: 4 },
    ],
    edges: [
      { source: "c-stabilize-sf", target: "cu-beacon", verb: "affects" },
      { source: "cu-beacon", target: "f-arr-risk", verb: "impacts" },
      { source: "f-arr-risk", target: "g-revenue-target", verb: "impacts" },
      { source: "g-revenue-target", target: "outcome-board", verb: "influences" },
    ],
  },
};

const traceCauseSeeds: Record<string, Trace> = {
  "c-stabilize-sf": {
    rootItemId: "c-stabilize-sf",
    direction: "cause",
    nodes: [
      { id: "c-stabilize-sf", assertion: "Stabilize Salesforce sync.", shortLabel: "Stabilize Salesforce sync", kind: "commitment", step: 0 },
      { id: "r-sf-instability", assertion: "Salesforce sync instability.", shortLabel: "Salesforce sync instability", kind: "risk", step: 1 },
      { id: "obs-sync-errors", assertion: "Increase in sync errors observed.", shortLabel: "Increase in sync errors", kind: "observation", step: 2 },
      { id: "src-support-crm", assertion: "Support tickets + CRM logs.", shortLabel: "Support tickets + CRM logs", kind: "evidence", step: 3, source: "Support, CRM" },
    ],
    edges: [
      { source: "r-sf-instability", target: "c-stabilize-sf", verb: "threatens" },
      { source: "obs-sync-errors", target: "r-sf-instability", verb: "supports" },
      { source: "src-support-crm", target: "obs-sync-errors", verb: "observed as" },
    ],
  },
};

// ---------------------------------------------------------------------
// Public fixture surface.
// ---------------------------------------------------------------------

const CATEGORY_ORDER: CategoryId[] = [
  "goals",
  "decisions",
  "commitments",
  "customers",
  "people",
  "risks",
  "systems",
  "finance",
];

export function fixtureOverview(mode: RelationshipBundle["mode"]): ModelOverview {
  const categories = CATEGORY_ORDER.map(buildCategory);
  const bundles = bundlesForMode(mode);
  const summary = {
    activeItemCount: categories.reduce((s, c) => s + c.itemCount, 0),
    changedTodayCount: 24,
    blockedCount: 6,
    contestedCount: categories.reduce((s, c) => s + c.contestedCount, 0),
    exposureAtRisk: 2_040_000,
    lastUpdatedAt: new Date().toISOString(),
  };
  return { summary, categories, relationshipBundles: bundles, mode };
}

export function fixtureCategoryFocus(
  cid: CategoryId,
  mode: RelationshipBundle["mode"],
): CategoryFocus {
  const category = buildCategory(cid);
  const all = bundlesForMode(mode);
  const relevant = all.filter(
    (b) => b.sourceCategoryId === cid || b.targetCategoryId === cid,
  );
  const relatedIds = new Set<CategoryId>();
  relevant.forEach((b) => {
    relatedIds.add(b.sourceCategoryId);
    relatedIds.add(b.targetCategoryId);
  });
  relatedIds.delete(cid);
  const relatedCategories = CATEGORY_ORDER.filter((c) => c !== cid).map((c) => ({
    ...buildCategory(c),
    isRelated: relatedIds.has(c),
  }));
  return {
    category,
    relatedCategories,
    relationshipBundles: relevant,
    topItems: byCategory(cid).slice(0, 8),
  };
}

export function fixtureRelationshipFocus(
  bundleIdStr: string,
): RelationshipFocus | null {
  const parts = bundleIdStr.split("__");
  if (parts.length !== 3) return null;
  const [src, verb, tgt] = parts as [CategoryId, string, CategoryId];
  const all = [
    ...bundlesForMode("impact"),
    ...bundlesForMode("dependencies"),
    ...bundlesForMode("ownership"),
    ...bundlesForMode("evidence"),
  ];
  const bundle = all.find((b) => b.id === bundleIdStr);
  if (!bundle) return null;
  const instances = instanceSeeds[bundleIdStr] ?? [];
  return {
    bundle,
    sourceCategory: {
      id: src,
      label: categoryMeta[src].label,
      colorToken: categoryMeta[src].colorToken,
    },
    targetCategory: {
      id: tgt,
      label: categoryMeta[tgt].label,
      colorToken: categoryMeta[tgt].colorToken,
    },
    instances,
    resolutionOpportunities:
      bundle.verb === "blocks"
        ? [
            { id: "ro-1", label: "Assign pricing owner" },
            { id: "ro-2", label: "Resolve Q3 scope tradeoff" },
            { id: "ro-3", label: "Delegate packaging decision" },
          ]
        : [],
  };
}

export function fixtureItemDetail(itemId: string): ItemDetail | null {
  const item = items[itemId];
  if (!item) return null;
  // Build neighbors from any bundle that references this item.
  const outgoing: RelationshipInstance[] = [];
  const incoming: RelationshipInstance[] = [];
  for (const bundleInsts of Object.values(instanceSeeds)) {
    for (const ri of bundleInsts) {
      if (ri.sourceItem.id === itemId) outgoing.push(ri);
      if (ri.targetItem.id === itemId) incoming.push(ri);
    }
  }
  // Compose the same relationshipCounts shape the backend returns so
  // the NodeZoom card renders the same humane summary line.
  const counts: Record<string, number> = {};
  for (const ri of outgoing) {
    counts[`out_${ri.verb}`] = (counts[`out_${ri.verb}`] ?? 0) + 1;
    counts[`${ri.verb}_${ri.targetItem.categoryId}`] =
      (counts[`${ri.verb}_${ri.targetItem.categoryId}`] ?? 0) + 1;
  }
  for (const ri of incoming) {
    counts[`in_${ri.verb}`] = (counts[`in_${ri.verb}`] ?? 0) + 1;
    counts[`in_${ri.sourceItem.categoryId}`] =
      (counts[`in_${ri.sourceItem.categoryId}`] ?? 0) + 1;
  }

  return {
    item: {
      ...item,
      authority: "mixed",
      evidenceSummary: "Triangulated from CRM, support tickets, and engineering capacity reports.",
      falsificationConditions: [
        "Sync errors drop below 0.5% for 14 days.",
        "Anchor renewal closes without sync-related blocker.",
      ],
      lifecycle: {
        createdAt: new Date(Date.now() - 14 * 86400_000).toISOString(),
        updatedAt: new Date(Date.now() - 21 * 60_000).toISOString(),
        lastConfirmedAt: new Date(Date.now() - 21 * 60_000).toISOString(),
      },
      metrics: {
        arrExposure: 2_040_000,
        affectedCustomers: 3,
      },
      relationshipCounts: counts,
    },
    neighbors: { incoming, outgoing },
    evidence: [
      { id: "ev-1", source: "Support", summary: "12 anchor support tickets reference sync failures this week." },
      { id: "ev-2", source: "CRM", summary: "Beacon CSM logged renewal blocker tied to sync reliability." },
    ],
    missingContext: [
      { reason: "Product usage data not connected.", impact: "Customer impact may be under-counted." },
    ],
  };
}

export function fixtureTrace(
  itemId: string,
  direction: "cause" | "consequence",
): Trace {
  const seed =
    direction === "cause"
      ? traceCauseSeeds[itemId]
      : traceConsequenceSeeds[itemId];
  if (seed) return seed;
  // Generic fallback: just show the item alone so the UI renders.
  const it = items[itemId];
  return {
    rootItemId: itemId,
    direction,
    nodes: it
      ? [{ id: it.id, assertion: it.assertion, shortLabel: it.shortLabel, kind: it.categoryId, step: 0 }]
      : [],
    edges: [],
  };
}

export const fixtureCategoryOrder = CATEGORY_ORDER;
