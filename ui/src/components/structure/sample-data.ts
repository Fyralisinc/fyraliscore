// Sample data for the Structure page (Part 16).
// 47 commitments distributed across the five territories.

import type {
  Commitment,
  LayerStripCounts,
  ShapeStatementToken,
} from "./types";

const today = new Date("2026-04-29");
const day = 24 * 60 * 60 * 1000;

function iso(date: Date): string {
  return date.toISOString().slice(0, 10);
}
function offset(days: number): string {
  return iso(new Date(today.getTime() + days * day));
}

// People + customer pool used to seed sample.
const owners = [
  "sarah",
  "marcus",
  "priya",
  "jen",
  "andre",
  "kim",
  "ravi",
  "lina",
];
const ownerDisplay: Record<string, string> = {
  sarah: "Sarah",
  marcus: "Marcus",
  priya: "Priya",
  jen: "Jen",
  andre: "Andre",
  kim: "Kim",
  ravi: "Ravi",
  lina: "Lina",
};
const customers = ["acme", "northwind", "globex", "initech", "umbrella"];

type Seed = {
  id: string;
  label: string;
  territory: Commitment["territory"];
  owner: string;
  due: number; // days from today (negative = overdue)
  status: Commitment["status"];
  priority: Commitment["priority"];
  stakeholder?: Commitment["stakeholder"];
  stakeholder_label?: string;
  customer?: string;
  traces_to?: string[];
  related?: string[];
  insight?: string;
};

// 47 seeds, biased to match the sample distribution + status mix.
const SEEDS: Seed[] = [
  // Strategic (4)
  { id: "c-101", label: "FY27 strategic narrative draft", territory: "strategic", owner: "sarah", due: 38, status: "on-track", priority: "high", stakeholder: "internal", stakeholder_label: "Exec staff" },
  { id: "c-102", label: "Pricing v3 alignment with finance", territory: "strategic", owner: "andre", due: 22, status: "on-track", priority: "high" },
  { id: "c-103", label: "Board memo — platform thesis", territory: "strategic", owner: "sarah", due: 12, status: "slipping", priority: "high", insight: "this commitment has slipped twice — once in February, once now. Worth attention." },
  { id: "c-104", label: "Competitive landscape refresh", territory: "strategic", owner: "priya", due: 60, status: "on-track", priority: "standard" },

  // Customer-facing (28)
  { id: "c-acme-renewal", label: "Acme renewal — Q3 contract close", territory: "customer-facing", owner: "sarah", due: 18, status: "at-risk", priority: "high", stakeholder: "customer", stakeholder_label: "Acme — Erin Park", customer: "acme", traces_to: ["d-12"], insight: "Sarah's load may be a factor in the recent slip; I noted this in Today." },
  { id: "c-201", label: "Northwind quarterly business review", territory: "customer-facing", owner: "marcus", due: 9, status: "on-track", priority: "high", customer: "northwind" },
  { id: "c-202", label: "Globex onboarding playbook handoff", territory: "customer-facing", owner: "marcus", due: 4, status: "on-track", priority: "standard", customer: "globex" },
  { id: "c-203", label: "Token bucket scoping (Acme)", territory: "customer-facing", owner: "marcus", due: 14, status: "on-track", priority: "standard", customer: "acme", related: ["c-187"] },
  { id: "c-204", label: "Initech success-plan revision", territory: "customer-facing", owner: "priya", due: 11, status: "slipping", priority: "standard", customer: "initech" },
  { id: "c-205", label: "Umbrella account expansion deck", territory: "customer-facing", owner: "sarah", due: 6, status: "on-track", priority: "high", customer: "umbrella" },
  { id: "c-206", label: "Acme — security review prep", territory: "customer-facing", owner: "marcus", due: 19, status: "on-track", priority: "standard", customer: "acme" },
  { id: "c-207", label: "Northwind onsite coordination", territory: "customer-facing", owner: "jen", due: 7, status: "on-track", priority: "standard", customer: "northwind" },
  { id: "c-208", label: "Customer health scoring rollout", territory: "customer-facing", owner: "priya", due: 33, status: "on-track", priority: "standard" },
  { id: "c-209", label: "Globex contract red-line review", territory: "customer-facing", owner: "andre", due: 21, status: "on-track", priority: "standard", customer: "globex" },
  { id: "c-210", label: "Quarterly NPS readout", territory: "customer-facing", owner: "kim", due: 27, status: "on-track", priority: "low" },
  { id: "c-211", label: "Architecture review — Acme", territory: "customer-facing", owner: "marcus", due: 28, status: "on-track", priority: "standard", customer: "acme", related: ["c-187", "c-203"] },
  { id: "c-212", label: "Initech feature gap analysis", territory: "customer-facing", owner: "priya", due: 41, status: "on-track", priority: "standard", customer: "initech" },
  { id: "c-213", label: "Umbrella exec briefing", territory: "customer-facing", owner: "sarah", due: 16, status: "on-track", priority: "high", customer: "umbrella" },
  { id: "c-214", label: "Northwind enablement materials", territory: "customer-facing", owner: "jen", due: 24, status: "on-track", priority: "standard", customer: "northwind" },
  { id: "c-215", label: "Customer advisory board agenda", territory: "customer-facing", owner: "sarah", due: 47, status: "on-track", priority: "standard" },
  { id: "c-216", label: "Globex feature parity gap", territory: "customer-facing", owner: "ravi", due: 13, status: "on-track", priority: "standard", customer: "globex" },
  { id: "c-217", label: "Acme support escalation closeout", territory: "customer-facing", owner: "marcus", due: -2, status: "slipping", priority: "standard", customer: "acme" },
  { id: "c-218", label: "Initech executive QBR", territory: "customer-facing", owner: "priya", due: 35, status: "on-track", priority: "standard", customer: "initech" },
  { id: "c-219", label: "Umbrella contract renewal scoping", territory: "customer-facing", owner: "sarah", due: 52, status: "on-track", priority: "high", customer: "umbrella" },
  { id: "c-220", label: "Northwind training rollout", territory: "customer-facing", owner: "jen", due: 38, status: "on-track", priority: "low" },
  { id: "c-221", label: "Globex security questionnaire", territory: "customer-facing", owner: "andre", due: 8, status: "on-track", priority: "standard", customer: "globex" },
  { id: "c-222", label: "Acme — adoption metrics review", territory: "customer-facing", owner: "marcus", due: 31, status: "on-track", priority: "standard", customer: "acme" },
  { id: "c-223", label: "Initech invoicing reconciliation", territory: "customer-facing", owner: "kim", due: 15, status: "on-track", priority: "low", customer: "initech" },
  { id: "c-224", label: "Umbrella enablement roadshow", territory: "customer-facing", owner: "lina", due: 44, status: "on-track", priority: "standard", customer: "umbrella" },
  { id: "c-225", label: "Northwind reference call setup", territory: "customer-facing", owner: "jen", due: 5, status: "on-track", priority: "standard", customer: "northwind" },
  { id: "c-226", label: "Globex billing dispute closeout", territory: "customer-facing", owner: "andre", due: 17, status: "on-track", priority: "standard", customer: "globex" },
  { id: "c-227", label: "Acme co-marketing draft", territory: "customer-facing", owner: "sarah", due: 56, status: "on-track", priority: "low", customer: "acme" },

  // Technical Infrastructure (8)
  { id: "c-187", label: "Implement distributed rate limiter using Redis", territory: "technical-infrastructure", owner: "marcus", due: 16, status: "on-track", priority: "standard", stakeholder: "internal", stakeholder_label: "Platform team", traces_to: ["d-5"], related: ["c-203", "c-211"], insight: "this commitment is currently part of d-5's drift cluster." },
  { id: "c-301", label: "Postgres major version upgrade", territory: "technical-infrastructure", owner: "ravi", due: 49, status: "on-track", priority: "high" },
  { id: "c-302", label: "Observability rollout — phase 2", territory: "technical-infrastructure", owner: "ravi", due: 25, status: "on-track", priority: "standard" },
  { id: "c-303", label: "CI pipeline rewrite", territory: "technical-infrastructure", owner: "andre", due: 12, status: "on-track", priority: "standard" },
  { id: "c-304", label: "Search index sharding", territory: "technical-infrastructure", owner: "marcus", due: 38, status: "on-track", priority: "standard" },
  { id: "c-305", label: "Auth service split", territory: "technical-infrastructure", owner: "ravi", due: -5, status: "blocked", priority: "high", insight: "this is the only commitment currently tied to the auth rebuild — see deprio recommendation in Today." },
  { id: "c-306", label: "Edge caching pilot", territory: "technical-infrastructure", owner: "lina", due: 22, status: "on-track", priority: "low" },
  { id: "c-307", label: "Background job queue migration", territory: "technical-infrastructure", owner: "marcus", due: 30, status: "on-track", priority: "standard" },

  // Internal Operations (5)
  { id: "c-401", label: "Q2 OKR ratification", territory: "internal-operations", owner: "sarah", due: 6, status: "on-track", priority: "high" },
  { id: "c-402", label: "Vendor consolidation audit", territory: "internal-operations", owner: "kim", due: 32, status: "on-track", priority: "standard" },
  { id: "c-403", label: "Travel policy update", territory: "internal-operations", owner: "kim", due: 14, status: "on-track", priority: "low" },
  { id: "c-404", label: "Finance close — May", territory: "internal-operations", owner: "andre", due: 20, status: "on-track", priority: "standard" },
  { id: "c-405", label: "All-hands logistics", territory: "internal-operations", owner: "lina", due: 9, status: "on-track", priority: "low" },

  // Personnel (2)
  { id: "c-501", label: "Eng director search closeout", territory: "personnel", owner: "sarah", due: 28, status: "on-track", priority: "high" },
  { id: "c-502", label: "Mid-year review calibration", territory: "personnel", owner: "sarah", due: 42, status: "on-track", priority: "standard" },
];

export const SAMPLE_COMMITMENTS: Commitment[] = SEEDS.map((s) => ({
  id: s.id,
  label: s.label,
  territory: s.territory,
  owner: s.owner,
  owner_display: ownerDisplay[s.owner] ?? s.owner,
  due_date: offset(s.due),
  created_date: offset(-Math.max(7, Math.floor(Math.random() * 60))),
  status: s.status,
  priority: s.priority,
  stakeholder: s.stakeholder ?? (s.customer ? "customer" : "internal"),
  stakeholder_label:
    s.stakeholder_label ??
    (s.customer
      ? `${s.customer.charAt(0).toUpperCase() + s.customer.slice(1)} — primary contact`
      : "Internal"),
  customer: s.customer,
  traces_to: s.traces_to ?? [],
  related: s.related ?? [],
  progress: s.priority === "high" ? "3 of 5 milestones" : "in progress",
  substrate_insight: s.insight,
  activity: [
    { date: offset(-5), desc: "scope confirmed" },
    { date: offset(-12), desc: "milestone update logged" },
    { date: offset(-26), desc: "created" },
  ],
}));

export const SAMPLE_OWNERS: { id: string; label: string }[] = owners.map(
  (o) => ({ id: o, label: ownerDisplay[o] ?? o })
);
export const SAMPLE_CUSTOMERS: { id: string; label: string }[] = customers.map(
  (c) => ({ id: c, label: c.charAt(0).toUpperCase() + c.slice(1) })
);

export const SAMPLE_LAYER_COUNTS: LayerStripCounts = {
  commits: { active: SAMPLE_COMMITMENTS.length, at_risk: 5 },
  decisions: { in_force: 12, in_drift: 3 },
  people: { count: 23, teams: 5 },
  customers: { active: 14, healthy_pct: 76 },
  model: { calibration: 0.81, contested: 2 },
};

// Marked-up shape statement for Section 4.
export const SAMPLE_SHAPE_STATEMENT: ShapeStatementToken[] = [
  {
    kind: "ref",
    ref: { type: "territory", id: "customer-facing", text: "Customer-facing work" },
  },
  { kind: "text", text: " is dominating this quarter — 28 of 47 commitments. " },
  { kind: "ref", ref: { type: "person", id: "sarah", text: "Sarah" } },
  { kind: "text", text: " and " },
  { kind: "ref", ref: { type: "person", id: "marcus", text: "Marcus" } },
  { kind: "text", text: " are carrying most of the load. The " },
  {
    kind: "ref",
    ref: { type: "commitment", id: "c-acme-renewal", text: "Acme renewal" },
  },
  { kind: "text", text: " sits at the center of what I'm watching." },
];

export const SAMPLE_RECENT_CHANGE: {
  direction: "up" | "down" | "mixed" | "flat";
  text: string;
} = { direction: "down", text: "3 newly slipping" };
