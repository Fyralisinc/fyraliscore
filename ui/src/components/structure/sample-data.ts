// Sample data for the Structure page (Part 16).
// 47 commitments distributed across the five territories.

import type {
  Commitment,
  CommitmentEdges,
  DecisionRef,
  GoalLearnings,
  GoalRef,
  LearnedPattern,
  PersonProfile,
  ResourceRef,
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

// ─────────────────────────────────────────────────────────────────
// Goals · Decisions · Resources — relational substrate the commitment
// graph hangs off of.
// ─────────────────────────────────────────────────────────────────

export const SAMPLE_GOALS: GoalRef[] = [
  { id: "g-arr",       label: "FY27 ARR plan",            altitude: "strategic" },
  { id: "g-platform",  label: "Platform thesis",          altitude: "strategic" },
  { id: "g-retention", label: "Retain top 5 accounts",    altitude: "strategic" },
  { id: "g-velocity",  label: "Ship velocity (eng)",      altitude: "operational" },
  { id: "g-trust",     label: "SOC2 + customer trust",    altitude: "operational" },
  { id: "g-team",      label: "Team scale-out",           altitude: "operational" },
];

export const SAMPLE_DECISIONS: DecisionRef[] = [
  { id: "d-12", label: "Tier-1 customers ratify scope",   state: "in-force" },
  { id: "d-22", label: "Token-bucket as default",         state: "drifting" },
  { id: "d-31", label: "Eng-led security posture",        state: "in-force" },
  { id: "d-44", label: "No new framework adoption Q2",    state: "in-force" },
  { id: "d-50", label: "Quarterly business reviews",      state: "in-force" },
];

export const SAMPLE_RESOURCES: ResourceRef[] = [
  { id: "r-eng-cap",    label: "Eng capacity",            kind: "human" },
  { id: "r-cs-cap",     label: "CS capacity",             kind: "human" },
  { id: "r-arr",        label: "ARR",                     kind: "financial" },
  { id: "r-runway",     label: "Runway",                  kind: "financial" },
  { id: "r-platform",   label: "Platform infra",          kind: "technical" },
  { id: "r-pipeline",   label: "CI/CD pipeline",          kind: "technical" },
];

// Commitment → relational edges. Drives the graph view.
// We keep this as a derived map by id so per-commitment lookups are O(1).
const EDGE_OVERRIDES: Record<string, Partial<CommitmentEdges>> = {
  "c-101": { contributes_to: ["g-platform", "g-arr"], constrained_by: ["d-44"], consumes: ["r-eng-cap"], contributors: ["andre"] },
  "c-102": { contributes_to: ["g-arr"], constrained_by: ["d-50"], consumes: ["r-arr"] },
  "c-103": { contributes_to: ["g-platform"], constrained_by: ["d-44"], consumes: ["r-eng-cap"] },
  "c-104": { contributes_to: ["g-platform"], consumes: ["r-eng-cap"] },
  "c-acme-renewal": { contributes_to: ["g-arr", "g-retention"], constrained_by: ["d-12"], consumes: ["r-cs-cap", "r-arr"], contributors: ["marcus", "priya"] },
  "c-201": { contributes_to: ["g-retention"], constrained_by: ["d-50"], consumes: ["r-cs-cap"] },
  "c-202": { contributes_to: ["g-retention"], consumes: ["r-cs-cap"] },
  "c-203": { contributes_to: ["g-velocity"], constrained_by: ["d-22"], consumes: ["r-eng-cap", "r-platform"] },
  "c-204": { contributes_to: ["g-retention"], consumes: ["r-cs-cap"] },
  "c-205": { contributes_to: ["g-arr"], consumes: ["r-cs-cap"] },
  "c-206": { contributes_to: ["g-trust"], constrained_by: ["d-31"], consumes: ["r-eng-cap"] },
  "c-207": { contributes_to: ["g-retention"], consumes: ["r-cs-cap"] },
  "c-208": { contributes_to: ["g-retention", "g-velocity"], consumes: ["r-cs-cap"] },
  "c-209": { contributes_to: ["g-arr"], constrained_by: ["d-12"], consumes: ["r-cs-cap"] },
  "c-210": { contributes_to: ["g-retention"], consumes: ["r-cs-cap"] },
  "c-211": { contributes_to: ["g-velocity"], constrained_by: ["d-22"], consumes: ["r-eng-cap"] },
  "c-212": { contributes_to: ["g-retention"], consumes: ["r-cs-cap"] },
  "c-301": { contributes_to: ["g-velocity"], consumes: ["r-eng-cap", "r-pipeline"] },
  "c-302": { contributes_to: ["g-trust"], constrained_by: ["d-31"], consumes: ["r-eng-cap"] },
  "c-303": { contributes_to: ["g-velocity"], consumes: ["r-eng-cap", "r-platform"] },
  "c-304": { contributes_to: ["g-velocity"], consumes: ["r-eng-cap"] },
  "c-305": { contributes_to: ["g-trust"], consumes: ["r-eng-cap"] },
  "c-306": { contributes_to: ["g-velocity"], consumes: ["r-eng-cap", "r-pipeline"] },
  "c-307": { contributes_to: ["g-velocity"], consumes: ["r-eng-cap", "r-platform"] },
  "c-401": { contributes_to: ["g-team"], consumes: ["r-cs-cap"] },
  "c-402": { contributes_to: ["g-team"], consumes: ["r-cs-cap"] },
  "c-403": { contributes_to: ["g-team"] },
  "c-404": { contributes_to: ["g-arr"], consumes: ["r-runway"] },
  "c-405": { contributes_to: ["g-team"] },
  "c-501": { contributes_to: ["g-team"], consumes: ["r-eng-cap"] },
  "c-502": { contributes_to: ["g-team"], consumes: ["r-cs-cap"] },
};

function edgesFor(id: string, traces_to: string[], related: string[]): CommitmentEdges {
  const o = EDGE_OVERRIDES[id] ?? {};
  return {
    contributes_to: o.contributes_to ?? [],
    constrained_by: o.constrained_by ?? traces_to ?? [],
    consumes: o.consumes ?? [],
    contributors: o.contributors ?? [],
    // related commitments are still on `commitment.related[]` so we don't
    // duplicate; the graph reads them from there.
    ...{},
  } as CommitmentEdges;
}

// Layer the edges back onto each commitment so the rest of the page can
// rely on `commitment.edges` without an extra lookup.
SAMPLE_COMMITMENTS.forEach((c) => {
  c.edges = edgesFor(c.id, c.traces_to, c.related);
});

// ─────────────────────────────────────────────────────────────────
// Commitment-level learnings — only seeded for commitments where the
// system has accumulated enough signal to be worth surfacing. Most
// commitments leave this blank, and the panel falls back to a generic
// "thin signal" message.
// ─────────────────────────────────────────────────────────────────

const COMMITMENT_LEARNINGS: Record<string, LearnedPattern[]> = {
  "c-acme-renewal": [
    {
      id: "cl-acme-conc",
      statement: "At-risk concentration: this commitment shares Sarah's bandwidth with two other high-priority items.",
      strength: 0.82,
      evidence: [
        { when: "2026-04-29", text: "Sarah currently owns c-103 and c-205 in addition to this — both high-priority.", ref: { kind: "commitment", id: "c-103" }, weight: 0.9 },
        { when: "2026-04-22", text: "Last status update slipped 2 days; correlates with parallel narrative work.", ref: { kind: "commitment", id: "c-103" }, weight: 0.7 },
      ],
    },
    {
      id: "cl-acme-decision",
      statement: "Constrained by the Tier-1 customer scope decision (d-12), which has held firm for two quarters.",
      strength: 0.88,
      evidence: [
        { when: "2026-Q1", text: "d-12 ratification has gone unchallenged across the renewal cycle.", ref: { kind: "decision", id: "d-12" } },
        { when: "ongoing", text: "Renewal scope tracks d-12 directly — any change here would unblock larger commit." },
      ],
    },
    {
      id: "cl-acme-pair",
      statement: "Strongest historical close pattern: Sarah + Marcus + Priya as a triad.",
      strength: 0.71,
      evidence: [
        { when: "2025-Q4", text: "Last Acme renewal closed cleanly with the same three named as contributors." },
        { when: "ongoing", text: "Marcus carries the Acme account context (5 of 7 Acme commitments)." },
      ],
    },
  ],
  "c-103": [
    {
      id: "cl-103-repeat",
      statement: "This commitment has slipped twice on the same fact pattern: heavy concurrent load on Sarah.",
      strength: 0.79,
      evidence: [
        { when: "2026-04-22", text: "Slipped under 3 concurrent high-priority commitments." },
        { when: "2026-02-14", text: "Earlier slip in the same cycle, same load profile." },
      ],
    },
    {
      id: "cl-103-platform",
      statement: "Tied to the platform thesis (g-platform); slippage here delays board narrative directly.",
      strength: 0.68,
      evidence: [
        { when: "ongoing", text: "Contributes to g-platform — the only commitment doing so this quarter.", ref: { kind: "goal", id: "g-platform" } },
      ],
    },
  ],
  "c-305": [
    {
      id: "cl-305-blocked",
      statement: "Blocked: this is the only commitment currently tied to the auth rebuild — see deprio recommendation in Today.",
      strength: 0.92,
      evidence: [
        { when: "2026-04-29", text: "Status: blocked. No parallel-track recovery possible (Ravi pattern).", ref: { kind: "commitment", id: "c-305" } },
        { when: "2026-04-15", text: "Block surfaced 2 weeks ago; downstream c-187 has been waiting silently.", ref: { kind: "commitment", id: "c-187" } },
      ],
    },
    {
      id: "cl-305-stall",
      statement: "Pattern: when this commitment blocks, Ravi's other infra commitments stall.",
      strength: 0.74,
      evidence: [
        { when: "2026-04", text: "Postgres upgrade (c-301) progress flat for 2 weeks, correlates with this block.", ref: { kind: "commitment", id: "c-301" } },
        { when: "2025-Q3", text: "Same correlation observed during prior pipeline block." },
      ],
    },
  ],
  "c-187": [
    {
      id: "cl-187-drift",
      statement: "This commitment is currently part of d-5's drift cluster — token-bucket assumptions are being revisited.",
      strength: 0.81,
      evidence: [
        { when: "ongoing", text: "Constrained by d-5; d-22 (token-bucket default) is in 'drifting' state.", ref: { kind: "decision", id: "d-22" } },
        { when: "2026-Q1", text: "Three commitments in this cluster (c-187, c-203, c-211) share the assumption." },
      ],
    },
    {
      id: "cl-187-pair",
      statement: "Highest-leverage when explicitly paired with Marcus — solo platform work historically slips.",
      strength: 0.69,
      evidence: [
        { when: "2025-Q4", text: "Joint Ravi+Marcus work on this commitment closed cleanly." },
        { when: "2025-Q3", text: "Solo Ravi platform commitments slipped 2 of 5 in the same window." },
      ],
    },
  ],
  "c-301": [
    {
      id: "cl-301-loadbearing",
      statement: "Load-bearing for ship velocity: blocks two downstream commitments if it slips.",
      strength: 0.73,
      evidence: [
        { when: "ongoing", text: "Background job queue migration (c-307) and search index sharding (c-304) both depend on the upgrade landing first.", ref: { kind: "commitment", id: "c-307" } },
      ],
    },
    {
      id: "cl-301-ravi",
      statement: "Sits in Ravi's heavy-load queue — see his queue-stall pattern.",
      strength: 0.66,
      evidence: [
        { when: "trailing 90d", text: "60% of technical-infrastructure load currently routes to Ravi." },
      ],
    },
  ],
  "c-217": [
    {
      id: "cl-217-marcus",
      statement: "Acme support escalations consistently route to Marcus on arrival — not surprising it sits with him.",
      strength: 0.86,
      evidence: [
        { when: "trailing year", text: "Marcus owns 5 of 7 active Acme commitments." },
        { when: "2026-Q1", text: "Last 4 Acme escalations all reassigned to Marcus within 24h." },
      ],
    },
  ],
};

SAMPLE_COMMITMENTS.forEach((c) => {
  const ll = COMMITMENT_LEARNINGS[c.id];
  if (ll) c.learnings = ll;
});

// ─────────────────────────────────────────────────────────────────
// Goal-level learnings — keyed by goal id. Same shape as person
// patterns; ungated goals fall through to a "thin signal" message.
// ─────────────────────────────────────────────────────────────────

export const SAMPLE_GOAL_LEARNINGS: Record<string, GoalLearnings> = {
  "g-arr": {
    calibration: 0.82,
    recent_observation: "ARR plan is anchored by the Acme renewal — that single commitment moves the goal more than any other.",
    patterns: [
      {
        id: "gl-arr-acme",
        statement: "Acme renewal carries disproportionate weight on this goal — its slip here moves the whole forecast.",
        strength: 0.91,
        evidence: [
          { when: "ongoing", text: "c-acme-renewal is at-risk and contributes to g-arr.", ref: { kind: "commitment", id: "c-acme-renewal" } },
          { when: "2026-Q1", text: "5 of 6 active commitments contributing to g-arr are customer-facing." },
        ],
      },
      {
        id: "gl-arr-spread",
        statement: "Concentration risk: 3 of 6 contributing commitments are owned by Sarah personally.",
        strength: 0.76,
        evidence: [
          { when: "ongoing", text: "Sarah owns c-acme-renewal, c-205, and c-219.", ref: { kind: "commitment", id: "c-205" } },
        ],
      },
    ],
  },
  "g-platform": {
    calibration: 0.64,
    recent_observation: "Platform thesis depends on a slipping board memo — narrative momentum is fragile this quarter.",
    patterns: [
      {
        id: "gl-platform-fragile",
        statement: "Single point of failure: c-103 (board memo) is the only narrative commitment currently driving this goal.",
        strength: 0.83,
        evidence: [
          { when: "2026-04-22", text: "c-103 is slipping; it's the only commitment with substantive contribution to g-platform.", ref: { kind: "commitment", id: "c-103" } },
        ],
      },
      {
        id: "gl-platform-d44",
        statement: "Constrained by d-44 (no new framework adoption); progress here cannot route around the decision.",
        strength: 0.71,
        evidence: [
          { when: "2025-Q4", text: "d-44 has held firm against two adoption proposals.", ref: { kind: "decision", id: "d-44" } },
        ],
      },
    ],
  },
  "g-retention": {
    calibration: 0.86,
    recent_observation: "Retention is broadly on-track but Initech is the soft spot — every Initech commitment slips at least once.",
    patterns: [
      {
        id: "gl-retention-initech",
        statement: "Initech account is the dominant slip source on this goal.",
        strength: 0.83,
        evidence: [
          { when: "ongoing", text: "c-204 (Initech success-plan) is currently slipping.", ref: { kind: "commitment", id: "c-204" } },
          { when: "2025-Q4", text: "c-212 (feature gap analysis) slipped twice last quarter.", ref: { kind: "commitment", id: "c-212" } },
        ],
      },
      {
        id: "gl-retention-jen",
        statement: "Northwind contribution is the most reliable lane — Jen's single-account focus shows here.",
        strength: 0.81,
        evidence: [
          { when: "trailing 90d", text: "All Northwind commitments on-track over the trailing window." },
        ],
      },
    ],
  },
  "g-velocity": {
    calibration: 0.72,
    recent_observation: "Velocity goal is bottlenecked on Ravi's infra queue — auth split block radiates outward.",
    patterns: [
      {
        id: "gl-velocity-ravi",
        statement: "Ravi's queue-stall pattern dominates this goal: when one infra commitment blocks, the whole goal stalls.",
        strength: 0.79,
        evidence: [
          { when: "2026-04", text: "Auth split (c-305) blocked → Postgres upgrade (c-301) flat for 2 weeks.", ref: { kind: "commitment", id: "c-305" } },
        ],
      },
      {
        id: "gl-velocity-d22",
        statement: "Drifting decision d-22 (token-bucket default) is reshaping this goal's commitment cluster.",
        strength: 0.68,
        evidence: [
          { when: "ongoing", text: "Three commitments (c-187, c-203, c-211) sit on the drifting assumption.", ref: { kind: "decision", id: "d-22" } },
        ],
      },
    ],
  },
  "g-trust": {
    calibration: 0.74,
    recent_observation: "Trust goal is steady but auth split block is starting to tug at the security narrative.",
    patterns: [
      {
        id: "gl-trust-auth",
        statement: "Security posture decisions hold (d-31 in-force), but auth implementation is the weak link.",
        strength: 0.77,
        evidence: [
          { when: "ongoing", text: "d-31 (eng-led security posture) in-force; c-305 (auth split) blocked.", ref: { kind: "decision", id: "d-31" } },
        ],
      },
    ],
  },
  "g-team": {
    calibration: 0.59,
    recent_observation: "Team goal has thin signal — most contributing commitments are recurring ops with low stretch.",
    patterns: [
      {
        id: "gl-team-thin",
        statement: "Limited signal — 4 of 5 contributing commitments are low-priority recurring work.",
        strength: 0.61,
        evidence: [
          { when: "ongoing", text: "Eng director search (c-501) is the only high-priority contributor.", ref: { kind: "commitment", id: "c-501" } },
        ],
      },
    ],
  },
};

export const SAMPLE_OWNERS: { id: string; label: string }[] = owners.map(
  (o) => ({ id: o, label: ownerDisplay[o] ?? o })
);
export const SAMPLE_CUSTOMERS: { id: string; label: string }[] = customers.map(
  (c) => ({ id: c, label: c.charAt(0).toUpperCase() + c.slice(1) })
);

// ─────────────────────────────────────────────────────────────────
// People — patterns and learnings the system has accumulated about
// each team member. Curated for the demo; in production these are
// derived continuously from commitment activity, drift events, and
// communication logs.
// ─────────────────────────────────────────────────────────────────

export const SAMPLE_PEOPLE: PersonProfile[] = [
  {
    id: "sarah",
    label: "Sarah",
    role: "CEO · Founder",
    recent_observation: "Two slips this quarter — both narrative work under high concurrent load.",
    calibration: 0.78,
    patterns: [
      {
        id: "sarah-load-slip",
        statement: "Slips on writing-heavy commitments when carrying ≥3 high-priority items in parallel.",
        strength: 0.82,
        evidence: [
          { when: "2026-04-22", text: "Board memo (c-103) slipped while Acme renewal + Umbrella deck were both in flight.", ref: { kind: "commitment", id: "c-103" }, weight: 0.9 },
          { when: "2026-02-14", text: "Same memo cycle slipped in February under a similar 4-item high-pri load.", ref: { kind: "commitment", id: "c-103" }, weight: 0.7 },
          { when: "2025-11-08", text: "FY27 narrative draft (c-101) shifted by a week when Q4 close + a renewal stacked.", ref: { kind: "commitment", id: "c-101" }, weight: 0.6 },
        ],
      },
      {
        id: "sarah-renewal-anchor",
        statement: "Anchors customer-facing renewals personally — Acme + Umbrella always route to her.",
        strength: 0.91,
        evidence: [
          { when: "ongoing", text: "Owns Acme renewal (c-acme-renewal) and Umbrella expansion deck (c-205) directly.", ref: { kind: "commitment", id: "c-acme-renewal" } },
          { when: "ongoing", text: "Umbrella exec briefing (c-213) and renewal scoping (c-219) both routed to Sarah.", ref: { kind: "commitment", id: "c-219" } },
          { when: "2025-Q4", text: "Acme co-marketing draft (c-227) re-assigned from Marcus to Sarah within 4 days.", ref: { kind: "commitment", id: "c-227" } },
        ],
      },
      {
        id: "sarah-decision-stickiness",
        statement: "Decisions ratified by Sarah hold; rarely reopens once a call is made.",
        strength: 0.74,
        evidence: [
          { when: "2026-Q1", text: "Tier-1 customer scope ratification (d-12) has held without revisit for two quarters.", ref: { kind: "decision", id: "d-12" } },
          { when: "2025-Q4", text: "No-new-framework decision (d-44) survived two adoption proposals from eng.", ref: { kind: "decision", id: "d-44" } },
        ],
      },
      {
        id: "sarah-cadence",
        statement: "Strongest cadence: short-cycle exec memos. Weaker on multi-week strategic drafts.",
        strength: 0.63,
        evidence: [
          { when: "trailing 90d", text: "11 of 12 short memos (≤7 day cycle) closed on time." },
          { when: "trailing 90d", text: "2 of 4 multi-week strategic drafts have slipped at least once." },
        ],
      },
    ],
  },
  {
    id: "marcus",
    label: "Marcus",
    role: "Eng Lead · Customer Platform",
    recent_observation: "0 slips in the last 60 days across 8 active commitments.",
    calibration: 0.92,
    patterns: [
      {
        id: "marcus-closer",
        statement: "Most reliable closer on the team — closes commitments at or before their stated due date 89% of the time.",
        strength: 0.94,
        evidence: [
          { when: "trailing 90d", text: "32 of 36 commitments closed on or before due date." },
          { when: "2026-04-18", text: "Globex onboarding playbook (c-202) shipped 3 days early.", ref: { kind: "commitment", id: "c-202" } },
          { when: "2026-04-02", text: "Northwind QBR prep (c-201) closed on the day, cleanly.", ref: { kind: "commitment", id: "c-201" } },
        ],
      },
      {
        id: "marcus-underclaim",
        statement: "Defaults to scoping things small; tends to under-claim what he ships.",
        strength: 0.68,
        evidence: [
          { when: "2026-03-30", text: "Token bucket scoping (c-203) shipped with hardening work he didn't list.", ref: { kind: "commitment", id: "c-203" } },
          { when: "2026-02-11", text: "Search index sharding (c-304) absorbed an unscoped index migration.", ref: { kind: "commitment", id: "c-304" } },
        ],
      },
      {
        id: "marcus-acme-context",
        statement: "Carries Acme account context — most Acme threads route through him.",
        strength: 0.86,
        evidence: [
          { when: "ongoing", text: "Owns 5 of 7 active Acme commitments (c-203, c-206, c-211, c-217, c-222).", ref: { kind: "commitment", id: "c-211" } },
          { when: "2026-Q1", text: "Acme support escalations (c-217) consistently re-assigned to Marcus on arrival.", ref: { kind: "commitment", id: "c-217" } },
        ],
      },
      {
        id: "marcus-quiet-block",
        statement: "Slow to escalate when blocked. Ravi's auth work has been blocking him quietly for 3 weeks.",
        strength: 0.71,
        evidence: [
          { when: "2026-04-29", text: "Auth service split (c-305) is blocked; downstream work on c-187 is waiting silently.", ref: { kind: "commitment", id: "c-305" } },
          { when: "2026-Q4 2025", text: "Two prior block events resolved only after 14+ days, never escalated by Marcus." },
        ],
      },
    ],
  },
  {
    id: "priya",
    label: "Priya",
    role: "Head of Customer Success",
    recent_observation: "Initech success-plan revision is currently slipping (1 of 4).",
    calibration: 0.71,
    patterns: [
      {
        id: "priya-discovery-strong",
        statement: "Strong on diagnostic / discovery work; weaker on synthesis deliverables (decks, write-ups).",
        strength: 0.76,
        evidence: [
          { when: "trailing 90d", text: "Customer health scoring rollout (c-208) discovery phase closed early.", ref: { kind: "commitment", id: "c-208" } },
          { when: "2026-04-19", text: "Initech success-plan revision (c-204) is slipping at the synthesis stage.", ref: { kind: "commitment", id: "c-204" } },
        ],
      },
      {
        id: "priya-initech",
        statement: "Initech is her hardest account — historically every Initech commitment slips at least once.",
        strength: 0.83,
        evidence: [
          { when: "ongoing", text: "Initech success plan (c-204) slipped this quarter.", ref: { kind: "commitment", id: "c-204" } },
          { when: "2025-Q4", text: "Initech feature gap analysis (c-212) slipped twice.", ref: { kind: "commitment", id: "c-212" } },
          { when: "2025-Q3", text: "Initech executive QBR (c-218) slipped once before closing.", ref: { kind: "commitment", id: "c-218" } },
        ],
      },
      {
        id: "priya-marcus-pair",
        statement: "Best collaborator with Marcus; pairs well on technical-customer commitments.",
        strength: 0.69,
        evidence: [
          { when: "2026-Q1", text: "Acme renewal (c-acme-renewal) lists both as contributors; trajectory steady.", ref: { kind: "commitment", id: "c-acme-renewal" } },
          { when: "2025-Q4", text: "Three joint Marcus+Priya commitments closed on time across 2025-Q4." },
        ],
      },
    ],
  },
  {
    id: "jen",
    label: "Jen",
    role: "Customer Success · Northwind",
    recent_observation: "All Northwind commitments on track. Steady cadence the last 90 days.",
    calibration: 0.85,
    patterns: [
      {
        id: "jen-focus-quality",
        statement: "Single-account focus (Northwind) — execution quality scales with that focus.",
        strength: 0.81,
        evidence: [
          { when: "trailing 90d", text: "All 5 Northwind commitments on-track (c-207, c-214, c-220, c-225)." },
          { when: "ongoing", text: "Reference call setup (c-225) and onsite coordination (c-207) both running early.", ref: { kind: "commitment", id: "c-207" } },
        ],
      },
      {
        id: "jen-logistics",
        statement: "Comfortable owning logistics-heavy commitments end-to-end.",
        strength: 0.72,
        evidence: [
          { when: "2026-04", text: "Northwind training rollout (c-220) — coordinated 4 sites without intervention.", ref: { kind: "commitment", id: "c-220" } },
          { when: "2025-Q4", text: "Owned all-hands logistics (c-405) the previous quarter — no slips." },
        ],
      },
      {
        id: "jen-low-signal",
        statement: "Underused on cross-account work — system has low signal outside Northwind.",
        strength: 0.55,
        evidence: [
          { when: "trailing 180d", text: "0 commitments outside the Northwind account in the last 6 months." },
        ],
      },
    ],
  },
  {
    id: "andre",
    label: "Andre",
    role: "Finance + Eng Ops",
    recent_observation: "Splits time across finance close and CI work; usually finishes both.",
    calibration: 0.74,
    patterns: [
      {
        id: "andre-cross-fn",
        statement: "Cross-functional anchor — only person currently bridging finance and eng-ops.",
        strength: 0.88,
        evidence: [
          { when: "ongoing", text: "Holds CI rewrite (c-303) and Finance close (c-404) simultaneously.", ref: { kind: "commitment", id: "c-303" } },
          { when: "2025-Q4", text: "Globex contract red-line (c-209) bridged legal + finance review." },
        ],
      },
      {
        id: "andre-deadlines",
        statement: "Performs better when commitments have hard external deadlines (close dates, contract dates).",
        strength: 0.73,
        evidence: [
          { when: "trailing 90d", text: "Finance closes (c-404) — 6 of 6 closed on date over the trailing year." },
          { when: "2026-Q1", text: "Globex billing dispute (c-226) closed on the contract-aligned deadline.", ref: { kind: "commitment", id: "c-226" } },
        ],
      },
      {
        id: "andre-ci-velocity",
        statement: "CI rewrite has slipped once before; current target is ambitious vs. observed velocity.",
        strength: 0.62,
        evidence: [
          { when: "2025-Q3", text: "Prior CI rewrite attempt slipped by 3 weeks before being descoped." },
          { when: "trailing 30d", text: "Current c-303 throughput trailing planned by ~18%.", ref: { kind: "commitment", id: "c-303" } },
        ],
      },
    ],
  },
  {
    id: "kim",
    label: "Kim",
    role: "Operations",
    recent_observation: "All low/standard-priority commitments on track.",
    calibration: 0.66,
    patterns: [
      {
        id: "kim-thin-signal",
        statement: "Mostly carries low-priority operational work — system has thin signal on her under stretch.",
        strength: 0.59,
        evidence: [
          { when: "trailing 180d", text: "0 high-priority commitments assigned in the last 6 months." },
        ],
      },
      {
        id: "kim-recurring",
        statement: "Reliable on recurring deliverables (NPS readout, vendor audit).",
        strength: 0.78,
        evidence: [
          { when: "trailing year", text: "NPS readout (c-210) closed on cadence 4 of 4 times.", ref: { kind: "commitment", id: "c-210" } },
          { when: "2026-Q1", text: "Vendor consolidation audit (c-402) on track at midpoint.", ref: { kind: "commitment", id: "c-402" } },
        ],
      },
      {
        id: "kim-no-stretch",
        statement: "Hasn't been assigned a high-priority commitment in 2 quarters.",
        strength: 0.71,
        evidence: [
          { when: "2025-Q4 → 2026-Q2", text: "All commitments in the window have been low or standard priority." },
        ],
      },
    ],
  },
  {
    id: "ravi",
    label: "Ravi",
    role: "Eng · Platform",
    recent_observation: "Auth service split is BLOCKED — flagged in Today's deprio recommendation.",
    calibration: 0.81,
    patterns: [
      {
        id: "ravi-infra-load",
        statement: "Carries the heaviest infra load on the team (Postgres, observability, auth).",
        strength: 0.90,
        evidence: [
          { when: "ongoing", text: "Owns Postgres upgrade (c-301), observability rollout (c-302), and auth split (c-305).", ref: { kind: "commitment", id: "c-301" } },
          { when: "trailing 90d", text: "60% of the technical-infrastructure territory load routes to Ravi." },
        ],
      },
      {
        id: "ravi-queue-stall",
        statement: "When one infra commitment blocks, the whole queue stalls — no parallel-track recovery.",
        strength: 0.77,
        evidence: [
          { when: "2026-04", text: "Auth split (c-305) blocked → Postgres upgrade (c-301) progress flat for 2 weeks.", ref: { kind: "commitment", id: "c-305" } },
          { when: "2025-Q3", text: "Prior pipeline block correlated with a 3-week stall on observability work." },
        ],
      },
      {
        id: "ravi-globex-stretch",
        statement: "Globex feature parity is a stretch given current platform load.",
        strength: 0.66,
        evidence: [
          { when: "ongoing", text: "Globex feature parity gap (c-216) shares Ravi's bandwidth with two infra commitments.", ref: { kind: "commitment", id: "c-216" } },
        ],
      },
      {
        id: "ravi-marcus-leverage",
        statement: "Highest leverage when paired explicitly with Marcus on shared platform work.",
        strength: 0.72,
        evidence: [
          { when: "2025-Q4", text: "Joint Ravi+Marcus work on c-187 closed cleanly with no slip.", ref: { kind: "commitment", id: "c-187" } },
          { when: "2025-Q3", text: "Solo platform commitments slipped 2 of 5 in the same window." },
        ],
      },
    ],
  },
  {
    id: "lina",
    label: "Lina",
    role: "Customer Success · Field",
    recent_observation: "Edge caching pilot and Umbrella roadshow both on track.",
    calibration: 0.58,
    patterns: [
      {
        id: "lina-new-signal",
        statement: "Newest team member tracked by the system — patterns are still forming.",
        strength: 0.50,
        evidence: [
          { when: "ongoing", text: "Joined the tracked surface 4 weeks ago; observation window is short." },
        ],
      },
      {
        id: "lina-logistics-early",
        statement: "Early signal: comfortable owning physical/logistical commitments (roadshows, all-hands).",
        strength: 0.61,
        evidence: [
          { when: "2026-04", text: "Umbrella enablement roadshow (c-224) on track.", ref: { kind: "commitment", id: "c-224" } },
          { when: "2026-04", text: "All-hands logistics (c-405) on track.", ref: { kind: "commitment", id: "c-405" } },
        ],
      },
      {
        id: "lina-untested",
        statement: "Not yet observed under deadline pressure.",
        strength: 0.45,
        evidence: [
          { when: "trailing 30d", text: "All current commitments are >2 weeks from their due date." },
        ],
      },
    ],
  },
];

export const SAMPLE_PEOPLE_INDEX: Record<string, PersonProfile> =
  Object.fromEntries(SAMPLE_PEOPLE.map((p) => [p.id, p]));

