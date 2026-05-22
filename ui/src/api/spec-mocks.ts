// Mock data for the spec-aligned abstractions. Used by *-spec-client.ts
// wrappers when the backend hasn't been wired (USE_MOCK=1 or 404 from
// the spec endpoints). Same fixtures power the demo build.

import type { EntityRef } from "./common-types";
import type {
  ConsequenceOp,
  ListSpecDeltasResponse,
  SpecDelta,
} from "./spec-delta-types";
import type {
  ListThreadsResponse,
  OperatingThread,
  RecentModelChange,
} from "./operating-thread-types";
import type { SpecForecast } from "./spec-forecast-types";
import type {
  ListLedgerEventsResponse,
  SpecLedgerEvent,
} from "./ledger-event-types";
import type {
  ContextGap,
  EvidenceTrace,
  SourceCoverage,
} from "./trust-types";

const beacon: EntityRef = { id: "cust-beacon", type: "customer", label: "Beacon", subtitle: "$840K ARR" };
const northvale: EntityRef = { id: "cust-northvale", type: "customer", label: "Northvale", subtitle: "$620K ARR" };
const conduit: EntityRef = { id: "cust-conduit", type: "customer", label: "Conduit", subtitle: "$580K ARR" };
const vpe: EntityRef = { id: "actor-vpe", type: "actor", label: "VP Engineering", subtitle: "Maya R." };
const ceo: EntityRef = { id: "actor-ceo", type: "actor", label: "Diana", subtitle: "CEO" };
const cs: EntityRef = { id: "actor-cs", type: "actor", label: "Head of CS", subtitle: "Priya S." };
const platform: EntityRef = { id: "team-platform", type: "team", label: "Platform Engineering" };
const gtm: EntityRef = { id: "team-gtm", type: "team", label: "GTM" };

const DEFAULT_SOURCE_COVERAGE: SourceCoverage[] = [
  { source: "support", status: "connected", label: "Zendesk" },
  { source: "crm", status: "connected", label: "Salesforce" },
  { source: "email", status: "connected", label: "Gmail" },
  { source: "slack", status: "limited", label: "Slack" },
  { source: "product", status: "not_connected", label: "Product usage" },
  { source: "calendar", status: "connected", label: "Calendar" },
];

const DEFAULT_GAPS: ContextGap[] = [
  {
    id: "gap-1",
    kind: "missing_human_context",
    text: "No recent Beacon call transcript.",
    suggestedAction: "ask_owner",
    target: cs,
  },
  {
    id: "gap-2",
    kind: "owner_unconfirmed",
    text: "Account owner has not confirmed severity.",
    suggestedAction: "ask_owner",
    target: cs,
  },
  {
    id: "gap-3",
    kind: "limited_telemetry",
    text: "Product usage telemetry unavailable.",
    suggestedAction: "connect_source",
  },
];

function evidenceTrace(seed: string): EvidenceTrace {
  return {
    id: `trace-${seed}`,
    summary: "12 signals → 3 claims → 1 pattern → customer-risk update",
    steps: [
      {
        id: `${seed}-obs-1`,
        kind: "observation",
        title: "Beacon support ticket #7421",
        description: "Sync failure during nightly batch.",
        source: "support",
        sourceLabel: "Zendesk",
        occurredAt: "2026-05-15T07:12:00Z",
        trustTier: "authoritative",
      },
      {
        id: `${seed}-obs-2`,
        kind: "observation",
        title: "Northvale support ticket #7438",
        description: "Sync delay >4h.",
        source: "support",
        sourceLabel: "Zendesk",
        occurredAt: "2026-05-15T09:01:00Z",
        trustTier: "authoritative",
      },
      {
        id: `${seed}-claim-1`,
        kind: "claim",
        title: "Salesforce sync is failing on anchor accounts",
        confidence: 0.84,
        trustTier: "attested",
      },
      {
        id: `${seed}-pattern-1`,
        kind: "pattern",
        title: "3 anchor accounts affected over 3 days",
        confidence: 0.78,
      },
      {
        id: `${seed}-belief-1`,
        kind: "belief",
        title: "Sync instability is now material to renewal risk",
        confidence: 0.78,
      },
      {
        id: `${seed}-rec-1`,
        kind: "recommendation",
        title: "Escalate customer risk to VP Engineering",
        confidence: 0.78,
      },
    ],
  };
}

const consequenceEscalate: ConsequenceOp[] = [
  { operation: "create", label: "Create escalation state" },
  { operation: "notify", label: "Notify VP Engineering", target: vpe },
  { operation: "update", label: "Link 3 affected renewal commitments" },
  { operation: "reevaluate", label: "Re-evaluate renewal forecast in 48h" },
  { operation: "archive", label: "Archive this recommendation" },
];

// ────────────────────────────────────────────────────────────────────
// Operating Threads
// ────────────────────────────────────────────────────────────────────

export const SPEC_THREADS_FIXTURE: OperatingThread[] = [
  {
    id: "thread-customer-reliability",
    lens: "company",
    title: "Customer Reliability",
    status: "under_pressure",
    currentReading:
      "Salesforce sync instability is now threatening 3 anchor renewals.",
    whyThisMatters:
      "Anchor renewals worth $2.04M ARR depend on a stable CRM. Without intervention, renewal risk likely escalates within 48 hours.",
    anchorSubjects: [beacon, northvale, conduit],
    causalRibbon: [
      { label: "Intent", value: "Improve CRM reliability", tone: "trust" },
      { label: "Promise", value: "Stabilize Salesforce sync", tone: "trust" },
      { label: "Friction", value: "Eng capacity + owner gap", tone: "review" },
      { label: "Exposure", value: "Beacon, Northvale, Conduit · $2.04M ARR", tone: "critical", refs: [beacon, northvale, conduit] },
      { label: "Next", value: "Escalate ownership", tone: "authority" },
    ],
    semanticMass: {
      representedNodes: 32,
      changedToday: 12,
      contested: 0,
      blockedCommitments: 2,
      affectedCustomers: 3,
      arrAtRisk: 2_040_000,
      typeCounts: { commitments: 4, risks: 3, decisions: 2, predictions: 2, observations: 12 },
    },
    trust: {
      confidence: 0.78,
      confidencePrevious: 0.61,
      evidenceQuality: "strong",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: DEFAULT_GAPS,
    },
    accountability: {
      owner: vpe,
      contributors: [platform, cs],
      waitingOn: [cs],
      blocking: [],
      loadSignal: "VPE load: high (3 active commitments)",
    },
    relatedDecisionDeltaIds: ["delta-escalate-csr-sync"],
    relatedForecastIds: ["forecast-beacon-renewal"],
    relatedCommitmentIds: ["commit-sync-stability"],
    hiddenStructure: [
      "3 unresolved support tickets in the last 72h",
      "1 unowned escalation thread in #cs-anchors",
      "Pricing decision unresolved; blocks data warehouse rollout",
    ],
    whatChanged: [
      { at: "2026-05-16T09:22:00Z", note: "Customer-risk state moved Watch → Critical" },
      { at: "2026-05-16T09:18:00Z", note: "Forecast confidence rose 61% → 78%" },
      { at: "2026-05-16T08:54:00Z", note: "Commitment flagged: CRM reliability constrained by capacity" },
    ],
    lastUpdatedAt: "2026-05-16T09:24:00Z",
  },
  {
    id: "thread-engineering-capacity",
    lens: "company",
    title: "Engineering Capacity",
    status: "needs_review",
    currentReading:
      "Platform team is forecasted to exceed 90% utilization by May 18; two anchor commitments at risk.",
    anchorSubjects: [platform],
    causalRibbon: [
      { label: "Intent", value: "Sustain delivery cadence", tone: "trust" },
      { label: "Promise", value: "Ship Q2 platform roadmap", tone: "trust" },
      { label: "Friction", value: "2 senior engineers on hiring", tone: "review" },
      { label: "Exposure", value: "Sync stability · warehouse pricing", tone: "review" },
      { label: "Next", value: "Pause new commitments", tone: "authority" },
    ],
    semanticMass: {
      representedNodes: 24,
      changedToday: 6,
      contested: 1,
      blockedCommitments: 2,
      affectedCustomers: 0,
      typeCounts: { commitments: 5, risks: 2, decisions: 1, predictions: 1, observations: 9 },
    },
    trust: {
      confidence: 0.74,
      confidencePrevious: 0.74,
      evidenceQuality: "medium",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: [
        {
          id: "gap-eng-1",
          kind: "limited_telemetry",
          text: "Sprint velocity from Linear is partial — only platform team is connected.",
          suggestedAction: "connect_source",
        },
      ],
    },
    accountability: {
      owner: vpe,
      contributors: [platform],
      waitingOn: [],
      blocking: [cs],
      loadSignal: "Team load: high",
    },
    relatedDecisionDeltaIds: ["delta-pause-commitments"],
    relatedForecastIds: ["forecast-eng-utilization"],
    relatedCommitmentIds: ["commit-q2-roadmap"],
    hiddenStructure: [
      "5 active commitments owned by 4 engineers",
      "Sprint slip: 2 of 6 stories carried over",
    ],
    whatChanged: [
      { at: "2026-05-16T07:48:00Z", note: "Utilization forecast: 88% → 92%" },
    ],
    lastUpdatedAt: "2026-05-16T08:01:00Z",
  },
  {
    id: "thread-strategic-decisions",
    lens: "company",
    title: "Strategic Decisions",
    status: "watch",
    currentReading:
      "Data warehouse pricing decision is 6 days unowned; blocks 2 commitments.",
    anchorSubjects: [],
    causalRibbon: [
      { label: "Intent", value: "Land data warehouse rollout", tone: "trust" },
      { label: "Promise", value: "Pricing aligned with anchor cohort", tone: "trust" },
      { label: "Friction", value: "Authority unassigned", tone: "review" },
      { label: "Exposure", value: "Beacon expansion timeline", tone: "review", refs: [beacon] },
      { label: "Next", value: "Assign decision owner", tone: "authority" },
    ],
    semanticMass: {
      representedNodes: 14,
      changedToday: 1,
      contested: 0,
      blockedCommitments: 2,
      affectedCustomers: 1,
      typeCounts: { commitments: 2, decisions: 1, predictions: 0, observations: 6 },
    },
    trust: {
      confidence: 0.66,
      evidenceQuality: "medium",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: [
        {
          id: "gap-strat-1",
          kind: "verbal_decision_unrecorded",
          text: "Pricing was discussed verbally Tuesday but not captured in connected sources.",
          suggestedAction: "add_context",
        },
      ],
    },
    accountability: {
      owner: undefined,
      contributors: [ceo],
      waitingOn: [ceo],
      blocking: [],
      loadSignal: "Authority required",
    },
    relatedDecisionDeltaIds: ["delta-assign-pricing-owner"],
    relatedForecastIds: [],
    relatedCommitmentIds: ["commit-warehouse-pricing"],
    hiddenStructure: [
      "Stalled in #strategy on 2026-05-10",
      "2 dependent commitments waiting on resolution",
    ],
    lastUpdatedAt: "2026-05-16T06:14:00Z",
  },
  {
    id: "thread-enterprise-gtm",
    lens: "company",
    title: "Enterprise GTM",
    status: "healthy",
    currentReading:
      "Pipeline coverage at 3.4x for Q3; 2 new design partners signed last week.",
    anchorSubjects: [],
    causalRibbon: [
      { label: "Intent", value: "Build enterprise pipeline", tone: "trust" },
      { label: "Promise", value: "3x coverage by Q3 close", tone: "trust" },
      { label: "Friction", value: "None material", tone: "trust" },
      { label: "Exposure", value: "Q3 bookings", tone: "trust" },
      { label: "Next", value: "Sustain cadence", tone: "trust" },
    ],
    semanticMass: {
      representedNodes: 18,
      changedToday: 2,
      contested: 0,
      blockedCommitments: 0,
      affectedCustomers: 0,
      typeCounts: { commitments: 3, decisions: 0, predictions: 1, observations: 7 },
    },
    trust: {
      confidence: 0.85,
      evidenceQuality: "strong",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: [],
    },
    accountability: {
      owner: { id: "actor-cro", type: "actor", label: "CRO", subtitle: "Alex Y." },
      contributors: [gtm],
      waitingOn: [],
      blocking: [],
    },
    relatedDecisionDeltaIds: [],
    relatedForecastIds: ["forecast-q3-pipeline"],
    relatedCommitmentIds: [],
    lastUpdatedAt: "2026-05-16T05:30:00Z",
  },
  {
    id: "thread-revenue-retention",
    lens: "company",
    title: "Revenue Retention",
    status: "watch",
    currentReading: "Net retention tracking at 112%; 1 renewal slipped to Q3.",
    anchorSubjects: [],
    causalRibbon: [
      { label: "Intent", value: "Hold NRR above 110%", tone: "trust" },
      { label: "Promise", value: "Retain anchor cohort", tone: "trust" },
      { label: "Friction", value: "1 slipped renewal", tone: "review" },
      { label: "Exposure", value: "Anchor cohort 2026 plan", tone: "review" },
      { label: "Next", value: "Monitor", tone: "trust" },
    ],
    semanticMass: {
      representedNodes: 15,
      changedToday: 1,
      contested: 0,
      blockedCommitments: 0,
      affectedCustomers: 1,
      typeCounts: { commitments: 2, predictions: 1, observations: 5 },
    },
    trust: {
      confidence: 0.82,
      evidenceQuality: "strong",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: [],
    },
    accountability: {
      owner: cs,
      contributors: [],
      waitingOn: [],
      blocking: [],
    },
    relatedDecisionDeltaIds: [],
    relatedForecastIds: [],
    relatedCommitmentIds: [],
    lastUpdatedAt: "2026-05-16T04:11:00Z",
  },
  {
    id: "thread-product-delivery",
    lens: "company",
    title: "Product Delivery",
    status: "healthy",
    currentReading: "All Q2 commitments on track; cycle time down 18%.",
    anchorSubjects: [],
    causalRibbon: [
      { label: "Intent", value: "Ship Q2 plan", tone: "trust" },
      { label: "Promise", value: "All anchor features", tone: "trust" },
      { label: "Friction", value: "None material", tone: "trust" },
      { label: "Exposure", value: "Customer satisfaction", tone: "trust" },
      { label: "Next", value: "Sustain cadence", tone: "trust" },
    ],
    semanticMass: {
      representedNodes: 21,
      changedToday: 0,
      contested: 0,
      blockedCommitments: 0,
      affectedCustomers: 0,
      typeCounts: { commitments: 4, predictions: 0, observations: 8 },
    },
    trust: {
      confidence: 0.88,
      evidenceQuality: "strong",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: [],
    },
    accountability: {
      owner: { id: "actor-pm", type: "actor", label: "Head of Product", subtitle: "Sam K." },
      contributors: [platform],
      waitingOn: [],
      blocking: [],
    },
    relatedDecisionDeltaIds: [],
    relatedForecastIds: [],
    relatedCommitmentIds: [],
    lastUpdatedAt: "2026-05-15T22:00:00Z",
  },
  {
    id: "thread-board-investor",
    lens: "company",
    title: "Board & Investor",
    status: "watch",
    currentReading:
      "Q2 board materials due in 3 days; 1 forecast confidence below board threshold.",
    anchorSubjects: [],
    causalRibbon: [
      { label: "Intent", value: "Maintain board trust", tone: "trust" },
      { label: "Promise", value: "Honest, calibrated readouts", tone: "trust" },
      { label: "Friction", value: "Renewal forecast in flux", tone: "review" },
      { label: "Exposure", value: "Series C narrative", tone: "review" },
      { label: "Next", value: "Resolve sync risk before readout", tone: "authority" },
    ],
    semanticMass: {
      representedNodes: 11,
      changedToday: 2,
      contested: 0,
      blockedCommitments: 0,
      affectedCustomers: 0,
      typeCounts: { commitments: 1, decisions: 1, predictions: 2, observations: 4 },
    },
    trust: {
      confidence: 0.7,
      evidenceQuality: "medium",
      sourceCoverage: DEFAULT_SOURCE_COVERAGE,
      contextGaps: [],
    },
    accountability: {
      owner: ceo,
      contributors: [],
      waitingOn: [],
      blocking: [],
    },
    relatedDecisionDeltaIds: [],
    relatedForecastIds: ["forecast-beacon-renewal"],
    relatedCommitmentIds: [],
    lastUpdatedAt: "2026-05-16T03:00:00Z",
  },
];

export const SPEC_THREADS_RESPONSE: ListThreadsResponse = {
  groups: [
    {
      id: "needs-attention",
      label: "Needs attention",
      threads: SPEC_THREADS_FIXTURE.filter((t) =>
        ["under_pressure", "needs_review", "critical", "contested", "stale"].includes(t.status)
      ),
    },
    {
      id: "stable",
      label: "Stable / watching",
      threads: SPEC_THREADS_FIXTURE.filter((t) =>
        ["healthy", "watch", "monitoring", "resolved"].includes(t.status)
      ),
    },
  ],
  total: SPEC_THREADS_FIXTURE.length,
  compressionSentence:
    "Fyralis has condensed 148 active Nodes into 7 operating threads.",
  statusCounters: {
    changedToday: 12,
    contested: 1,
    blockedCommitments: 4,
    arrAtRisk: 2_040_000,
  },
  lastUpdatedAt: "2026-05-16T09:24:00Z",
};

export const RECENT_MODEL_CHANGES_FIXTURE: RecentModelChange[] = [
  {
    id: "rmc-1",
    occurredAt: "2026-05-16T09:22:00Z",
    summary: "Customer-risk state updated: Watch → Critical",
    threadId: "thread-customer-reliability",
    kind: "state_change",
  },
  {
    id: "rmc-2",
    occurredAt: "2026-05-16T09:18:00Z",
    summary: "Forecast created: Beacon renewal risk likely to increase",
    threadId: "thread-customer-reliability",
    kind: "forecast_created",
  },
  {
    id: "rmc-3",
    occurredAt: "2026-05-16T08:54:00Z",
    summary: "Commitment flagged: CRM reliability constrained by capacity",
    threadId: "thread-customer-reliability",
    kind: "commitment_flagged",
  },
  {
    id: "rmc-4",
    occurredAt: "2026-05-16T07:48:00Z",
    summary: "Utilization forecast: 88% → 92%",
    threadId: "thread-engineering-capacity",
    kind: "state_change",
  },
];

// ────────────────────────────────────────────────────────────────────
// Decision Deltas (spec)
// ────────────────────────────────────────────────────────────────────

export const SPEC_DELTAS_FIXTURE: SpecDelta[] = [
  {
    id: "delta-escalate-csr-sync",
    userFacingType: "Proposed Change",
    status: "proposed",
    queueSection: "requires_authority",
    proposal:
      "Escalate customer risk for Salesforce sync instability.",
    currentState: "Watch",
    proposedState: "Critical · Owner: VP Engineering · Re-evaluate 48h",
    sourceThreadId: "thread-customer-reliability",
    sourceThreadTitle: "Customer Reliability",
    category: "Customer Risk",
    whySurfaced: [
      "12 related signals across Support, CRM, and Email",
      "3 affected anchor customers: Beacon, Northvale, Conduit",
      "Issue has persisted for 3 days",
      "Forecast confidence rose from 61% to 78%",
    ],
    impactChips: ["$2.04M ARR", "3 customers", "12 signals"],
    arrAtRisk: 2_040_000,
    affectedCustomers: [beacon, northvale, conduit],
    confidence: 0.78,
    confidenceBasis: "limited by missing product usage telemetry",
    evidenceTrace: evidenceTrace("delta-esc"),
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: DEFAULT_GAPS,
    falsificationCondition: "No new sync failures for 7 business days.",
    consequencePreview: consequenceEscalate,
    severity: "critical",
    staleLabel: "Sustained 3 days",
    createdAt: "2026-05-16T09:22:00Z",
    updatedAt: "2026-05-16T09:22:00Z",
  },
  {
    id: "delta-assign-pricing-owner",
    userFacingType: "Proposed Change",
    status: "proposed",
    queueSection: "requires_authority",
    proposal:
      "Assign an owner to the data warehouse pricing decision.",
    currentState: "Unowned · stalled 6 days",
    proposedState: "Owner: CFO · Decision due in 5 business days",
    sourceThreadId: "thread-strategic-decisions",
    sourceThreadTitle: "Strategic Decisions",
    category: "Decision",
    whySurfaced: [
      "Decision is 6 days unowned",
      "Blocks Beacon expansion timeline",
      "Verbal pricing direction not captured in sources",
    ],
    impactChips: ["2 commitments blocked", "1 expansion blocked"],
    confidence: 0.66,
    confidenceBasis: "based on calendar + Slack mentions only",
    evidenceTrace: evidenceTrace("delta-pricing"),
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: [
      {
        id: "gap-pricing-1",
        kind: "verbal_decision_unrecorded",
        text: "Pricing was discussed verbally Tuesday but not captured in connected sources.",
        suggestedAction: "add_context",
      },
    ],
    falsificationCondition: "Decision is resolved or owner publicly assigned.",
    consequencePreview: [
      { operation: "update", label: "Assign owner: CFO" },
      { operation: "notify", label: "Notify CFO + dependent owners" },
      { operation: "reevaluate", label: "Re-evaluate dependent commitments in 5 days" },
      { operation: "archive", label: "Archive this recommendation" },
    ],
    severity: "high",
    staleLabel: "Stalled 6 days",
    createdAt: "2026-05-16T06:14:00Z",
    updatedAt: "2026-05-16T06:14:00Z",
  },
  {
    id: "delta-pause-commitments",
    userFacingType: "Proposed Change",
    status: "proposed",
    queueSection: "delegatable",
    proposal:
      "Pause net-new platform commitments until utilization drops below 85%.",
    currentState: "Accepting new commitments",
    proposedState: "Paused · re-open when utilization ≤ 85%",
    sourceThreadId: "thread-engineering-capacity",
    sourceThreadTitle: "Engineering Capacity",
    category: "Capacity",
    whySurfaced: [
      "Forecast: utilization will exceed 90% by May 18",
      "2 senior engineers focused on hiring this sprint",
      "1 sprint slip in last 2 weeks",
    ],
    impactChips: ["2 commitments at risk", "1 sprint slipped"],
    confidence: 0.74,
    evidenceTrace: evidenceTrace("delta-pause"),
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: [
      {
        id: "gap-pause-1",
        kind: "limited_telemetry",
        text: "Sprint velocity covers only Platform team in Linear.",
        suggestedAction: "connect_source",
      },
    ],
    falsificationCondition: "Utilization drops below 85% for 2 sprints in a row.",
    consequencePreview: [
      { operation: "update", label: "Mark Platform Engineering: paused for net-new" },
      { operation: "notify", label: "Notify CRO + Product", target: gtm },
      { operation: "reevaluate", label: "Re-evaluate weekly" },
    ],
    severity: "medium",
    staleLabel: "Updated today",
    createdAt: "2026-05-16T07:48:00Z",
    updatedAt: "2026-05-16T07:48:00Z",
  },
  {
    id: "delta-confirm-beacon-severity",
    userFacingType: "Proposed Change",
    status: "proposed",
    queueSection: "needs_context",
    proposal: "Confirm severity for Beacon escalation.",
    currentState: "Inferred from support tickets",
    proposedState: "Confirmed by account owner",
    sourceThreadId: "thread-customer-reliability",
    sourceThreadTitle: "Customer Reliability",
    category: "Context",
    whySurfaced: [
      "Account owner has not confirmed severity",
      "Conflicting signal: account exec last note was positive",
    ],
    impactChips: ["1 forecast pending"],
    confidence: 0.62,
    confidenceBasis: "mixed evidence — support vs. account owner",
    evidenceTrace: { ...evidenceTrace("delta-confirm"), contested: true, contestationNote: "Account owner says partial resolution; support says ongoing." },
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: [
      {
        id: "gap-confirm-1",
        kind: "owner_unconfirmed",
        text: "Account owner has not confirmed severity.",
        suggestedAction: "ask_owner",
        target: cs,
      },
    ],
    consequencePreview: [
      { operation: "update", label: "Attach owner-confirmed severity to the thread" },
      { operation: "reevaluate", label: "Re-evaluate renewal forecast" },
    ],
    severity: "medium",
    createdAt: "2026-05-16T08:01:00Z",
    updatedAt: "2026-05-16T08:01:00Z",
  },
];

export const SPEC_DELTAS_RESPONSE: ListSpecDeltasResponse = {
  deltas: SPEC_DELTAS_FIXTURE,
  sinceLastReview: {
    proposedChanges: 3,
    delegatable: 1,
    contested: 1,
    modelUpdates: 12,
    signalsAbsorbed: 98,
    arrExposed: 2_040_000,
  },
  lastUpdatedAt: "2026-05-16T09:24:00Z",
};

// ────────────────────────────────────────────────────────────────────
// Forecasts (spec)
// ────────────────────────────────────────────────────────────────────

export const SPEC_FORECASTS_FIXTURE: SpecForecast[] = [
  {
    id: "forecast-beacon-renewal",
    statement:
      "Beacon renewal risk is likely to increase if Salesforce sync failures persist.",
    domain: "customer",
    status: "intervention_available",
    confidence: 0.78,
    confidencePrevious: 0.61,
    resolutionDate: "2026-05-17",
    leadingIndicators: [
      { label: "Support tickets rising", movement: "rising", detail: "3 tickets in 72h" },
      { label: "Renewal-thread sentiment", movement: "falling", detail: "Negative shift in last 48h" },
      { label: "Owner gap", detail: "Account owner has not confirmed severity" },
    ],
    evidenceTrace: evidenceTrace("fc-beacon"),
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: DEFAULT_GAPS,
    falsificationCondition: "No new sync failures for 7 business days.",
    relatedThreadId: "thread-customer-reliability",
    relatedThreadTitle: "Customer Reliability",
    relatedDeltaId: "delta-escalate-csr-sync",
    interventionLabel: "Escalate customer risk",
    severityHint: "review",
  },
  {
    id: "forecast-eng-utilization",
    statement: "Platform engineering utilization will exceed 90% by May 18.",
    domain: "capacity",
    status: "active",
    confidence: 0.74,
    confidencePrevious: 0.66,
    resolutionDate: "2026-05-18",
    leadingIndicators: [
      { label: "Active commitments", movement: "rising" },
      { label: "Senior eng on hiring", detail: "2 of 6" },
      { label: "Sprint slip", detail: "2 stories carried over" },
    ],
    evidenceTrace: evidenceTrace("fc-eng"),
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: [
      {
        id: "gap-eng-fc",
        kind: "limited_telemetry",
        text: "Velocity data only from Platform team.",
        suggestedAction: "connect_source",
      },
    ],
    falsificationCondition: "Utilization drops below 85% for 2 sprints in a row.",
    relatedThreadId: "thread-engineering-capacity",
    relatedThreadTitle: "Engineering Capacity",
    relatedDeltaId: "delta-pause-commitments",
    interventionLabel: "Pause new commitments",
    severityHint: "forecast",
  },
  {
    id: "forecast-q3-pipeline",
    statement: "Q3 enterprise pipeline coverage will hold above 3.0x.",
    domain: "revenue",
    status: "active",
    confidence: 0.82,
    confidencePrevious: 0.78,
    resolutionDate: "2026-07-31",
    leadingIndicators: [
      { label: "Design partners signed", movement: "rising", detail: "2 new" },
      { label: "Top-of-funnel quality", movement: "steady" },
    ],
    evidenceTrace: evidenceTrace("fc-pipeline"),
    sourceCoverage: DEFAULT_SOURCE_COVERAGE,
    contextGaps: [],
    falsificationCondition: "Coverage drops below 2.5x for 2 weeks.",
    relatedThreadId: "thread-enterprise-gtm",
    relatedThreadTitle: "Enterprise GTM",
    severityHint: "forecast",
  },
];

// ────────────────────────────────────────────────────────────────────
// Ledger (spec)
// ────────────────────────────────────────────────────────────────────

export const SPEC_LEDGER_EVENTS_FIXTURE: SpecLedgerEvent[] = [
  {
    id: "ledger-1",
    occurredAt: "2026-05-16T09:22:00Z",
    kind: "thread_status_changed",
    category: "model_update",
    summary: "Customer Reliability moved Watch → Under Pressure.",
    body: "Three anchor customers showed sync failures; the model upgraded the thread status and surfaced a Decision Delta in Today.",
    actor: { id: "actor-fyralis", type: "actor", label: "Fyralis" },
    before: "Watch",
    after: "Under Pressure",
    relatedRefs: [beacon, northvale, conduit],
    affectedThreadId: "thread-customer-reliability",
    affectedDeltaId: "delta-escalate-csr-sync",
    actionsTaken: ["Surfaced Decision Delta", "Notified subscribers"],
    severity: "review",
  },
  {
    id: "ledger-2",
    occurredAt: "2026-05-16T09:18:00Z",
    kind: "forecast_created",
    category: "forecast",
    summary: "Forecast created: Beacon renewal risk likely to increase.",
    actor: { id: "actor-fyralis", type: "actor", label: "Fyralis" },
    relatedRefs: [beacon],
    affectedForecastId: "forecast-beacon-renewal",
    affectedThreadId: "thread-customer-reliability",
    severity: "forecast",
  },
  {
    id: "ledger-3",
    occurredAt: "2026-05-16T08:54:00Z",
    kind: "commitment_blocked",
    category: "commitment_state",
    summary: "Commitment flagged: CRM reliability constrained by capacity.",
    actor: { id: "actor-fyralis", type: "actor", label: "Fyralis" },
    relatedRefs: [vpe],
    affectedCommitmentId: "commit-sync-stability",
    affectedThreadId: "thread-customer-reliability",
    severity: "authority",
  },
  {
    id: "ledger-4",
    occurredAt: "2026-05-15T17:30:00Z",
    kind: "decision_delta_accepted",
    category: "decision_action",
    summary: "Diana accepted: Add 2 contractors for hiring backfill.",
    actor: ceo,
    relatedRefs: [vpe],
    actionsTaken: ["Created contractor commitments", "Notified VP Engineering"],
    severity: "authority",
  },
  {
    id: "ledger-5",
    occurredAt: "2026-05-15T14:02:00Z",
    kind: "decision_delta_contested",
    category: "contestation",
    summary: "Priya contested: Beacon severity is partial, not full.",
    actor: cs,
    before: "Critical",
    after: "Watch",
    relatedRefs: [beacon],
    affectedThreadId: "thread-customer-reliability",
    severity: "review",
  },
  {
    id: "ledger-6",
    occurredAt: "2026-05-15T09:10:00Z",
    kind: "forecast_resolved",
    category: "forecast",
    summary: "Forecast resolved true: Engineering utilization exceeded 90% by May 15.",
    actor: { id: "actor-fyralis", type: "actor", label: "Fyralis" },
    outcome: "true",
    calibrationImpact: 0.03,
    relatedRefs: [vpe, platform],
    affectedThreadId: "thread-engineering-capacity",
    severity: "trust",
  },
  {
    id: "ledger-7",
    occurredAt: "2026-05-15T08:40:00Z",
    kind: "observation_ingested",
    category: "observation",
    summary: "12 Salesforce sync failure observations ingested.",
    actor: { id: "src-zendesk", type: "source", label: "Zendesk" },
    relatedRefs: [beacon, northvale, conduit],
    severity: "info",
  },
];

export const SPEC_LEDGER_RESPONSE: ListLedgerEventsResponse = {
  events: SPEC_LEDGER_EVENTS_FIXTURE,
  total: SPEC_LEDGER_EVENTS_FIXTURE.length,
  rangeLabel: "May 12 – May 16",
};
