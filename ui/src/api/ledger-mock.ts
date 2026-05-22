// Ledger fixture — 30 events across 14 days plus a summary counter
// fixture matching the spec §6.1 screenshot. Used by the mock-server
// integration glue (registered in mock-server.ts) and by the unit and
// e2e tests so behaviour is consistent across environments.

import type {
  LedgerEvent,
  LedgerEventType,
  LedgerSummary,
} from "./history-types";

// Anchor "today" to May 15, 2025 so the screenshot copy (and the date
// range "Apr 15 – May 15, 2025") stays stable in fixture-driven tests.
// Tests that depend on relative dates should freeze Date.now too.
const TODAY_ISO = "2025-05-15";

function ts(day: number, hour: number, minute: number): string {
  // day is the day-of-May (1..31); negative values roll back into April.
  const d = new Date(`${TODAY_ISO}T00:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() - (15 - day));
  d.setUTCHours(hour, minute, 0, 0);
  return d.toISOString();
}

export const SALESFORCE_ESCALATION_EVENT: LedgerEvent = {
  id: "evt-salesforce-escalation",
  timestamp: ts(15, 9, 22),
  type: "action_taken",
  title: "Escalated customer risk: Salesforce sync instability",
  summary:
    "Diana escalated the customer risk to VP Engineering.",
  body:
    "Diana escalated the customer risk to VP Engineering.",
  tags: ["Customer Risk", "Decision"],
  actor: { kind: "person", name: "Diana", role: "CEO" },
  target: "VP Engineering",
  scope: ["Beacon", "Northvale", "Conduit"],
  detail_type: "Escalation",
  related_nodes: [
    {
      id: "node-customer-risk-salesforce",
      label: "Customer risk: Salesforce sync instability",
    },
  ],
  changes: [
    { verb: "created", text: "Created escalation decision node" },
    { verb: "notified", text: "Notified 3 owners" },
    {
      verb: "scheduled",
      text: "Scheduled re-evaluation in 48h",
    },
    {
      verb: "archived",
      text: 'Archived recommendation node ("Escalate customer risk")',
    },
  ],
  evidence: [
    { source: "support", label: "Support tickets", count: 12 },
    { source: "email", label: "Emails", count: 8 },
    { source: "crm", label: "CRM notes", count: 6 },
    { source: "documents", label: "Documents", count: 4 },
    { source: "support", label: "More", count: 4 },
  ],
  mini_timeline: [
    {
      id: "mt-1",
      timestamp: ts(15, 9, 18),
      text: "Customer risk state updated Watch → Critical",
      event_type: "model_update",
    },
    {
      id: "mt-2",
      timestamp: ts(15, 9, 19),
      text: "Escalation recommendation created",
      event_type: "model_update",
    },
    {
      id: "mt-3",
      timestamp: ts(15, 9, 22),
      text: "Escalation action taken by Diana",
      event_type: "action_taken",
    },
  ],
};

export const LEDGER_EVENTS_FIXTURE: LedgerEvent[] = [
  // ── Today (May 15) ───────────────────────────────────────────────
  SALESFORCE_ESCALATION_EVENT,
  {
    id: "evt-customer-risk-state",
    timestamp: ts(15, 9, 18),
    type: "model_update",
    title: "Customer risk state updated",
    summary:
      "Salesforce sync instability changed from Watch to Critical.",
    tags: ["Customer Risk", "Model"],
    actor: { kind: "system", name: "Fyralis System" },
    body:
      "The customer risk node moved from Watch to Critical following sustained anchor renewal signals.",
    related_nodes: [
      {
        id: "node-customer-risk-salesforce",
        label: "Customer risk: Salesforce sync instability",
      },
    ],
  },
  {
    id: "evt-engineering-utilization-resolved",
    timestamp: ts(15, 8, 44),
    type: "prediction_resolved",
    title: "Engineering utilization prediction resolved",
    summary:
      "Utilization exceeded 90% — outcome matched prediction within 4 days.",
    tags: ["Capacity", "Prediction"],
    actor: { kind: "system", name: "Fyralis System" },
    body:
      "Resolved correctly — observed utilization 92.1% vs forecast threshold 90%.",
  },
  {
    id: "evt-pricing-cohort-observed",
    timestamp: ts(15, 7, 15),
    type: "observation_ingested",
    title: "Pricing cohort signals ingested",
    summary:
      "32 new pricing-related observations imported from Salesforce.",
    tags: ["Pricing model", "Observation"],
    actor: { kind: "integration", name: "Salesforce Integration" },
  },
  {
    id: "evt-conduit-contestation",
    timestamp: ts(15, 7, 2),
    type: "contestation",
    title: "Conduit account contestation",
    summary:
      "Alex Kim contested the Conduit churn risk assertion.",
    tags: ["Customer Risk", "Contestation"],
    actor: { kind: "person", name: "Alex Kim", role: "Head of Finance" },
  },

  // ── May 14 ────────────────────────────────────────────────────────
  {
    id: "evt-q3-roadmap-decision",
    timestamp: ts(14, 17, 5),
    type: "action_taken",
    title: "Approved Q3 roadmap commitment",
    summary:
      "Roadmap commitment ratified — engineering sequencing locked.",
    tags: ["Decision", "Roadmap"],
    actor: { kind: "person", name: "Diana", role: "CEO" },
  },
  {
    id: "evt-prediction-pricing-conversion",
    timestamp: ts(14, 14, 30),
    type: "prediction_made",
    title: "Pricing conversion forecast filed",
    summary:
      "Forecast: pricing model A converts +6pp over model B by May 28.",
    tags: ["Pricing model", "Forecast"],
    actor: { kind: "system", name: "Fyralis System" },
  },
  {
    id: "evt-northvale-state",
    timestamp: ts(14, 13, 17),
    type: "model_update",
    title: "Northvale account state recalibrated",
    summary:
      "Expansion likelihood revised from 62% to 71% after pipeline review.",
    tags: ["Customer Risk"],
    actor: { kind: "system", name: "Fyralis System" },
  },
  {
    id: "evt-linear-issues-observed",
    timestamp: ts(14, 11, 4),
    type: "observation_ingested",
    title: "Linear issue snapshot ingested",
    summary: "147 issues across 6 teams observed since last snapshot.",
    tags: ["Observation"],
    actor: { kind: "integration", name: "Linear Integration" },
  },
  {
    id: "evt-finance-contestation-runway",
    timestamp: ts(14, 9, 50),
    type: "contestation",
    title: "Runway projection contested",
    summary:
      "Finance contested the 14-month runway projection — added new assumptions.",
    tags: ["Decision", "Contestation"],
    actor: { kind: "person", name: "Alex Kim", role: "Head of Finance" },
  },
  {
    id: "evt-pricing-experiment-recommendation",
    timestamp: ts(14, 8, 12),
    type: "model_update",
    title: "Pricing experiment recommendation created",
    summary:
      "New recommendation: split pricing experiment between cohorts A and B.",
    tags: ["Pricing model"],
    actor: { kind: "system", name: "Fyralis System" },
  },

  // ── May 13 ────────────────────────────────────────────────────────
  {
    id: "evt-prediction-renewal-resolved",
    timestamp: ts(13, 16, 40),
    type: "prediction_resolved",
    title: "Beacon renewal prediction resolved",
    summary:
      "Beacon renewed on May 11 — matched 78% confidence forecast.",
    tags: ["Customer Risk", "Prediction"],
    actor: { kind: "system", name: "Fyralis System" },
  },
  {
    id: "evt-decision-headcount",
    timestamp: ts(13, 15, 22),
    type: "action_taken",
    title: "Approved headcount plan revision",
    summary: "Engineering hiring lifted by 2 FTE for next quarter.",
    tags: ["Decision"],
    actor: { kind: "person", name: "Diana", role: "CEO" },
  },
  {
    id: "evt-stripe-observations",
    timestamp: ts(13, 12, 5),
    type: "observation_ingested",
    title: "Stripe revenue feed ingested",
    summary: "12 invoices and 9 refund signals captured.",
    tags: ["Observation"],
    actor: { kind: "integration", name: "Stripe Integration" },
  },
  {
    id: "evt-pattern-engineering-velocity",
    timestamp: ts(13, 10, 35),
    type: "model_update",
    title: "Engineering velocity pattern emerged",
    summary: "Sustained 12% velocity dip detected across two sprints.",
    tags: ["Capacity"],
    actor: { kind: "system", name: "Fyralis System" },
  },

  // ── May 12 ────────────────────────────────────────────────────────
  {
    id: "evt-contestation-velocity",
    timestamp: ts(12, 18, 12),
    type: "contestation",
    title: "Engineering velocity pattern contested",
    summary: "VP Engineering contested the velocity pattern signal.",
    tags: ["Capacity", "Contestation"],
    actor: { kind: "person", name: "Priya Shah", role: "VP Engineering" },
  },
  {
    id: "evt-pred-customer-churn",
    timestamp: ts(12, 15, 30),
    type: "prediction_made",
    title: "Customer churn risk forecast filed",
    summary:
      "Forecast: Conduit churn probability over 60% in next 30 days.",
    tags: ["Customer Risk"],
    actor: { kind: "system", name: "Fyralis System" },
  },
  {
    id: "evt-decision-marketing-spend",
    timestamp: ts(12, 11, 20),
    type: "action_taken",
    title: "Approved marketing spend reallocation",
    summary: "$120K shifted from paid search to lifecycle programs.",
    tags: ["Decision"],
    actor: { kind: "person", name: "Diana", role: "CEO" },
  },

  // ── May 11 ────────────────────────────────────────────────────────
  {
    id: "evt-zendesk-tickets-ingested",
    timestamp: ts(11, 14, 5),
    type: "observation_ingested",
    title: "Zendesk ticket snapshot ingested",
    summary: "238 tickets ingested; 31 flagged as high-severity.",
    tags: ["Observation"],
    actor: { kind: "integration", name: "Zendesk Integration" },
  },
  {
    id: "evt-model-pricing-update",
    timestamp: ts(11, 9, 45),
    type: "model_update",
    title: "Pricing model confidence recalibrated",
    summary: "Pricing model confidence moved 64% → 71%.",
    tags: ["Pricing model"],
    actor: { kind: "system", name: "Fyralis System" },
  },

  // ── May 10 ────────────────────────────────────────────────────────
  {
    id: "evt-decision-hiring-freeze",
    timestamp: ts(10, 16, 0),
    type: "action_taken",
    title: "Hiring freeze decision archived",
    summary: "Previous hiring freeze decision archived after policy change.",
    tags: ["Decision"],
    actor: { kind: "person", name: "Diana", role: "CEO" },
  },
  {
    id: "evt-pred-capacity-prediction",
    timestamp: ts(10, 11, 22),
    type: "prediction_made",
    title: "Capacity prediction filed",
    summary:
      "Forecast: Engineering capacity will exceed 90% utilization by May 21.",
    tags: ["Capacity", "Forecast"],
    actor: { kind: "system", name: "Fyralis System" },
  },

  // ── May 9 ─────────────────────────────────────────────────────────
  {
    id: "evt-github-prs-ingested",
    timestamp: ts(9, 13, 50),
    type: "observation_ingested",
    title: "GitHub pull request snapshot ingested",
    summary: "85 PRs across 12 repos captured since last snapshot.",
    tags: ["Observation"],
    actor: { kind: "integration", name: "GitHub Integration" },
  },
  {
    id: "evt-decision-customer-tier",
    timestamp: ts(9, 10, 15),
    type: "action_taken",
    title: "Customer tier policy updated",
    summary: "Tier promotion thresholds revised.",
    tags: ["Decision"],
    actor: { kind: "person", name: "Diana", role: "CEO" },
  },

  // ── May 8 ─────────────────────────────────────────────────────────
  {
    id: "evt-model-arc-pattern-resolved",
    timestamp: ts(8, 17, 10),
    type: "model_update",
    title: "Q1 customer-risk arc resolved",
    summary: "Customer-risk arc closed after 38 days.",
    tags: ["Customer Risk"],
    actor: { kind: "system", name: "Fyralis System" },
  },
  {
    id: "evt-prediction-revenue-resolved",
    timestamp: ts(8, 9, 20),
    type: "prediction_resolved",
    title: "Revenue forecast resolved",
    summary: "April revenue forecast resolved correctly at 96% accuracy.",
    tags: ["Forecast"],
    actor: { kind: "system", name: "Fyralis System" },
  },

  // ── May 7 ─────────────────────────────────────────────────────────
  {
    id: "evt-decision-pricing-experiment",
    timestamp: ts(7, 14, 30),
    type: "action_taken",
    title: "Pricing experiment greenlit",
    summary:
      "Pricing experiment approved for two-week run across cohorts A/B.",
    tags: ["Decision", "Pricing model"],
    actor: { kind: "person", name: "Diana", role: "CEO" },
  },
  {
    id: "evt-contestation-revenue-forecast",
    timestamp: ts(7, 10, 5),
    type: "contestation",
    title: "Revenue forecast contested",
    summary: "Finance contested the revenue calibration for Q2.",
    tags: ["Forecast", "Contestation"],
    actor: { kind: "person", name: "Alex Kim", role: "Head of Finance" },
  },

  // ── May 6 ─────────────────────────────────────────────────────────
  {
    id: "evt-segment-ingested",
    timestamp: ts(6, 11, 15),
    type: "observation_ingested",
    title: "Segment events ingested",
    summary: "Onboarding funnel events captured for the past 7 days.",
    tags: ["Observation"],
    actor: { kind: "integration", name: "Segment Integration" },
  },
  {
    id: "evt-pred-onboarding-conversion",
    timestamp: ts(6, 9, 0),
    type: "prediction_made",
    title: "Onboarding conversion forecast filed",
    summary: "Forecast: onboarding step-3 conversion at 41% (+/- 3pp).",
    tags: ["Forecast"],
    actor: { kind: "system", name: "Fyralis System" },
  },
];

// Summary counters matching the screenshot copy.
export const LEDGER_SUMMARY_FIXTURE: LedgerSummary = {
  events: {
    value: 1248,
    delta_pct: 0.18,
    delta_label: "↗ 18% vs Apr 15 – Apr 30",
  },
  model_updates: {
    value: 146,
    delta_pct: 0.12,
    delta_label: "↗ 12%",
  },
  predictions_made: {
    value: 28,
    split: "7 resolved · 21 active",
  },
  predictions_accuracy: {
    value: 0.71,
    delta_pp: 0.06,
    delta_label: "↗ 6pp last 30 days",
  },
  actions_taken: {
    value: 64,
    delta_pct: 0.21,
    delta_label: "↗ 21%",
  },
  contestations: {
    value: 9,
    split: "3 unresolved",
  },
  range_days: 30,
};

export function filterLedgerEvents(
  events: LedgerEvent[],
  types?: LedgerEventType[]
): LedgerEvent[] {
  if (!types || types.length === 0) return events;
  const allow = new Set(types);
  return events.filter((e) => allow.has(e.type));
}
