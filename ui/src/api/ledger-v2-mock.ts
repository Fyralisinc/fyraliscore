// Ledger v2 fixture. Mirrors the screenshot in
// fyralis_ledger_page_implementation_spec_v1.md and is used until a
// backend chain endpoint exists.

import type {
  LedgerChainCard,
  LedgerChainDetail,
  LedgerPagePayload,
} from "./ledger-v2-types";

const STAGES_RESOLVED: LedgerChainCard["stages"] = [
  { id: "s1", label: "Detected", status: "complete", timestamp: "2026-05-16T09:18:00Z" },
  { id: "s2", label: "Forecasted", status: "complete", timestamp: "2026-05-16T09:20:00Z" },
  { id: "s3", label: "Proposed", status: "complete", timestamp: "2026-05-16T09:22:00Z" },
  { id: "s4", label: "Accepted", status: "complete", timestamp: "2026-05-16T09:22:00Z" },
  { id: "s5", label: "Monitoring", status: "complete", timestamp: "2026-05-17T16:42:00Z" },
  { id: "s6", label: "Resolved", status: "complete", timestamp: "2026-05-18T08:15:00Z" },
];

const CHAIN_CUSTOMER: LedgerChainDetail = {
  id: "chain_customer_reliability",
  title: "Customer Reliability Escalation",
  status: "resolved",
  summary:
    "Salesforce sync instability created renewal risk. Escalated, assigned, and monitored until resolved.",
  eventCount: 6,
  impactLabel: "$2.04M exposure reduced",
  startedAt: "2026-05-16T09:18:00Z",
  resolvedAt: "2026-05-18T08:15:00Z",
  stages: STAGES_RESOLVED,
  events: [
    {
      id: "ev1",
      timestamp: "2026-05-16T09:18:00Z",
      type: "risk_escalated",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Detected",
      description: "Fyralis detected reliability issues across 3 anchor accounts.",
      chainId: "chain_customer_reliability",
    },
    {
      id: "ev2",
      timestamp: "2026-05-16T09:20:00Z",
      type: "forecast_created",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Forecast created",
      description: "Beacon renewal risk likely to increase.",
      confidencePct: 78,
      chainId: "chain_customer_reliability",
    },
    {
      id: "ev3",
      timestamp: "2026-05-16T09:22:00Z",
      type: "proposed_change_accepted",
      actor: { type: "user", name: "Diana" },
      title: "Proposed Change accepted",
      description: "Escalate customer risk for Salesforce sync instability.",
      chainId: "chain_customer_reliability",
    },
    {
      id: "ev4",
      timestamp: "2026-05-16T09:24:00Z",
      type: "owner_assigned",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Owner notified",
      description: "VP Engineering (Parker Li) assigned as escalation owner.",
      chainId: "chain_customer_reliability",
    },
    {
      id: "ev5",
      timestamp: "2026-05-17T16:42:00Z",
      type: "risk_downgraded",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Risk downgraded",
      description: "No new sync failures for 5 business days.",
      chainId: "chain_customer_reliability",
    },
    {
      id: "ev6",
      timestamp: "2026-05-18T08:15:00Z",
      type: "forecast_resolved_true",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Forecast resolved true",
      description: "Risk decreased within forecast window.",
      chainId: "chain_customer_reliability",
    },
  ],
  beforeState: {
    capturedAt: "2026-05-16T09:18:00Z",
    fields: [
      { label: "Risk level", value: "Critical", tone: "critical" },
      { label: "Owner", value: "Unassigned", tone: "muted" },
      { label: "Renewal exposure", value: "$2.04M ARR" },
      { label: "Sync failure rate", value: "High (recurring)", tone: "warn" },
      { label: "Re-evaluation", value: "Pending", tone: "muted" },
    ],
  },
  afterState: {
    capturedAt: "2026-05-18T08:15:00Z",
    fields: [
      { label: "Risk level", value: "Watch", tone: "positive" },
      { label: "Owner", value: "VP Engineering" },
      { label: "Renewal exposure", value: "Reduced", tone: "positive" },
      { label: "Sync failure rate", value: "None (5 days)", tone: "positive" },
      { label: "Re-evaluation", value: "Complete", tone: "positive" },
    ],
  },
  evidenceAtTime: {
    capturedAt: "2026-05-16T09:18:00Z",
    signalCount: 12,
    sources: [
      { label: "Support tickets", strength: "strong", count: 12 },
      { label: "CRM logs", strength: "strong" },
      { label: "Email threads", strength: "partial" },
      { label: "Product usage", strength: "weak" },
      { label: "Customer calls", strength: "missing" },
    ],
  },
  outcome: {
    outcomeLabel: "Risk downgraded",
    impactRows: [
      { label: "Outcome", value: "Risk downgraded" },
      { label: "Impact", value: "$2.04M ARR exposure reduced" },
      { label: "Renewals affected", value: "3 accounts" },
      { label: "Time to resolution", value: "2 days" },
      { label: "Business impact", value: "Positive" },
      {
        label: "Notes",
        value:
          "No new sync failures observed for 5 business days.",
      },
    ],
  },
  accuracy: {
    forecastId: "fcst_beacon_renewal",
    statement: "Beacon renewal risk likely to increase",
    initialConfidencePct: 78,
    outcome: "true",
    resolvedAt: "2026-05-18T08:15:00Z",
    calibrationImpactPp: 2,
  },
  relatedContext: {
    todayItems: [
      { label: "Escalate customer risk", proposedChangeId: "pc_customer_escalation" },
    ],
    modelLinks: [
      { label: "Customers & Revenue · Systems & Capacity", href: "/model?focus=customers_revenue" },
    ],
    forecastLinks: [
      { label: "Beacon renewal risk", forecastId: "fcst_beacon_renewal" },
    ],
  },
  decisionReceipt: {
    acceptedBy: "Diana",
    acceptedAt: "2026-05-16T09:22:00Z",
    proposedChangeLabel:
      "Escalate customer risk for Salesforce sync instability",
    changedRows: [
      { label: "Risk level", before: "Watch", after: "Critical" },
      { label: "Owner", before: "Unassigned", after: "VP Engineering" },
      { label: "Re-evaluation", before: "—", after: "48h" },
    ],
    fyralisActions: [
      "Created escalation",
      "Notified VP Engineering",
      "Linked 3 renewal commitments",
      "Scheduled re-evaluation",
    ],
    outcomeLabel: "Risk downgraded after 5 days",
  },
};

const CHAIN_PRICING: LedgerChainDetail = {
  id: "chain_pricing_owner",
  title: "Pricing Ownership Delay",
  status: "open",
  summary:
    "Pricing ownership has remained unassigned for 6 days, blocking two Q3 commitments.",
  eventCount: 4,
  impactLabel: "2 commitments blocked",
  startedAt: "2026-05-12T10:00:00Z",
  stages: [
    { id: "s1", label: "Detected", status: "complete", timestamp: "2026-05-12T10:00:00Z" },
    { id: "s2", label: "Proposed", status: "complete", timestamp: "2026-05-13T11:30:00Z" },
    { id: "s3", label: "Monitoring", status: "current", timestamp: "2026-05-14T09:00:00Z" },
    { id: "s4", label: "Resolved", status: "pending" },
  ],
  events: [
    {
      id: "ev1",
      timestamp: "2026-05-12T10:00:00Z",
      type: "model_item_updated",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Ownership gap detected",
      description: "Pricing has no current owner across two commitments.",
      chainId: "chain_pricing_owner",
    },
    {
      id: "ev2",
      timestamp: "2026-05-13T11:30:00Z",
      type: "proposed_change_created",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Owner proposal posted",
      description: "Suggest CRO accept pricing ownership through Q3.",
      chainId: "chain_pricing_owner",
    },
    {
      id: "ev3",
      timestamp: "2026-05-14T09:00:00Z",
      type: "reevaluation_scheduled",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Re-evaluation scheduled",
      description: "Will re-evaluate ownership Friday if unresolved.",
      chainId: "chain_pricing_owner",
    },
    {
      id: "ev4",
      timestamp: "2026-05-18T08:00:00Z",
      type: "claim_contested",
      actor: { type: "user", name: "Parker" },
      title: "Owner pushed back",
      description: "CRO requested handoff doc before accepting ownership.",
      chainId: "chain_pricing_owner",
    },
  ],
  outcome: {
    outcomeLabel: "Still open",
    impactRows: [
      { label: "Status", value: "Owner unassigned" },
      { label: "Commitments blocked", value: "2" },
      { label: "Days open", value: "6" },
    ],
    notes: "Re-evaluation Friday if still unresolved.",
  },
  relatedContext: {
    todayItems: [
      { label: "Assign pricing owner", proposedChangeId: "pc_assign_pricing" },
    ],
    modelLinks: [
      { label: "Commercial · Pricing", href: "/model?focus=commercial_pricing" },
    ],
    forecastLinks: [],
  },
};

const CHAIN_CAPACITY: LedgerChainDetail = {
  id: "chain_engineering_capacity",
  title: "Engineering Capacity Forecast",
  status: "resolved_true",
  summary:
    "Engineering utilization forecast resolved true 2 days ahead of the predicted window.",
  eventCount: 5,
  impactLabel: "Forecast 72% → True",
  startedAt: "2026-05-10T09:00:00Z",
  resolvedAt: "2026-05-17T13:00:00Z",
  stages: [
    { id: "s1", label: "Forecasted", status: "complete", timestamp: "2026-05-10T09:00:00Z" },
    { id: "s2", label: "Monitoring", status: "complete", timestamp: "2026-05-13T12:00:00Z" },
    { id: "s3", label: "Resolved", status: "complete", timestamp: "2026-05-17T13:00:00Z" },
  ],
  events: [
    {
      id: "ev1",
      timestamp: "2026-05-10T09:00:00Z",
      type: "forecast_created",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Forecast created",
      description: "Engineering utilization likely to exceed 90% by May 21.",
      confidencePct: 72,
      chainId: "chain_engineering_capacity",
    },
    {
      id: "ev2",
      timestamp: "2026-05-13T12:00:00Z",
      type: "evidence_added",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Hiring delay observed",
      description: "Two open reqs slipped beyond start dates.",
      chainId: "chain_engineering_capacity",
    },
    {
      id: "ev3",
      timestamp: "2026-05-17T13:00:00Z",
      type: "forecast_resolved_true",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Forecast resolved true",
      description: "Utilization crossed 90% on May 19 — 2 days early.",
      chainId: "chain_engineering_capacity",
    },
  ],
  accuracy: {
    forecastId: "fcst_eng_capacity",
    statement: "Engineering utilization to exceed 90% by May 21",
    initialConfidencePct: 72,
    finalConfidencePct: 88,
    outcome: "true",
    resolvedAt: "2026-05-17T13:00:00Z",
    calibrationImpactPp: 1,
    notes: "Capacity signal was early by ~2 days. Hiring delay dominant.",
  },
  relatedContext: {
    todayItems: [],
    modelLinks: [
      { label: "Systems & Capacity · Engineering", href: "/model?focus=systems_capacity" },
    ],
    forecastLinks: [
      { label: "Engineering utilization", forecastId: "fcst_eng_capacity" },
    ],
  },
};

const CHAIN_DESIGN: LedgerChainDetail = {
  id: "chain_design_partner",
  title: "Design Partner Health Drift",
  status: "monitoring",
  summary:
    "Partner engagement improved after escalation; monitoring for sustained signal before downgrade.",
  eventCount: 3,
  impactLabel: "Partner engagement improved",
  startedAt: "2026-05-16T09:00:00Z",
  stages: [
    { id: "s1", label: "Detected", status: "complete", timestamp: "2026-05-16T09:00:00Z" },
    { id: "s2", label: "Proposed", status: "complete", timestamp: "2026-05-16T11:30:00Z" },
    { id: "s3", label: "Monitoring", status: "current", timestamp: "2026-05-17T15:00:00Z" },
    { id: "s4", label: "Resolved", status: "pending" },
  ],
  events: [
    {
      id: "ev1",
      timestamp: "2026-05-16T09:00:00Z",
      type: "model_item_updated",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Health drift detected",
      description: "Partner engagement signals weakened across 3 accounts.",
      chainId: "chain_design_partner",
    },
    {
      id: "ev2",
      timestamp: "2026-05-16T11:30:00Z",
      type: "proposed_change_accepted",
      actor: { type: "user", name: "Diana" },
      title: "Outreach proposal accepted",
      description: "Schedule executive outreach to top three design partners.",
      chainId: "chain_design_partner",
    },
    {
      id: "ev3",
      timestamp: "2026-05-17T15:00:00Z",
      type: "evidence_added",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Engagement improving",
      description: "Two of three partners re-engaged within 24h.",
      chainId: "chain_design_partner",
    },
  ],
  outcome: {
    outcomeLabel: "Monitoring",
    impactRows: [
      { label: "Status", value: "Engagement improving" },
      { label: "Accounts re-engaged", value: "2 of 3" },
      { label: "Re-evaluation", value: "Friday" },
    ],
  },
  relatedContext: {
    todayItems: [],
    modelLinks: [
      { label: "Customers & Revenue · Design Partners", href: "/model?focus=design_partners" },
    ],
    forecastLinks: [],
  },
};

const CHAIN_RESCOPE: LedgerChainDetail = {
  id: "chain_conv_ai_rescope",
  title: "Conversation-AI Re-scope",
  status: "corrected",
  summary:
    "Initial scope proposal was corrected to reflect Q3 capacity constraints. New scope accepted.",
  eventCount: 5,
  impactLabel: "Scope changed",
  startedAt: "2026-05-13T10:00:00Z",
  resolvedAt: "2026-05-14T17:00:00Z",
  stages: [
    { id: "s1", label: "Proposed", status: "complete", timestamp: "2026-05-13T10:00:00Z" },
    { id: "s2", label: "Corrected", status: "complete", timestamp: "2026-05-14T09:00:00Z" },
    { id: "s3", label: "Accepted", status: "complete", timestamp: "2026-05-14T17:00:00Z" },
  ],
  events: [
    {
      id: "ev1",
      timestamp: "2026-05-13T10:00:00Z",
      type: "proposed_change_created",
      actor: { type: "fyralis", name: "Fyralis" },
      title: "Initial scope proposed",
      description: "Conversation-AI scope set to ship Q3.",
      chainId: "chain_conv_ai_rescope",
    },
    {
      id: "ev2",
      timestamp: "2026-05-14T09:00:00Z",
      type: "claim_corrected",
      actor: { type: "user", name: "Parker" },
      title: "Scope corrected",
      description: "Removed multi-tenant work from Q3 plan.",
      chainId: "chain_conv_ai_rescope",
    },
    {
      id: "ev3",
      timestamp: "2026-05-14T17:00:00Z",
      type: "proposed_change_accepted",
      actor: { type: "user", name: "Diana" },
      title: "Corrected scope accepted",
      description: "Q3 plan reflects revised capacity.",
      chainId: "chain_conv_ai_rescope",
    },
  ],
  outcome: {
    outcomeLabel: "Scope changed",
    impactRows: [
      { label: "Outcome", value: "Q3 plan revised" },
      { label: "Capacity match", value: "Aligned" },
      { label: "Resolution", value: "1 day" },
    ],
  },
  relatedContext: {
    todayItems: [],
    modelLinks: [
      { label: "Product · Conversation-AI", href: "/model?focus=conversation_ai" },
    ],
    forecastLinks: [],
  },
};

const CHAINS: LedgerChainDetail[] = [
  CHAIN_CUSTOMER,
  CHAIN_PRICING,
  CHAIN_CAPACITY,
  CHAIN_DESIGN,
  CHAIN_RESCOPE,
];

function asCard(c: LedgerChainDetail): LedgerChainCard {
  const {
    id,
    title,
    status,
    summary,
    eventCount,
    impactLabel,
    startedAt,
    resolvedAt,
    stages,
  } = c;
  return { id, title, status, summary, eventCount, impactLabel, startedAt, resolvedAt, stages };
}

export const LEDGER_PAGE_FIXTURE: LedgerPagePayload = {
  header: {
    eventCount: 42,
    chainCount: 8,
    resolvedCount: 3,
    forecastsClosedCount: 2,
    correctionCount: 1,
    dateRange: {
      label: "Last 30 days",
      start: "2026-04-18",
      end: "2026-05-18",
    },
  },
  brief: {
    statement:
      "Since your last review, customer reliability moved from active risk to monitoring and engineering capacity forecast resolved true.",
    resolved: [
      { id: "b1", label: "Customer reliability risk downgraded" },
      { id: "b2", label: "Beacon renewal risk resolved true" },
    ],
    stillOpen: [
      { id: "b3", label: "Pricing ownership remains unassigned", severity: "medium" },
      { id: "b4", label: "Q3 scope tradeoff is 6 days stale", severity: "medium" },
    ],
    learned: [
      { id: "b5", label: "Capacity forecasts were early by ~2 days" },
      { id: "b6", label: "Escalations resolve faster with owners in <48h" },
    ],
  },
  chains: CHAINS.map(asCard),
  chainDetails: Object.fromEntries(CHAINS.map((c) => [c.id, c])),
  defaultSelectedChainId: CHAIN_CUSTOMER.id,
  accuracy: {
    calibratedAccuracyPct: 71,
    resolvedTrue: 7,
    resolvedFalse: 2,
    pending: 3,
    byDomain: [
      { domain: "Customers & Revenue", accuracyPct: 78, resolvedTrue: 4, resolvedFalse: 1, pending: 1 },
      { domain: "Systems & Capacity", accuracyPct: 75, resolvedTrue: 2, resolvedFalse: 0, pending: 1 },
      { domain: "Commercial", accuracyPct: 50, resolvedTrue: 1, resolvedFalse: 1, pending: 1 },
    ],
    resolved: [
      {
        chainId: "chain_customer_reliability",
        forecast: "Beacon renewal risk likely to increase",
        initialConfidencePct: 78,
        outcome: "true",
        resolvedAt: "2026-05-18T08:15:00Z",
        calibrationImpactPp: 2,
      },
      {
        chainId: "chain_engineering_capacity",
        forecast: "Engineering utilization to exceed 90% by May 21",
        initialConfidencePct: 72,
        finalConfidencePct: 88,
        outcome: "true",
        resolvedAt: "2026-05-17T13:00:00Z",
        calibrationImpactPp: 1,
      },
      {
        chainId: "chain_acme_churn",
        forecast: "Acme churn risk to spike by May 10",
        initialConfidencePct: 64,
        outcome: "false",
        resolvedAt: "2026-05-11T11:00:00Z",
        calibrationImpactPp: -3,
      },
    ],
    falsePositives: [
      { label: "Acme churn risk spike", note: "Renewal landed early; signal weakened mid-window." },
    ],
    falseNegatives: [
      { label: "Marketing budget overrun", note: "Missed two off-platform vendor invoices." },
    ],
    missedContext: [
      { label: "Customer calls", note: "No transcripts ingested in Beacon escalation." },
    ],
  },
  auditEvents: CHAINS.flatMap((c) =>
    c.events.map((e) => ({
      id: e.id + "@" + c.id,
      chainId: c.id,
      chainTitle: c.title,
      timestamp: e.timestamp,
      type: e.type,
      actor: e.actor,
      object: { id: c.id, label: c.title },
      before: e.before ? JSON.stringify(e.before) : undefined,
      after: e.after ? JSON.stringify(e.after) : undefined,
      source: undefined,
    })),
  ),
};
