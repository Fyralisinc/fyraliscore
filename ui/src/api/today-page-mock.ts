// Mock fixture for the Today page v2 (USE_MOCK=1 dev + e2e tests).
// Mirrors the wire shape served by services/gateway/today_routes.py.
//
// Two-pane scenario:
//   - Primary judgment: Salesforce sync escalation (needs_authority)
//   - 3 other changes: pricing decision, delivery delegation,
//     people/teams monitoring
//   - 1 delegated, 1 monitoring already handled

import type {
  ApplyResult,
  CorrectionBody,
  CorrectionResult,
  DecisionDelta,
  DelegateBody,
  DelegateResult,
  EvidenceResponse,
  TodayPageData,
} from "./today-page-types";

const NOW = "2026-05-17T13:40:00Z";
const SINCE = "2026-05-16T18:00:00Z";

const PRIMARY: DecisionDelta = {
  id: "delta-primary-001",
  title: "Escalate customer risk for Salesforce sync instability",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "needs_authority",
  priorityRank: 0,
  sourceCategory: "customers_revenue",
  relatedCategories: ["customers_revenue", "systems_capacity", "commitments"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T13:22:00Z",
  updatedAt: "2026-05-17T13:22:00Z",
  currentState: [
    { key: "risk", label: "Risk level", value: "At watch", valueType: "status", severity: "watch" },
    { key: "owner", label: "Owner", value: "Unassigned", valueType: "owner", severity: "neutral" },
    { key: "reeval", label: "Re-evaluate in", value: "7 days", valueType: "duration", severity: "neutral" },
    { key: "notify", label: "Notify", value: "—", valueType: "text", severity: "neutral" },
  ],
  proposedState: [
    { key: "risk", label: "Risk level", value: "Critical", valueType: "status", severity: "critical" },
    { key: "owner", label: "Owner", value: "VP Engineering", valueType: "owner", severity: "neutral" },
    { key: "reeval", label: "Re-evaluate in", value: "48 hours", valueType: "duration", severity: "watch" },
    { key: "notify", label: "Notify", value: "VP Engineering and 3 account owners", valueType: "text", severity: "neutral" },
  ],
  summaryLine: "At watch → Critical",
  whyThisMatters:
    "Three anchor customers are experiencing recurring sync failures. Renewal exposure is increasing as confidence in reliability declines.",
  keyMetrics: [
    { label: "$2.04M at risk in renewal pipeline", value: "$2.04M", unit: "renewal pipeline", severity: "critical" },
    { label: "3 anchor customers affected", value: 3, unit: "customers", severity: "high" },
    { label: "12 signals across 4 sources", value: 12, unit: "signals", severity: "medium" },
    { label: "78% confidence", value: 78, unit: "percent", severity: "high" },
  ],
  evidenceSummary: {
    totalSignals: 4,
    quality: "strong",
    groups: [
      { id: "src-sf", label: "5 sync failure alerts in last 7 days", sourceType: "Salesforce logs", count: 5, quality: "strong", strengthScore: 0.92 },
      { id: "src-tickets", label: "3 support tickets from anchor customers in last 10 days", sourceType: "Support tickets", count: 3, quality: "strong", strengthScore: 0.9 },
      { id: "src-renewal", label: "Renewal threads show declining confidence", sourceType: "Priority, Northvale, Beacon", count: 1, quality: "medium", strengthScore: 0.74 },
      { id: "src-usage", label: "Usage reporting incomplete for 2 accounts", sourceType: "Product analytics", count: 1, quality: "partial", strengthScore: 0.5 },
    ],
  },
  missingContext: [
    { id: "miss-1", text: "No RCA provided by Engineering yet", severity: "medium" },
    { id: "miss-2", text: "No customer call transcripts from this week", severity: "medium" },
    { id: "miss-3", text: "Product usage trend not connected for Priority", severity: "low" },
  ],
  impactIfAccepted: [
    { id: "op-1", text: "Create escalation in Risks & Constraints", operationType: "create_node", severity: "positive" },
    { id: "op-2", text: "Notify VP Engineering and account owners", operationType: "notify_actor", severity: "neutral" },
    { id: "op-3", text: "Link 3 renewal commitments", operationType: "link_nodes", severity: "neutral" },
    { id: "op-4", text: "Re-evaluate in 48 hours", operationType: "schedule_re_evaluation", severity: "neutral" },
  ],
  relatedModelLinks: [
    { category: "customers_revenue", label: "Customers & Revenue", href: "/model?focus=category&categoryId=customers_revenue" },
    { category: "risks_constraints", label: "Risks & Constraints", href: "/model?focus=category&categoryId=risks_constraints" },
    { category: "commitments", label: "Commitments", href: "/model?focus=category&categoryId=commitments" },
  ],
  availableActions: ["accept", "delegate", "review_evidence", "report_correction"],
  applyPreview: {
    nodeOpsCount: 4,
    notificationsCount: 4,
    reEvaluationScheduledAt: "2026-05-19T13:22:00Z",
    ledgerEventWillBeCreated: true,
  },
  targetNodeKind: "customer",
  targetNodeId: "00000000-0000-4000-a000-000000000001",
  confidence: 0.78,
  resolutionTargetAt: "2026-05-19T13:22:00Z",
  evidence: [
    { id: "ev-1", source: "salesforce", sourceLabel: "Salesforce logs", title: "Sync failure alert #441 — Priority", occurredAt: "2026-05-14T09:21:00Z", trustTier: "attested", quality: "strong", excerpt: "Priority reported recurring Salesforce sync failures affecting renewal reporting.", ordinal: 0 },
    { id: "ev-2", source: "support", sourceLabel: "Support tickets", title: "Northvale ticket #492 — sync down 2h", occurredAt: "2026-05-15T11:02:00Z", trustTier: "attested", quality: "strong", excerpt: "Northvale ops escalation — Salesforce sync down for 2h during business hours.", ordinal: 1 },
    { id: "ev-3", source: "crm", sourceLabel: "CRM renewal thread", title: "Account: Beacon — health declining", occurredAt: "2026-05-16T08:11:00Z", trustTier: "verified", quality: "medium", excerpt: "Beacon CSM noted account health moved from B to C; sync uptime flagged as primary concern.", ordinal: 2 },
    { id: "ev-4", source: "product", sourceLabel: "Product analytics", title: "Usage reporting incomplete for Priority + Beacon", occurredAt: "2026-05-16T22:40:00Z", trustTier: "secondhand", quality: "partial", excerpt: "Usage events not flowing reliably; gaps in last 7 days for 2 accounts.", ordinal: 3 },
  ],
};

const OTHER_PRICING: DecisionDelta = {
  id: "delta-other-pricing",
  title: "Assign owner for pricing model decision",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "delegatable",
  priorityRank: 1,
  sourceCategory: "decisions",
  relatedCategories: ["decisions", "customers_revenue", "finance_capital"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T11:58:00Z",
  updatedAt: "2026-05-17T11:58:00Z",
  currentState: [
    { key: "owner", label: "Owner", value: "Unassigned", valueType: "owner", severity: "neutral" },
    { key: "decision_date", label: "Decision needed by", value: "Open-ended", valueType: "date", severity: "neutral" },
  ],
  proposedState: [
    { key: "owner", label: "Owner", value: "CFO", valueType: "owner", severity: "neutral" },
    { key: "decision_date", label: "Decision needed by", value: "May 22 (5 business days)", valueType: "date", severity: "watch" },
  ],
  summaryLine: "Unassigned",
  whyThisMatters:
    "Two commitments and one Q3 forecast are blocked pending a pricing decision. Sales has flagged this as the most repeated objection in the last 30 days.",
  keyMetrics: [
    { label: "$720K opportunity blocked", value: "$720K", unit: "opportunity", severity: "high" },
    { label: "2 commitments blocked", value: 2, unit: "commitments", severity: "medium" },
    { label: "9 signals across 3 sources", value: 9, unit: "signals", severity: "medium" },
  ],
  evidenceSummary: {
    totalSignals: 3,
    quality: "medium",
    groups: [
      { id: "src-slack", label: "4 Slack threads in #pricing-decisions", sourceType: "Slack", count: 4, quality: "medium" },
      { id: "src-crm", label: "3 CRM deals stalled on pricing question", sourceType: "Salesforce", count: 3, quality: "strong" },
      { id: "src-finance", label: "Finance model awaiting input", sourceType: "Pigment", count: 2, quality: "medium" },
    ],
  },
  missingContext: [
    { id: "miss-1", text: "No alignment yet between Sales and Finance on tier structure", severity: "medium" },
  ],
  impactIfAccepted: [
    { id: "op-1", text: "Assign CFO as decision owner", operationType: "update_node" },
    { id: "op-2", text: "Notify CFO with context bundle", operationType: "notify_actor" },
    { id: "op-3", text: "Unblock 2 dependent commitments", operationType: "link_nodes" },
    { id: "op-4", text: "Re-evaluate in 5 business days", operationType: "schedule_re_evaluation" },
  ],
  relatedModelLinks: [
    { category: "decisions", label: "Decisions", href: "/model?focus=category&categoryId=decisions" },
  ],
  availableActions: ["delegate", "accept", "review_evidence", "report_correction"],
  applyPreview: { nodeOpsCount: 1, notificationsCount: 1, reEvaluationScheduledAt: "2026-05-22T11:58:00Z", ledgerEventWillBeCreated: true },
  confidence: 0.72,
  resolutionTargetAt: "2026-05-22T17:00:00Z",
};

const OTHER_Q3_SCOPE: DecisionDelta = {
  id: "delta-other-q3-scope",
  title: "Clarify Q3 scope trade-off",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "needs_authority",
  priorityRank: 2,
  sourceCategory: "commitments",
  relatedCategories: ["commitments", "decisions", "people_teams"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T10:46:00Z",
  updatedAt: "2026-05-17T10:46:00Z",
  currentState: [
    { key: "scope", label: "Q3 scope", value: "Five committed initiatives", valueType: "text", severity: "neutral" },
    { key: "alignment", label: "Cross-team alignment", value: "Diverging", valueType: "status", severity: "watch" },
  ],
  proposedState: [
    { key: "scope", label: "Q3 scope", value: "Drop or defer one initiative", valueType: "text", severity: "watch" },
    { key: "alignment", label: "Cross-team alignment", value: "Re-aligned by next Friday", valueType: "status", severity: "positive" },
  ],
  summaryLine: "Product & Engineering misaligned",
  whyThisMatters:
    "Product and Engineering are diverging on Q3 priorities. Without an explicit trade-off, at least one committed initiative is likely to slip past the quarter.",
  keyMetrics: [
    { label: "1 commitment at risk of slipping", value: 1, unit: "commitment", severity: "high" },
    { label: "7 signals across 2 sources", value: 7, unit: "signals", severity: "medium" },
  ],
  evidenceSummary: {
    totalSignals: 2,
    quality: "medium",
    groups: [
      { id: "src-linear", label: "5 Linear issues flagged as P0-conflict", sourceType: "Linear", count: 5, quality: "medium" },
      { id: "src-meeting", label: "Last 2 leadership meetings show split priorities", sourceType: "Meeting notes", count: 2, quality: "partial" },
    ],
  },
  missingContext: [
    { id: "miss-1", text: "No customer impact assessment for each candidate cut", severity: "medium" },
  ],
  impactIfAccepted: [
    { id: "op-1", text: "Open a Q3 scope decision in Decisions", operationType: "create_node" },
    { id: "op-2", text: "Notify Product + Engineering leads", operationType: "notify_actor" },
    { id: "op-3", text: "Re-evaluate by next Friday", operationType: "schedule_re_evaluation" },
  ],
  relatedModelLinks: [
    { category: "commitments", label: "Commitments", href: "/model?focus=category&categoryId=commitments" },
  ],
  availableActions: ["accept", "delegate", "review_evidence", "report_correction"],
  applyPreview: { nodeOpsCount: 1, notificationsCount: 2, ledgerEventWillBeCreated: true },
  confidence: 0.65,
  resolutionTargetAt: "2026-05-24T17:00:00Z",
};

const OTHER_PACKAGING: DecisionDelta = {
  id: "delta-other-packaging",
  title: "Approve enterprise packaging proposal",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "needs_authority",
  priorityRank: 3,
  sourceCategory: "decisions",
  relatedCategories: ["decisions", "customers_revenue"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T09:32:00Z",
  updatedAt: "2026-05-17T09:32:00Z",
  currentState: [
    { key: "status", label: "Packaging status", value: "Draft", valueType: "status", severity: "neutral" },
  ],
  proposedState: [
    { key: "status", label: "Packaging status", value: "Approved for pilot", valueType: "status", severity: "positive" },
  ],
  summaryLine: "$1.2M pipeline affected",
  whyThisMatters:
    "GTM has $1.2M of pipeline waiting on the enterprise packaging proposal. Three deals stalled this week pending tier clarity.",
  keyMetrics: [
    { label: "$1.2M pipeline affected", value: "$1.2M", unit: "pipeline", severity: "high" },
    { label: "3 deals waiting", value: 3, unit: "deals", severity: "medium" },
  ],
  evidenceSummary: {
    totalSignals: 2,
    quality: "strong",
    groups: [
      { id: "src-crm", label: "3 enterprise opps stalled on packaging clarity", sourceType: "Salesforce", count: 3, quality: "strong" },
      { id: "src-rev", label: "Revenue Ops requested approval timeline", sourceType: "Email", count: 1, quality: "medium" },
    ],
  },
  missingContext: [
    { id: "miss-1", text: "Legal sign-off on tier-3 terms not yet documented", severity: "medium" },
  ],
  impactIfAccepted: [
    { id: "op-1", text: "Approve packaging for pilot", operationType: "update_node" },
    { id: "op-2", text: "Notify GTM + Revenue Ops", operationType: "notify_actor" },
    { id: "op-3", text: "Unblock 3 dependent deals", operationType: "link_nodes" },
  ],
  relatedModelLinks: [
    { category: "decisions", label: "Decisions", href: "/model?focus=category&categoryId=decisions" },
  ],
  availableActions: ["accept", "delegate", "review_evidence", "report_correction"],
  applyPreview: { nodeOpsCount: 1, notificationsCount: 2, ledgerEventWillBeCreated: true },
  confidence: 0.81,
  resolutionTargetAt: "2026-05-19T17:00:00Z",
};

const OTHER_DELIVERY: DecisionDelta = {
  id: "delta-other-delivery",
  title: "Reassign delivery owner for Northvale onboarding",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "delegatable",
  priorityRank: 4,
  sourceCategory: "commitments",
  relatedCategories: ["commitments", "people_teams"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T09:11:00Z",
  updatedAt: "2026-05-17T09:11:00Z",
  currentState: [
    { key: "owner", label: "Owner", value: "Mira Chen", valueType: "owner" },
    { key: "capacity", label: "Owner capacity", value: "Over-allocated", valueType: "status", severity: "watch" },
  ],
  proposedState: [
    { key: "owner", label: "Owner", value: "Tom Reilly", valueType: "owner" },
    { key: "capacity", label: "Owner capacity", value: "Within plan", valueType: "status", severity: "positive" },
  ],
  summaryLine: "Mira → Tom",
  whyThisMatters:
    "Mira is over-allocated this sprint and Northvale onboarding is slipping by a week. Tom has bandwidth and prior context.",
  keyMetrics: [
    { label: "$340K renewal at risk", value: "$340K", unit: "renewal", severity: "high" },
    { label: "1 anchor customer affected", value: 1, unit: "customers", severity: "medium" },
    { label: "5 signals across 2 sources", value: 5, unit: "signals", severity: "low" },
  ],
  evidenceSummary: {
    totalSignals: 2,
    quality: "medium",
    groups: [
      { id: "src-linear", label: "3 onboarding tasks blocked in Linear", sourceType: "Linear", count: 3, quality: "medium" },
      { id: "src-cal", label: "Mira's calendar shows 130% allocation this week", sourceType: "Calendar", count: 2, quality: "partial" },
    ],
  },
  missingContext: [],
  impactIfAccepted: [
    { id: "op-1", text: "Reassign delivery commitment to Tom Reilly", operationType: "update_node" },
    { id: "op-2", text: "Notify Mira, Tom, and Northvale account owner", operationType: "notify_actor" },
    { id: "op-3", text: "Re-evaluate in 7 days", operationType: "schedule_re_evaluation" },
  ],
  relatedModelLinks: [
    { category: "commitments", label: "Commitments", href: "/model?focus=category&categoryId=commitments" },
  ],
  availableActions: ["delegate", "accept", "review_evidence", "report_correction"],
  applyPreview: { nodeOpsCount: 1, notificationsCount: 3, ledgerEventWillBeCreated: true },
  confidence: 0.66,
  resolutionTargetAt: "2026-05-21T17:00:00Z",
};

const OTHER_VP_SUCCESSOR: DecisionDelta = {
  id: "delta-other-vp-successor",
  title: "VP Eng successor open 8+ weeks — Q3 forecast-model release blocked",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "needs_authority",
  priorityRank: 5,
  sourceCategory: "people_teams",
  relatedCategories: ["people_teams", "commitments", "systems_capacity"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T08:14:00Z",
  updatedAt: "2026-05-17T08:14:00Z",
  currentState: [
    { key: "status", label: "Search status", value: "Open 8+ weeks", valueType: "status", severity: "watch" },
  ],
  proposedState: [
    { key: "status", label: "Search status", value: "Promote internal interim", valueType: "status", severity: "positive" },
  ],
  summaryLine: "Open 8+ weeks → Promote interim",
  whyThisMatters:
    "VP Engineering successor search has been open eight weeks. The Q3 forecast-model release depends on engineering leadership being in place by June 1.",
  keyMetrics: [
    { label: "Q3 release at risk", value: "Q3", unit: "release", severity: "high" },
    { label: "8 weeks open", value: 8, unit: "weeks", severity: "medium" },
  ],
  evidenceSummary: {
    totalSignals: 2,
    quality: "medium",
    groups: [
      { id: "src-greenhouse", label: "12 candidates screened; 0 progressed past final", sourceType: "Greenhouse", count: 12, quality: "medium" },
      { id: "src-board", label: "Board prep flagged leadership gap as blocker", sourceType: "Board notes", count: 1, quality: "strong" },
    ],
  },
  missingContext: [
    { id: "miss-1", text: "Interim candidate has not formally accepted", severity: "high" },
  ],
  impactIfAccepted: [
    { id: "op-1", text: "Open interim promotion decision", operationType: "create_node" },
    { id: "op-2", text: "Notify People Ops + Board", operationType: "notify_actor" },
  ],
  relatedModelLinks: [
    { category: "people_teams", label: "People & Teams", href: "/model?focus=category&categoryId=people_teams" },
  ],
  availableActions: ["accept", "delegate", "review_evidence", "report_correction"],
  applyPreview: { nodeOpsCount: 1, notificationsCount: 2, ledgerEventWillBeCreated: true },
  confidence: 0.61,
  resolutionTargetAt: "2026-05-26T17:00:00Z",
};

const OTHER_MONITORING: DecisionDelta = {
  id: "delta-other-monitoring",
  title: "Monitor team capacity drift in Platform",
  userFacingType: "proposed_change",
  internalType: "decision_delta",
  status: "monitoring",
  priorityRank: 6,
  sourceCategory: "systems_capacity",
  relatedCategories: ["systems_capacity", "people_teams"],
  proposedBy: "fyralis",
  createdAt: "2026-05-17T07:02:00Z",
  updatedAt: "2026-05-17T07:02:00Z",
  currentState: [{ key: "capacity", label: "Capacity utilization", value: "Stable", severity: "neutral" }],
  proposedState: [{ key: "capacity", label: "Capacity utilization", value: "Drift detected", severity: "watch" }],
  summaryLine: "Stable → Drift detected",
  whyThisMatters:
    "Platform team throughput is trending down 8% week-over-week. No customer impact yet, but Fyralis is tracking for a sustained pattern before re-classifying.",
  keyMetrics: [
    { label: "8% throughput drift", value: "8%", unit: "throughput", severity: "medium" },
    { label: "6 signals across 2 sources", value: 6, unit: "signals", severity: "low" },
  ],
  evidenceSummary: {
    totalSignals: 2,
    quality: "partial",
    groups: [
      { id: "src-linear", label: "4 Linear cycles closed below target velocity", sourceType: "Linear", count: 4, quality: "partial" },
      { id: "src-product", label: "Product event volume flat for 2 weeks", sourceType: "Product analytics", count: 2, quality: "partial" },
    ],
  },
  missingContext: [
    { id: "miss-1", text: "No 1:1 notes connected — root cause unknown", severity: "low" },
  ],
  impactIfAccepted: [
    { id: "op-1", text: "Open monitoring on Platform capacity", operationType: "update_node" },
    { id: "op-2", text: "Re-evaluate in 7 days", operationType: "schedule_re_evaluation" },
  ],
  relatedModelLinks: [
    { category: "systems_capacity", label: "Systems & Capacity", href: "/model?focus=category&categoryId=systems_capacity" },
  ],
  availableActions: ["accept", "review_evidence", "open_model", "snooze"],
  applyPreview: { nodeOpsCount: 1, notificationsCount: 0, reEvaluationScheduledAt: "2026-05-24T07:02:00Z", ledgerEventWillBeCreated: true },
  confidence: 0.52,
};

export const TODAY_PAGE_FIXTURE: TodayPageData = {
  viewer: {
    userId: "actor-rachin",
    name: "Rachin",
    role: "CEO",
    tenantId: "tenant-fyralis-demo",
  },
  lastReviewAt: SINCE,
  generatedAt: NOW,
  summary: {
    signalsProcessed: 98,
    signalsAbsorbed: 91,
    modelUpdates: 12,
    needJudgment: 7,
    requiresAuthority: 4,
    delegatable: 2,
    monitoring: 1,
    contested: 0,
    exposure: { amount: 2_040_000, currency: "USD", formatted: "$2.04M" },
  },
  primaryJudgment: PRIMARY,
  otherChanges: [
    OTHER_PRICING,
    OTHER_Q3_SCOPE,
    OTHER_PACKAGING,
    OTHER_DELIVERY,
    OTHER_VP_SUCCESSOR,
    OTHER_MONITORING,
  ],
  handledWithoutYou: {
    signalsAbsorbed: 91,
    modelUpdatesApplied: 12,
    itemsUnderMonitoring: 5,
    delegatedChanges: 1,
    contestedChanges: 0,
    reassuranceCopy:
      "Customer reliability and pricing ownership are the only areas requiring your attention.",
  },
};

const ALL: Record<string, DecisionDelta> = Object.fromEntries(
  [
    PRIMARY,
    OTHER_PRICING,
    OTHER_Q3_SCOPE,
    OTHER_PACKAGING,
    OTHER_DELIVERY,
    OTHER_VP_SUCCESSOR,
    OTHER_MONITORING,
  ].map((d) => [d.id, d]),
);

// In-memory mutation tracker so the mock survives the round-trip of an
// apply / delegate / correction call. We don't persist beyond the
// session — refresh resets the state.
const MUTATED: Map<string, DecisionDelta> = new Map();

function withMutations(d: DecisionDelta): DecisionDelta {
  return MUTATED.get(d.id) ?? d;
}

export function mockGetTodayPage(): TodayPageData {
  const primary = withMutations(PRIMARY);
  const others = [
    OTHER_PRICING,
    OTHER_Q3_SCOPE,
    OTHER_PACKAGING,
    OTHER_DELIVERY,
    OTHER_VP_SUCCESSOR,
    OTHER_MONITORING,
  ].map(withMutations);
  return {
    ...TODAY_PAGE_FIXTURE,
    primaryJudgment:
      primary.status === "accepted" || primary.status === "archived"
        ? null
        : primary,
    otherChanges: others.filter(
      (d) => d.status !== "accepted" && d.status !== "archived",
    ),
  };
}

export function mockGetDelta(id: string): DecisionDelta | null {
  const d = ALL[id];
  if (!d) return null;
  return withMutations(d);
}

export function mockGetEvidence(id: string): EvidenceResponse | null {
  const d = ALL[id];
  if (!d) return null;
  return {
    deltaId: id,
    totalSignals: d.evidenceSummary.totalSignals,
    evidenceGroups: d.evidenceSummary.groups,
    items: d.evidence ?? [],
  };
}

export function mockApply(id: string): ApplyResult | null {
  const d = ALL[id];
  if (!d) return null;
  const updated: DecisionDelta = {
    ...d,
    status: "accepted",
    updatedAt: new Date().toISOString(),
  };
  MUTATED.set(id, updated);
  const remaining = [
    PRIMARY,
    OTHER_PRICING,
    OTHER_Q3_SCOPE,
    OTHER_PACKAGING,
    OTHER_DELIVERY,
    OTHER_VP_SUCCESSOR,
    OTHER_MONITORING,
  ]
    .map(withMutations)
    .filter((x) => x.id !== id && x.status !== "accepted" && x.status !== "archived");
  return {
    status: "applied",
    resultMessage: `Change accepted. ${
      d.impactIfAccepted.length > 0
        ? `Executed ${d.impactIfAccepted.length} operation${d.impactIfAccepted.length === 1 ? "" : "s"}.`
        : "Recorded ledger event."
    }`,
    updatedDelta: updated,
    nextDeltaId: remaining[0]?.id ?? null,
    ledgerEventId: `ledger-${id}-mock`,
  };
}

export function mockDelegate(id: string, body: DelegateBody): DelegateResult | null {
  const d = ALL[id];
  if (!d) return null;
  const updated: DecisionDelta = {
    ...d,
    status: "delegated",
    annotations: {
      ...(d.annotations ?? {}),
      delegation: {
        owner_id: body.delegateToActorId,
        message: body.message ?? null,
        due_at: body.dueAt ?? null,
        notify_now: body.notifyNow,
        monitor_confirmation: body.monitorConfirmation,
        at: new Date().toISOString(),
      },
    },
  };
  MUTATED.set(id, updated);
  return {
    status: "delegated",
    resultMessage: "Delegated. Fyralis will monitor for confirmation.",
    updatedDelta: updated,
  };
}

export function mockCorrection(
  id: string,
  body: CorrectionBody,
): CorrectionResult | null {
  const d = ALL[id];
  if (!d) return null;
  const updated: DecisionDelta = {
    ...d,
    status: "correction_submitted",
    annotations: {
      ...(d.annotations ?? {}),
      correction: {
        type: body.correctionType,
        explanation: body.explanation,
        supporting_link: body.supportingLink ?? null,
        apply_to_related: body.applyToRelatedItems ?? false,
        by: "actor-rachin",
        at: new Date().toISOString(),
      },
    },
  };
  MUTATED.set(id, updated);
  return {
    status: "correction_submitted",
    resultMessage:
      "Correction submitted. Fyralis will re-evaluate this change and any dependent model items.",
    updatedDelta: updated,
  };
}

// Test-only: reset all in-memory mutations between e2e tests.
export function _resetTodayPageMock() {
  MUTATED.clear();
}
