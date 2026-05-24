// Ledger v2 data contracts.
// Spec: fyralis_ledger_page_implementation_spec_v1.md §27.

export type LedgerMode = "timeline" | "resolutions" | "accuracy" | "audit";

export type ChainStatus =
  | "open"
  | "monitoring"
  | "resolved"
  | "resolved_true"
  | "resolved_false"
  | "contested"
  | "corrected"
  | "archived";

export type StageLabel =
  | "Detected"
  | "Forecasted"
  | "Proposed"
  | "Accepted"
  | "Monitoring"
  | "Resolved"
  | "Corrected"
  | "Contested"
  | "Failed";

export type StageStatus = "complete" | "current" | "pending" | "failed";

export interface LedgerStage {
  id: string;
  label: StageLabel;
  status: StageStatus;
  timestamp?: string;
}

export interface LedgerChainCard {
  id: string;
  title: string;
  status: ChainStatus;
  summary: string;
  eventCount: number;
  impactLabel?: string;
  startedAt: string;
  resolvedAt?: string;
  stages: LedgerStage[];
}

export type LedgerEventType =
  | "model_item_created"
  | "model_item_updated"
  | "forecast_created"
  | "forecast_resolved_true"
  | "forecast_resolved_false"
  | "proposed_change_created"
  | "proposed_change_accepted"
  | "proposed_change_delegated"
  | "proposed_change_corrected"
  | "owner_assigned"
  | "risk_escalated"
  | "risk_downgraded"
  | "evidence_added"
  | "claim_contested"
  | "claim_corrected"
  | "reevaluation_scheduled";

export interface LedgerActor {
  type: "fyralis" | "user" | "system";
  name?: string;
  id?: string;
}

export interface LedgerEvent {
  id: string;
  timestamp: string;
  type: LedgerEventType;
  actor: LedgerActor;
  title: string;
  description?: string;
  object?: {
    id: string;
    type: string;
    label: string;
  };
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  sourceRefs?: { label: string; type: string; href?: string }[];
  confidencePct?: number;
  chainId: string;
}

export interface StateField {
  label: string;
  value: string;
  tone?: "critical" | "warn" | "neutral" | "positive" | "muted";
}

export interface ModelStateSnapshot {
  capturedAt: string;
  fields: StateField[];
}

export type EvidenceStrength = "weak" | "partial" | "moderate" | "strong" | "missing";

export interface EvidenceSnapshot {
  capturedAt: string;
  signalCount: number;
  sources: {
    label: string;
    strength: EvidenceStrength;
    count?: number;
  }[];
  missingContext?: string[];
}

export interface OutcomeImpact {
  outcomeLabel: string;
  impactRows: { label: string; value: string }[];
  notes?: string;
}

export interface ForecastAccuracyResult {
  forecastId: string;
  statement: string;
  initialConfidencePct: number;
  finalConfidencePct?: number;
  outcome: "true" | "false" | "partial";
  resolvedAt: string;
  calibrationImpactPp?: number;
  notes?: string;
}

export interface RelatedContext {
  todayItems: { label: string; proposedChangeId: string }[];
  modelLinks: { label: string; href: string }[];
  forecastLinks: { label: string; forecastId: string }[];
  ledgerLinks?: { label: string; eventId: string }[];
}

export interface LedgerChainDetail extends LedgerChainCard {
  events: LedgerEvent[];
  beforeState?: ModelStateSnapshot;
  afterState?: ModelStateSnapshot;
  evidenceAtTime?: EvidenceSnapshot;
  outcome?: OutcomeImpact;
  accuracy?: ForecastAccuracyResult;
  relatedContext?: RelatedContext;
  decisionReceipt?: DecisionReceipt;
}

export interface DecisionReceipt {
  acceptedBy: string;
  acceptedAt: string;
  proposedChangeLabel: string;
  changedRows: { label: string; before: string; after: string }[];
  fyralisActions: string[];
  outcomeLabel: string;
}

export interface BriefItem {
  id: string;
  label: string;
  severity?: "low" | "medium" | "high";
  href?: string;
}

export interface LedgerBriefData {
  statement: string;
  resolved: BriefItem[];
  stillOpen: BriefItem[];
  learned: BriefItem[];
}

export interface LedgerHeaderData {
  eventCount: number;
  chainCount: number;
  resolvedCount: number;
  forecastsClosedCount: number;
  correctionCount: number;
  dateRange: {
    label: string;
    start: string;
    end: string;
  };
}

export interface LedgerAccuracyByDomain {
  domain: string;
  accuracyPct: number;
  resolvedTrue: number;
  resolvedFalse: number;
  pending: number;
}

export interface LedgerAccuracyResolvedRow {
  chainId: string;
  forecast: string;
  initialConfidencePct: number;
  finalConfidencePct?: number;
  outcome: "true" | "false" | "partial";
  resolvedAt: string;
  calibrationImpactPp?: number;
}

export interface LedgerAccuracySummary {
  calibratedAccuracyPct: number;
  resolvedTrue: number;
  resolvedFalse: number;
  pending: number;
  byDomain: LedgerAccuracyByDomain[];
  resolved: LedgerAccuracyResolvedRow[];
  falsePositives: { label: string; note?: string }[];
  falseNegatives: { label: string; note?: string }[];
  missedContext: { label: string; note?: string }[];
}

export interface LedgerAuditEvent {
  id: string;
  chainId: string;
  chainTitle: string;
  timestamp: string;
  type: LedgerEventType;
  actor: LedgerActor;
  object: { id: string; label: string };
  before?: string;
  after?: string;
  source?: string;
}

export interface LedgerPagePayload {
  header: LedgerHeaderData;
  brief: LedgerBriefData;
  chains: LedgerChainCard[];
  chainDetails: Record<string, LedgerChainDetail>;
  accuracy: LedgerAccuracySummary;
  auditEvents: LedgerAuditEvent[];
  defaultSelectedChainId: string;
}
