// Wire contract for the Today page v2 (Briefing + Focused Review).
// Mirrors services/gateway/today_routes.py — the gateway is the source
// of truth. Keep these types read-only on the wire boundary; UI-only
// enrichments live in dedicated `*View` helper types in the components.

// --- Status + category enums -----------------------------------------

export type DeltaStatus =
  | "needs_authority"
  | "delegatable"
  | "monitoring"
  | "contested"
  | "accepted"
  | "delegated"
  | "correction_submitted"
  | "archived"
  | "failed_apply";

export type ModelCategoryKey =
  | "goals_priorities"
  | "commitments"
  | "decisions"
  | "risks_constraints"
  | "customers_revenue"
  | "people_teams"
  | "systems_capacity"
  | "finance_capital";

export type DeltaAction =
  | "accept"
  | "delegate"
  | "review_evidence"
  | "report_correction"
  | "mark_known"
  | "snooze"
  | "open_model";

export type ImpactOperationType =
  | "create_node"
  | "update_node"
  | "archive_node"
  | "notify_actor"
  | "schedule_re_evaluation"
  | "link_nodes"
  | "create_ledger_event";

export type ValueSeverity = "neutral" | "watch" | "critical" | "positive";

export type EvidenceQuality = "weak" | "partial" | "medium" | "strong";

// --- Sub-shapes ------------------------------------------------------

export interface DeltaField {
  key: string;
  label: string;
  value: string;
  valueType?: "status" | "owner" | "date" | "duration" | "money" | "text";
  severity?: ValueSeverity;
}

export interface DeltaMetric {
  label: string;
  value: string | number;
  unit?: string;
  icon?: string;
  severity?: "low" | "medium" | "high" | "critical";
}

export interface EvidenceGroup {
  id: string;
  label: string;
  sourceType: string;
  count: number;
  quality: EvidenceQuality;
  strengthScore?: number;
}

export interface EvidenceSummary {
  totalSignals: number;
  quality: EvidenceQuality;
  groups: EvidenceGroup[];
}

export interface EvidenceItem {
  id: string;
  source: string;
  sourceLabel?: string;
  title: string;
  occurredAt: string;
  trustTier?: string | null;
  quality?: EvidenceQuality;
  excerpt?: string | null;
  weight?: number | null;
  ordinal?: number;
}

export interface MissingContextItem {
  id: string;
  text: string;
  severity: "low" | "medium" | "high";
  relatedSource?: string | null;
}

export interface ImpactItem {
  id: string;
  text: string;
  operationType: ImpactOperationType;
  severity?: ValueSeverity;
}

export interface RelatedModelLink {
  category: ModelCategoryKey;
  label: string;
  href: string;
}

export interface ApplyPreview {
  nodeOpsCount: number;
  notificationsCount: number;
  reEvaluationScheduledAt?: string | null;
  ledgerEventWillBeCreated: boolean;
}

export interface MoneyAmount {
  amount: number;
  currency: string;
  formatted: string;
}

// Optional in-flight annotations stored on the delta's impact JSONB.
// Surfaced through the wire so the focused-review timeline can render
// them without a second fetch.
export interface DeltaAnnotations {
  delegation?: {
    owner_id: string;
    by?: string;
    message?: string | null;
    due_at?: string | null;
    notify_now?: boolean;
    monitor_confirmation?: boolean;
    at: string;
  };
  contest?: { by: string; reason: string; at: string };
  correction?: {
    type: string;
    explanation: string;
    supporting_link?: string | null;
    apply_to_related?: boolean;
    by: string;
    at: string;
  };
  context_notes?: Array<{ by: string; note: string; at: string }>;
}

// --- Core delta wire DTO ---------------------------------------------

export interface DecisionDelta {
  id: string;
  title: string;
  userFacingType: "proposed_change";
  internalType: "decision_delta";
  status: DeltaStatus;
  priorityRank: number;
  sourceCategory: ModelCategoryKey;
  relatedCategories: ModelCategoryKey[];
  proposedBy: "fyralis" | "user" | "system";
  createdAt: string;
  updatedAt: string;
  currentState: DeltaField[];
  proposedState: DeltaField[];
  summaryLine: string;
  whyThisMatters: string;
  keyMetrics: DeltaMetric[];
  evidenceSummary: EvidenceSummary;
  missingContext: MissingContextItem[];
  impactIfAccepted: ImpactItem[];
  relatedModelLinks: RelatedModelLink[];
  availableActions: DeltaAction[];
  applyPreview: ApplyPreview;
  // Routing metadata
  targetNodeKind?: string | null;
  targetNodeId?: string | null;
  confidence?: number | null;
  resolutionTargetAt?: string | null;
  // In-flight notes (delegation/contest/correction)
  annotations?: DeltaAnnotations;
  // Detail endpoint only.
  evidence?: EvidenceItem[];
}

// --- Page-level summary ----------------------------------------------

export interface TodaySummary {
  signalsProcessed: number;
  signalsAbsorbed: number;
  modelUpdates: number;
  needJudgment: number;
  requiresAuthority: number;
  delegatable: number;
  monitoring: number;
  contested: number;
  exposure: MoneyAmount | null;
}

export interface HandledWithoutYouSummary {
  signalsAbsorbed: number;
  modelUpdatesApplied: number;
  itemsUnderMonitoring: number;
  delegatedChanges: number;
  contestedChanges: number;
  reassuranceCopy: string;
}

export interface ViewerContext {
  userId: string;
  name: string;
  role: string;
  tenantId: string;
}

export interface TodayPageData {
  viewer: ViewerContext;
  lastReviewAt: string;
  generatedAt: string;
  summary: TodaySummary;
  primaryJudgment: DecisionDelta | null;
  otherChanges: DecisionDelta[];
  handledWithoutYou: HandledWithoutYouSummary;
}

// --- Request bodies --------------------------------------------------

export interface ApplyResult {
  status: "applied" | "failed" | "requires_refresh";
  resultMessage: string;
  updatedDelta?: DecisionDelta;
  nextDeltaId?: string | null;
  ledgerEventId?: string | null;
  monitoringItemId?: string | null;
  triggered?: Record<string, unknown>;
}

export interface DelegateBody {
  delegateToActorId: string;
  dueAt?: string;
  message?: string;
  notifyNow: boolean;
  monitorConfirmation: boolean;
}

export interface DelegateResult {
  status: "delegated" | "requires_refresh";
  resultMessage: string;
  updatedDelta?: DecisionDelta;
}

export type CorrectionType =
  | "wrong_conclusion"
  | "wrong_owner"
  | "already_handled"
  | "missing_context"
  | "not_important"
  | "misleading_source"
  | "other";

export interface CorrectionBody {
  correctionType: CorrectionType;
  explanation: string;
  supportingLink?: string;
  applyToRelatedItems?: boolean;
}

export interface CorrectionResult {
  status: "correction_submitted" | "requires_refresh";
  resultMessage: string;
  updatedDelta?: DecisionDelta;
}

export interface EvidenceResponse {
  deltaId: string;
  totalSignals: number;
  evidenceGroups: EvidenceGroup[];
  items: EvidenceItem[];
}
