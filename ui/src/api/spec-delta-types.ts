// Spec-aligned Decision Delta view model. Wraps the existing backend
// DecisionDelta wire type with a richer per-spec view: trust-stage
// naming, current/proposed state strings, consequence preview, evidence
// trace + context gaps. The wire type lives in decision-deltas-types.ts.

import type { ContextGap, EvidenceTrace, SourceCoverage } from "./trust-types";
import type { EntityRef, ID } from "./common-types";

export type SpecDeltaStatus =
  | "candidate"
  | "proposed"
  | "under_review"
  | "accepted"
  | "delegated"
  | "contested"
  | "snoozed"
  | "applied"
  | "monitoring"
  | "resolved"
  | "archived";

// User-facing type label adapts to tenant trust stage (spec §5.5).
export type DeltaFacingType =
  | "Proposed Change"
  | "Recommended Change"
  | "Decision Delta";

export type DeltaQueueSection =
  | "requires_authority"
  | "delegatable"
  | "needs_context"
  | "watching";

export interface ConsequenceOp {
  operation: "create" | "update" | "archive" | "notify" | "reevaluate";
  label: string;                    // "Notify VP Engineering"
  target?: EntityRef;
}

export interface SpecDelta {
  id: ID;
  userFacingType: DeltaFacingType;
  status: SpecDeltaStatus;
  queueSection: DeltaQueueSection;
  // One-sentence proposal.
  proposal: string;
  // Plain-text current / proposed state ("Watch" / "Critical").
  currentState: string;
  proposedState: string;
  // Source operating thread.
  sourceThreadId?: ID;
  sourceThreadTitle?: string;
  category?: string;                // "Customer Risk", "Decision", …
  // Why this surfaced — top 3-5 evidence points (short text).
  whySurfaced: string[];
  // Quantitative impact strip ("$2.04M ARR · 3 customers · 12 signals").
  impactChips: string[];
  arrAtRisk?: number;
  affectedCustomers?: EntityRef[];
  // Trust.
  confidence?: number;              // 0..1
  confidenceBasis?: string;         // "limited by missing product usage data"
  evidenceTrace: EvidenceTrace;
  sourceCoverage: SourceCoverage[];
  contextGaps: ContextGap[];
  falsificationCondition?: string;
  // Consequence preview — what happens on accept.
  consequencePreview: ConsequenceOp[];
  // Severity / urgency.
  severity: "critical" | "high" | "medium" | "low";
  staleLabel?: string;              // "Sustained 3 days"
  // Lifecycle metadata.
  createdAt: string;
  updatedAt: string;
  // Routing metadata.
  delegatedTo?: EntityRef;
  delegationNote?: string;
  contestReason?: string;
  contextNotes?: Array<{ by: EntityRef; note: string; at: string }>;
}

export interface ListSpecDeltasResponse {
  deltas: SpecDelta[];
  sinceLastReview: {
    proposedChanges: number;
    delegatable: number;
    contested: number;
    modelUpdates: number;
    signalsAbsorbed: number;
    arrExposed?: number;
  };
  lastUpdatedAt: string;
}
