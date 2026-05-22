// Operating Thread — spec §4. Persistent, human-readable storyline
// through the company model. A compressed reading of related goals,
// commitments, decisions, risks, customers, owners, predictions, and
// evidence. Primarily lives on the Model page.

import type { ContextGap, EvidenceQuality, SourceCoverage } from "./trust-types";
import type { EntityRef, ID } from "./common-types";

export type OperatingThreadStatus =
  | "healthy"
  | "watch"
  | "under_pressure"
  | "needs_review"
  | "critical"
  | "stale"
  | "contested"
  | "monitoring"
  | "resolved";

export type ModelLens =
  | "company"
  | "commitments"
  | "decisions"
  | "customers"
  | "teams"
  | "risks"
  | "owners"
  | "predictions";

// Causal ribbon column — label/value pair shown left-to-right under the
// thread title. Default Company lens uses Intent / Promise / Friction /
// Exposure / Next.
export interface CausalRibbonCell {
  label: string;
  value: string;
  refs?: EntityRef[];
  tone?: "neutral" | "trust" | "review" | "authority" | "critical" | "forecast";
}

export interface ThreadAccountability {
  owner?: EntityRef;
  contributors: EntityRef[];
  waitingOn: EntityRef[];
  blocking: EntityRef[];
  loadSignal?: string;
}

export interface ThreadSemanticMass {
  representedNodes: number;
  changedToday: number;
  contested: number;
  blockedCommitments: number;
  affectedCustomers: number;
  arrAtRisk?: number;
  opportunityValue?: number;
  // Free-form rollup: { commitments: 4, risks: 3, predictions: 2, observations: 12 }
  typeCounts: Record<string, number>;
}

export interface ThreadTrust {
  confidence?: number;            // 0..1
  confidencePrevious?: number;
  evidenceQuality: EvidenceQuality;
  sourceCoverage: SourceCoverage[];
  contextGaps: ContextGap[];
}

export interface OperatingThread {
  id: ID;
  lens: ModelLens;
  title: string;
  status: OperatingThreadStatus;
  // One sentence: subject, condition/change, consequence.
  currentReading: string;
  // Optional richer "why this matters" — shown in inspector.
  whyThisMatters?: string;
  anchorSubjects: EntityRef[];
  causalRibbon: CausalRibbonCell[];
  semanticMass: ThreadSemanticMass;
  trust: ThreadTrust;
  accountability: ThreadAccountability;
  // Cross-links.
  relatedDecisionDeltaIds: ID[];
  relatedForecastIds: ID[];
  relatedCommitmentIds: ID[];
  // "Hidden structure" — short bullets shown in the inspector explaining
  // what was compressed (e.g. "3 stale CRM tickets · 1 unowned escalation").
  hiddenStructure?: string[];
  // Recent change strokes for the inspector "What changed" block.
  whatChanged?: Array<{ at: string; note: string }>;
  lastUpdatedAt: string;
}

export interface OperatingThreadGroup {
  // "Needs attention" | "Stable / watching" | lens-specific groups.
  id: string;
  label: string;
  threads: OperatingThread[];
}

export interface ListThreadsParams {
  lens?: ModelLens;
  status?: OperatingThreadStatus[];
  search?: string;
}

export interface ListThreadsResponse {
  groups: OperatingThreadGroup[];
  total: number;
  compressionSentence: string;       // "Fyralis has condensed 148 active Nodes into 7 operating threads."
  statusCounters: {
    changedToday: number;
    contested: number;
    blockedCommitments: number;
    arrAtRisk?: number;
  };
  lastUpdatedAt: string;
}

// Recent model changes — used in the Recent Changes strip.
export interface RecentModelChange {
  id: ID;
  occurredAt: string;
  summary: string;       // "Customer-risk state updated: Watch → Critical"
  threadId?: ID;
  refs?: EntityRef[];
  kind: "state_change" | "forecast_created" | "commitment_flagged" | "delta_proposed" | "delta_accepted" | "contestation";
}
