// Shared trust scaffolding: context gaps, source coverage, evidence
// trace primitives. Used across Operating Threads, Decision Deltas,
// Forecasts, and Commitments per spec §6 trust requirements.

import type { EntityRef, ID } from "./common-types";

export type EvidenceQuality = "weak" | "medium" | "strong";

export type SourceStatus = "connected" | "limited" | "not_connected" | "stale";

export interface SourceCoverage {
  source: string;            // "support" | "crm" | "email" | "slack" | "product" | …
  status: SourceStatus;
  // Optional human label override ("Salesforce", "Zendesk").
  label?: string;
  // Time of the last successful sync, if known.
  lastSyncAt?: string;
}

export type ContextGapKind =
  | "missing_source"
  | "unconfirmed_severity"
  | "missing_human_context"
  | "limited_telemetry"
  | "owner_unconfirmed"
  | "stale_evidence"
  | "verbal_decision_unrecorded";

export interface ContextGap {
  id: ID;
  kind: ContextGapKind;
  text: string;              // "Product usage data is not connected."
  // Optional pointer to the resource needed to close the gap.
  suggestedAction?: "add_context" | "connect_source" | "ask_owner" | "mark_irrelevant";
  // Optional target entity (e.g. the owner being asked).
  target?: EntityRef;
}

export type EvidenceStepKind =
  | "observation"
  | "claim"
  | "pattern"
  | "belief"
  | "recommendation"
  | "forecast"
  | "commitment";

export type TrustTier =
  | "authoritative"
  | "attested"
  | "reputable"
  | "inferential"
  | "unvetted";

export interface EvidenceStep {
  id: ID;
  kind: EvidenceStepKind;
  title: string;             // "Beacon support ticket #7421"
  description?: string;      // "Sync failure during nightly batch."
  source?: string;           // "support" | "crm" | "slack" | …
  sourceLabel?: string;      // "Zendesk"
  occurredAt?: string;
  confidence?: number;
  trustTier?: TrustTier;
  // Restricted access? Show a redacted summary only.
  restricted?: boolean;
  refs?: EntityRef[];
  // Sublabel for the source-link badge ("View in Zendesk").
  externalUrl?: string | null;
}

export interface EvidenceTrace {
  id: ID;
  summary: string;            // "12 signals → 3 claims → 1 pattern → customer-risk update"
  steps: EvidenceStep[];
  // Mixed/contradictory evidence?
  contested?: boolean;
  contestationNote?: string;
}

export interface SemanticCompression {
  // Short text describing what is hidden:
  // "32 Nodes represented · 4 commitments · 3 risks · 2 decisions · 12 observations"
  oneLiner: string;
  // Optional pill counts for table-mode rendering.
  pills?: Array<{ label: string; count: number; tone?: string }>;
}
