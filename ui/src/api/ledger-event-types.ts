// Unified Ledger Event — spec §9. Durable record of something Fyralis
// observed, inferred, predicted, changed, contested, resolved, or
// caused through user action.

import type { EntityRef, ID } from "./common-types";

export type LedgerEventKind =
  | "observation_ingested"
  | "model_updated"
  | "thread_created"
  | "thread_status_changed"
  | "thread_split"
  | "thread_merged"
  | "decision_delta_proposed"
  | "decision_delta_accepted"
  | "decision_delta_delegated"
  | "decision_delta_contested"
  | "commitment_created"
  | "commitment_blocked"
  | "forecast_created"
  | "forecast_confidence_changed"
  | "forecast_resolved"
  | "user_context_added"
  | "node_archived";

export type LedgerEventCategory =
  | "observation"
  | "model_update"
  | "decision_action"
  | "contestation"
  | "critical_risk"
  | "forecast"
  | "commitment_state";

export interface SpecLedgerEvent {
  id: ID;
  occurredAt: string;
  kind: LedgerEventKind;
  category: LedgerEventCategory;
  // One sentence summary ("Diana escalated customer risk to VP Engineering.").
  summary: string;
  // Optional longer body shown in inspector.
  body?: string;
  actor?: EntityRef;
  // Plain-text before -> after when applicable.
  before?: string;
  after?: string;
  // Cross-links.
  relatedRefs: EntityRef[];
  affectedThreadId?: ID;
  affectedDeltaId?: ID;
  affectedForecastId?: ID;
  affectedCommitmentId?: ID;
  evidenceTraceId?: ID;
  // Actions taken at this event ("Created escalation Node · scheduled re-evaluation in 48h").
  actionsTaken?: string[];
  outcome?: string;
  // Forecast events only.
  calibrationImpact?: number;
  // Severity for color-coding.
  severity?: "info" | "trust" | "authority" | "review" | "critical" | "forecast";
}

export interface ListLedgerEventsParams {
  rangeStart?: string;
  rangeEnd?: string;
  kinds?: LedgerEventKind[];
  categories?: LedgerEventCategory[];
  actorId?: ID;
  threadId?: ID;
  highImpactOnly?: boolean;
  search?: string;
  limit?: number;
}

export interface ListLedgerEventsResponse {
  events: SpecLedgerEvent[];
  total: number;
  rangeLabel: string;     // "May 12 – May 16"
}
