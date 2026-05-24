// Ledger surface types — canonical event taxonomy + summary counters.
// Mirrors services/history/aggregator.py CANONICAL_LEDGER_TYPES and
// services/history/summary.py output shape.

export const LEDGER_EVENT_TYPES = [
  "action_taken",
  "model_update",
  "prediction_made",
  "prediction_resolved",
  "observation_ingested",
  "contestation",
] as const;

export type LedgerEventType = (typeof LEDGER_EVENT_TYPES)[number];

export type LedgerActorSource =
  | { kind: "person"; name: string; role?: string }
  | { kind: "system"; name: string }
  | { kind: "integration"; name: string };

export type LedgerEvidenceSource =
  | "support"
  | "email"
  | "crm"
  | "documents"
  | "slack"
  | "linear"
  | "github"
  | "calendar"
  | "finance"
  | "product";

export type LedgerEvidenceItem = {
  source: LedgerEvidenceSource;
  label: string;
  count: number;
};

export type LedgerRelatedNode = {
  id: string;
  label: string;
  href?: string;
};

export type LedgerTimelineStep = {
  id: string;
  timestamp: string; // ISO datetime
  text: string;
  event_type: LedgerEventType;
};

export type LedgerChange = {
  verb: "created" | "updated" | "archived" | "notified" | "scheduled";
  text: string;
};

export type LedgerEvent = {
  id: string;
  timestamp: string;            // ISO datetime
  type: LedgerEventType;
  title: string;
  summary: string;
  tags: string[];
  actor: LedgerActorSource;
  body?: string;
  target?: string;
  scope?: string[];
  related_nodes?: LedgerRelatedNode[];
  changes?: LedgerChange[];
  evidence?: LedgerEvidenceItem[];
  mini_timeline?: LedgerTimelineStep[];
  detail_type?: string;
};

export type WoWDeltaCounter = {
  value: number;
  delta_pct: number;
  delta_label: string;
};

export type SplitCounter = {
  value: number;
  split: string;
};

export type PpDeltaCounter = {
  value: number;        // 0..1
  delta_pp: number;     // 0..1
  delta_label: string;
};

export type LedgerSummary = {
  events: WoWDeltaCounter;
  model_updates: WoWDeltaCounter;
  predictions_made: SplitCounter;
  predictions_accuracy: PpDeltaCounter;
  actions_taken: WoWDeltaCounter;
  contestations: SplitCounter;
  range_days: number;
};
