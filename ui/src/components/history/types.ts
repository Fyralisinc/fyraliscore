// Driftwood — History page types.
// See DRIFTWOOD_HISTORY_SPEC.md Part 20 for canonical event/prediction/arc shapes.

export type HistoryLayerId = "chronicle" | "predictions" | "arcs";

export type EventProminence = "major" | "standard" | "minor";

export type EventType =
  | "decision-made"
  | "decision-ratified"
  | "decision-reversed"
  | "decision-superseded"
  | "decision-contested"
  | "prediction-made"
  | "prediction-resolved"
  | "pattern-emerged"
  | "pattern-dissolved"
  | "commitment-completed"
  | "commitment-slipped"
  | "commitment-blocked"
  | "customer-event"
  | "today-action"
  | "arc-opened"
  | "arc-resolved";

export type EntityKind =
  | "decision"
  | "commitment"
  | "prediction"
  | "person"
  | "customer"
  | "pattern"
  | "event"
  | "arc";

export type EventLink = {
  type: EntityKind;
  id: string;
  label?: string;
};

export type ArcPosition = "first" | "middle" | "last" | "single";

export type HistoryEvent = {
  id: string;
  timestamp: string; // ISO datetime
  type: EventType;
  prominence: EventProminence;
  title: string;
  descriptor: string;
  substrate_voice?: string;
  links?: EventLink[];
  arc?: string;
  arc_position?: ArcPosition;
  today_card_id?: string;
  structure_link?: string;
  // for aggregated routine events
  aggregated?: HistoryEvent[];
};

export type PredictionStatus = "pending" | "correct" | "wrong";
export type PredictionDomain =
  | "patterns"
  | "decisions"
  | "personnel"
  | "customer health"
  | "predictions";

export type Prediction = {
  id: string;
  made_on: string;
  domain: PredictionDomain;
  prediction_text: string;
  confidence: number; // 0..1
  reasoning_at_time?: string;
  status: PredictionStatus;
  resolved_on?: string;
  outcome_voice?: string;
  calibration_impact?: { domain: string; before: number; after: number };
  links?: EventLink[];
};

export type ArcStatus = "open" | "resolved";

export type Arc = {
  id: string;
  name: string;
  status: ArcStatus;
  started: string;
  ended?: string;
  narrative: string;
  events: string[]; // event ids in chronological order
};

export type CalibrationSummary = {
  overall: number;
  domains: { name: string; correct: number; total: number; score: number }[];
  trend?: {
    direction: "improving" | "declining" | "flat";
    from_score: number;
    from_date: string;
    to_score: number;
    to_date: string;
  };
};

export type HistoryRefKind =
  | "arc"
  | "decision"
  | "commitment"
  | "person"
  | "customer";

export type ShapeRef = { type: HistoryRefKind; id: string; text: string };
export type ShapeToken =
  | { kind: "text"; text: string }
  | { kind: "ref"; ref: ShapeRef };

export type HistoryFilters = {
  period: "7d" | "30d" | "90d" | "365d" | "all";
  types: Set<EventType>;
  significance: "all" | "major-standard" | "major";
  arcsOn: boolean;
  search: string;
  arcId: string | null; // active arc filter
};

export type LayerStripCounts = {
  chronicle: { events: number; period_label: string };
  predictions: { calibration: number; correct: number; total: number };
  arcs: { active: number; resolved: number };
};

export type PredictionFilter = "all" | "pending" | "correct" | "wrong";
