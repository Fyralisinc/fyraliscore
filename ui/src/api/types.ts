// Types mirror the shapes in CONTRACTS.md §1.1–1.4.
// Backend is the source of truth; treat these as read-only structures.

export type QueryTag = "urgent" | "relevant" | "2min" | "evergreen";

export type QueryChip = {
  id: string;
  icon: string;
  label: string;
  tag?: QueryTag;
  hot: boolean;
};

export type CardKind = "observation" | "decision" | "question";
export type CardTagColor = "hot" | "warm" | "soft";

export type CardVerb = {
  id: string;
  label: string;
  primary: boolean;
  query_template: string;
};

export type CardEvidence = {
  label: string;
  body_html: string;
};

export type CardExpanded = {
  reasoning_html: string;
  evidence: CardEvidence[];
  verbs: CardVerb[];
};

export type Card = {
  id: string;
  kind: CardKind;
  tag_color: CardTagColor;
  tag_label: string;
  meta: string;
  body_html: string;
  expanded: CardExpanded;
  cached_at: string;
};

export type Greeting = {
  meta: {
    date_iso: string;
    recomputed_at: string;
    signals_watched_count: number;
  };
  body_html: string;
  cached_at: string;
  staleness_seconds: number;
};

export type QueryGrid = {
  queries: QueryChip[];
  cached_at: string;
};

export type CloseLine = {
  body: string;
  metadata: {
    signal_count: number;
    external_moves: number;
    calibration_pct: number;
  };
};

export type Status = {
  substrate_alive: boolean;
  calibration_pct: number;
  needs_you_count: number;
};

export type HomeResponse = {
  greeting: Greeting;
  query_grid: QueryGrid;
  cards: Card[];
  close_line: CloseLine;
  status: Status;
};

// POST /view/ceo/ask
export type TurnVerbId = "followup" | "save" | "done";

export type TurnVerb = {
  id: TurnVerbId;
  label: string;
};

export type AskRequest = {
  query: string;
  context_card_id?: string;
};

export type AskResponse = {
  turn_id: string;
  query_echo: string;
  response_html: string;
  verbs: TurnVerb[];
  computed_at: string;
  latency_ms: number;
};

// POST /view/ceo/turn-action
export type TurnActionRequest = {
  turn_id: string;
  action: "save" | "done" | "followup";
  follow_up_query?: string;
};

export type TurnActionResponse = {
  ok: boolean;
  new_turn_id?: string;
};

// WebSocket messages
export type StreamMessage =
  | { type: "greeting_updated"; greeting: Greeting }
  | { type: "cards_updated"; cards: Card[] }
  | { type: "query_grid_updated"; query_grid: QueryGrid }
  | { type: "status_updated"; status: Status };
