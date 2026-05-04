// Today page contract — mirrors FYRALIS_TODAY_SPEC.md.
// Backend is the source of truth; treat as read-only structures.

export type Severity = "critical" | "strategic" | "high" | "med" | "low";

export type CardKind =
  | "decision_drift"
  | "strategic_feature"
  | "strategic_personnel"
  | "strategic_prioritization"
  | "vp_signal_conflict"
  | "customer_reciprocity"
  | "quick_approval"
  | "other";

export type TriageAction =
  | "act"
  | "hold"
  | "route"
  | "snooze"
  | "dismiss";

export type TagKind = "new" | "quiet";

export type Tag = {
  kind: TagKind;
  label: string;
};

export type StatTone = "default" | "warn" | "amber" | "green";

export type Stat = {
  label: string;
  value: string;
  tone?: StatTone;
};

export type EvidenceRow = {
  src: string;             // mono date+kind, e.g. "apr 12 · call"
  quote_html: string;      // serif italic
  attribution?: string;    // sans gray attribution
};

export type ConfidenceCell = {
  label: string;           // "On pattern"
  value_html: string;      // value + italic phrase
};

export type SuggestedPath = {
  id: string;
  label: string;           // short uppercase, e.g. "Reaffirm"
  body_html: string;       // action sentence with <strong>
};

// Driftwood revision: a substrate-emitted suggested probe shown above
// the in-card Ask field. Each chip has a stable id (used to look the
// probe up server-side) and human-readable text.
export type ProbeChip = {
  id: string;
  text: string;
};

// UX-3 expanded-card additions: structured "show your work" bands.
// Every field is optional so older backends keep parsing.
export type DiffPanel = {
  target_title: string;        // e.g. "Drive SSO requirements doc"
  target_kind: string;         // e.g. "commitment" | "goal" | "decision" | "resource"
  current_state?: string;      // e.g. "proposed"
  to_state?: string;           // e.g. "active" — null for archive/create/update
  operation: string;           // operation name from proposed_change
  owner_name?: string;         // e.g. "Sarah Chen"
  created_at?: string;         // ISO timestamp
  days_idle?: number;          // optional integer derived from updated_at
  acceptance?: string;         // commitment acceptance criteria / decision summary / etc
};

export type SignalRow = {
  date_label: string;          // mono "apr 12"
  source: string;              // mono "linear" / "vercel" / "slack"
  attribution?: string;        // sans gray "cto · q1 review"
  quote: string;               // serif italic
  observation_id?: string;     // optional; lets the UI route to a probe
};

export type ReasoningItem = {
  natural: string;             // "3 design partners blocked on SSO"
  confidence: number;          // 0..1
  model_id?: string;           // optional UUID for navigation
};
export type ReasoningGroup = {
  kind: string;                // proposition_kind
  label: string;               // "STATE" | "PATTERN" | "PREDICTION" etc
  items: ReasoningItem[];
};

export type Calibration = {
  kind_label: string;          // "SSO-style" / "decision-drift" / "personnel"
  hit_rate?: number;           // 0..1, omitted when n_prior < 3
  n_prior: number;             // total prior recommendations of this kind
  window_days: number;         // e.g. 90
};

export type Falsifier = {
  text: string;                // "the cluster fades to two or fewer signals"
  watchable: boolean;          // can the user subscribe to a watcher?
  predicate?: string;          // server-side predicate id (when watchable)
};

export type DetailPanel = {
  // Legacy fields — kept on the wire so older clients don't break.
  reasoning_html?: string;
  evidence?: EvidenceRow[];
  evidence_label?: string;
  confidence?: ConfidenceCell[];
  paths?: SuggestedPath[];
  show_ask?: boolean;
  // Driftwood revision (probe/ask conversation):
  probe_chips?: ProbeChip[];
  conversation_id?: string;
  // UX-3 expanded-card bands:
  diff?: DiffPanel;
  signals?: SignalRow[];
  reasoning?: ReasoningGroup[];
  calibration?: Calibration;
  falsifier?: Falsifier;
  // True when the user has an active watch on this recommendation's
  // falsifier predicate (so the UI can show "Watching" instead of
  // "Watch for revision").
  is_watched?: boolean;
};

export type WatchRequest = { recommendation_id: string };
export type WatchResponse = { ok: boolean; watch_id: string; recommendation_id: string };

// One probe → response exchange in a card-scoped conversation.
// Mirrors the row shape of card_exchanges (migration 0024).
export type ProbeFollowUp = ProbeChip;

export type ProbeKind = "phrase" | "chip" | "ask";

export type CardExchange = {
  id: string;
  conversation_id: string;
  probe_kind: ProbeKind;
  probe_id?: string;
  probe_action: string;       // e.g. "You clicked"
  probe_text: string;         // e.g. '"three customers"'
  response_html: string;      // may include <probe> markup
  follow_ups: ProbeFollowUp[];
  created_at: string;
};

export type CardConversation = {
  conversation_id: string;
  card_id: string;
  exchanges: CardExchange[];
  probed_phrase_ids: string[];  // for marking already-probed phrases
  used_chip_ids: string[];      // suppressed from the main probe row
  last_probed_at?: string;
  archived: boolean;
};

export type ProbeRequest =
  | { kind: "phrase"; probe_id: string }
  | { kind: "chip"; probe_id: string }
  | { kind: "ask"; query: string };

export type ProbeResponse = {
  exchange: CardExchange;
};

export type CardCategory = "operational" | "strategic";

export type RecCard = {
  id: string;
  severity: Severity;
  category: CardCategory;
  kind_label: string;            // e.g. "Decision drift · d-5"
  meta?: string;                 // e.g. "15 min · only you can ratify"
  tag?: Tag;
  headline_html: string;         // serif sentence with <em> refs
  supporting_html?: string;      // sans line with single <em> emphasis
  stats?: Stat[];                // legacy — UI no longer renders, kept on wire
  // Driftwood UX-2 additions: structured proposal / honest epistemics /
  // specialized approval. Optional so older backends keep working.
  proposed_change_text?: string; // e.g. "Transition c-5 → closed"
  epistemic_line?: string;       // e.g. "82% — would revise if Marcus reaffirms in writing."
  approve_label?: string;        // specialized: "Close c-5", "Schedule 1:1", "Add to Q2"
  expand_cta?: string;           // "Ask why" | "Inspect" | "Open"
  actions: TriageAction[];       // primary first; UI collapses into Approve / Discuss / Not now
  detail?: DetailPanel;
};

export type SignalTone = "default" | "accent" | "warn" | "amber";

export type SignalMetric = {
  id: string;
  label: string;                 // e.g. "ARR"
  value: string;                 // serif 26px
  value_unit?: string;           // sans-serif suffix, e.g. "months"
  trend_html?: string;           // sub-line with optional emphasis
  tone?: SignalTone;             // colors trend
  unavailable?: boolean;         // shows em-dash
};

export type VitalRow = {
  id: string;
  label: string;
  value: string;
  tone?: SignalTone;
};

export type NavItem = {
  id: string;
  label: string;
  shortcut?: string;             // e.g. "⌘7"
  badge?: string;                // count or "soon"
  badge_warn?: boolean;
  active?: boolean;
  disabled?: boolean;
};

export type NavSection = {
  id: string;
  label: string;
  items: NavItem[];
};

export type RoutedRow = {
  recipient: string;             // "Marcus" | "Watching, no owner"
  count: number;
  items: string;                 // comma-joined item summaries
};

export type RoutedCoda = {
  total: number;
  rows: RoutedRow[];
};

export type StateLineTone =
  | "tense" | "quiet" | "productive" | "unsettled"
  | "clear" | "loaded" | "urgent" | "steady";

export type PageHeader = {
  date_label: string;            // "Saturday, April 25."
  state_tone: StateLineTone;
  state_text: string;            // first-person sentence(s)
};

export type JustUpdated = {
  text_html: string;             // "Just now: …"
};

export type CalibrationAlert = {
  text: string;                   // shown when global cal < 0.6
};

export type AskSuggestion = string;

export type TodayResponse = {
  brand: { name: string; mark: string; pulse_day: number };
  page: PageHeader;
  signal_strip: SignalMetric[];          // exactly 4
  vitals: VitalRow[];
  nav: NavSection[];
  cards: RecCard[];
  cleared_today: number;
  just_updated?: JustUpdated;
  routed_coda?: RoutedCoda;
  ask_suggestions: AskSuggestion[];
  calibration_alert?: CalibrationAlert;
  empty_state?: { headline: string; body: string };
};

// Triage write
export type TriageRequest = {
  action: TriageAction;
  reason?: string;               // required for dismiss
  routed_to?: string;            // for route
  snooze_until?: string;         // ISO for snooze
  notes?: string;                // for act
  selected_path_id?: string;     // for act
};

export type TriageResponse = {
  ok: boolean;
  recommendation_id: string;
  action: TriageAction;
};

export type StreamMessageToday =
  | { type: "today_updated"; today: TodayResponse }
  | { type: "card_triaged"; card_id: string; action: TriageAction }
  | { type: "vitals_updated"; vitals: VitalRow[] }
  | { type: "signal_strip_updated"; signal_strip: SignalMetric[] };
