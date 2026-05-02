// Sample data for the History page. Anchored on 2026-04-29 (today).
// See DRIFTWOOD_HISTORY_SPEC.md Part 20 for the canonical samples.

import type {
  Arc,
  CalibrationSummary,
  HistoryEvent,
  LayerStripCounts,
  Prediction,
  ShapeToken,
} from "./types";

// Helpers — build ISO datetimes relative to a fixed anchor so screenshots
// and tests are deterministic.
const ANCHOR = new Date("2026-04-29T10:00:00Z");
const day = 24 * 60 * 60 * 1000;
function isoAt(daysOffset: number, hour = 10, minute = 0): string {
  const d = new Date(ANCHOR.getTime() + daysOffset * day);
  d.setUTCHours(hour, minute, 0, 0);
  return d.toISOString();
}

export const SAMPLE_EVENTS: HistoryEvent[] = [
  // — Today (Apr 29) —
  {
    id: "evt-pattern-audit",
    timestamp: isoAt(0, 9, 12),
    type: "pattern-emerged",
    prominence: "major",
    title: "AUDIT-LOGS PATTERN CROSSED ACTION THRESHOLD",
    descriptor:
      "Three customer asks and two internal mentions converged. Audit logs are now a structural priority candidate.",
    substrate_voice:
      "I've watched this signal accumulate for three weeks. Five touchpoints across two stakeholder classes is my historical threshold for action — calling this one explicitly.",
    links: [
      { type: "pattern", id: "audit-logs", label: "audit-logs pattern" },
      { type: "customer", id: "northwind", label: "Northwind" },
    ],
    arc: "audit-logs",
    arc_position: "last",
  },
  {
    id: "evt-c178-completed",
    timestamp: isoAt(0, 14, 5),
    type: "commitment-completed",
    prominence: "standard",
    title: "c-178 COMPLETED",
    descriptor: "SOC2 evidence collection wrapped. Sarah closed it.",
    links: [
      { type: "commitment", id: "c-178", label: "c-178" },
      { type: "person", id: "sarah", label: "Sarah" },
    ],
  },

  // — Yesterday (Apr 28) —
  {
    id: "evt-d5-ratify",
    timestamp: isoAt(-1, 13, 42),
    type: "decision-ratified",
    prominence: "major",
    title: "d-5 RATIFIED",
    descriptor:
      "You and Marcus ratified the Redis call. Three contradicting commitments redirected.",
    substrate_voice:
      "This resolves the drift cluster I'd been watching since April 2. My prediction (high confidence the decision would be reaffirmed) resolved correctly. This is the kind of pattern my model is well-calibrated on — decisions explicitly ratified after drift usually stay in force for many months afterward.",
    links: [
      { type: "decision", id: "d-5", label: "d-5" },
      { type: "commitment", id: "c-187", label: "c-187" },
      { type: "commitment", id: "c-203", label: "c-203" },
      { type: "commitment", id: "c-211", label: "c-211" },
      { type: "prediction", id: "p-37", label: "p-37" },
    ],
    arc: "d-5-drift",
    arc_position: "last",
    today_card_id: "today-d5-drift-2026-04-21",
    structure_link: "d-5",
  },
  {
    id: "evt-northwind-renewed",
    timestamp: isoAt(-1, 16, 20),
    type: "customer-event",
    prominence: "major",
    title: "NORTHWIND: RENEWED",
    descriptor:
      "Northwind signed a 24-month renewal. The March scare is fully resolved.",
    substrate_voice:
      "I was wrong about Northwind. I'd modeled them as a churn risk in February with 61% confidence. They renewed. The reason I was wrong: I overweighted recent communication tone and underweighted contract milestone proximity. I've adjusted.",
    links: [
      { type: "customer", id: "northwind", label: "Northwind" },
      { type: "prediction", id: "p-22", label: "p-22" },
    ],
    arc: "northwind",
    arc_position: "last",
  },

  // — This week (Apr 22-27) —
  {
    id: "evt-d5-surfaced-today",
    timestamp: isoAt(-7, 8, 0),
    type: "decision-contested",
    prominence: "standard",
    title: "d-5 SURFACED IN TODAY",
    descriptor:
      "I surfaced d-5 as a critical drift card. Three commitments were contradicting it quietly.",
    substrate_voice: "Currently in Today as a strategic card.",
    links: [{ type: "decision", id: "d-5" }],
    arc: "d-5-drift",
    arc_position: "middle",
    today_card_id: "today-d5-drift-2026-04-21",
  },
  {
    id: "evt-prediction-p44",
    timestamp: isoAt(-6, 11, 0),
    type: "prediction-made",
    prominence: "standard",
    title: "PREDICTION MADE",
    descriptor:
      "Audit logs will become structural priority. Confidence: 87%.",
    links: [{ type: "prediction", id: "p-44" }],
  },
  {
    id: "evt-routine-week",
    timestamp: isoAt(-5, 17, 0),
    type: "commitment-completed",
    prominence: "minor",
    title: "",
    descriptor: "12 commitments completed (routine)",
    aggregated: [
      {
        id: "evt-r1",
        timestamp: isoAt(-5, 9, 30),
        type: "commitment-completed",
        prominence: "minor",
        title: "",
        descriptor: "c-310 completed — quarterly NPS readout",
      },
      {
        id: "evt-r2",
        timestamp: isoAt(-5, 11, 0),
        type: "commitment-completed",
        prominence: "minor",
        title: "",
        descriptor: "c-405 completed — all-hands logistics",
      },
      {
        id: "evt-r3",
        timestamp: isoAt(-6, 14, 25),
        type: "commitment-completed",
        prominence: "minor",
        title: "",
        descriptor: "c-403 completed — travel policy update",
      },
    ],
  },
  {
    id: "evt-c305-blocked",
    timestamp: isoAt(-4, 9, 12),
    type: "commitment-blocked",
    prominence: "standard",
    title: "c-305 BLOCKED",
    descriptor:
      "Auth service split blocked on platform team capacity. Ravi flagged.",
    links: [{ type: "commitment", id: "c-305" }],
  },

  // — Last week (Apr 14-20) —
  {
    id: "evt-pattern-drift-cluster",
    timestamp: isoAt(-12, 10, 0),
    type: "pattern-emerged",
    prominence: "standard",
    title: "DRIFT-CLUSTER PATTERN EMERGED",
    descriptor:
      "Three commitments contradicting d-5 emerged a pattern threshold.",
    substrate_voice:
      "This is what I was watching for. Two contradictions could be coincidence; three is a pattern.",
    links: [{ type: "decision", id: "d-5" }],
    arc: "d-5-drift",
    arc_position: "middle",
  },
  {
    id: "evt-c211-contradiction",
    timestamp: isoAt(-13, 14, 12),
    type: "commitment-completed",
    prominence: "minor",
    title: "",
    descriptor: "c-211 scoping veered from d-5",
    arc: "d-5-drift",
    arc_position: "middle",
  },
  {
    id: "evt-c203-contradiction",
    timestamp: isoAt(-15, 11, 0),
    type: "commitment-completed",
    prominence: "minor",
    title: "",
    descriptor: "c-203 scoping veered from d-5",
    arc: "d-5-drift",
    arc_position: "middle",
  },

  // — 2-3 weeks ago —
  {
    id: "evt-arc-opened-northwind",
    timestamp: isoAt(-15, 9, 0),
    type: "arc-opened",
    prominence: "standard",
    title: "ARC OPENED: NORTHWIND",
    descriptor:
      "Tone shift in Northwind comms triggered an arc. Outreach scheduled.",
    arc: "northwind",
    arc_position: "first",
  },
  {
    id: "evt-c187-scope-departure",
    timestamp: isoAt(-32, 10, 30),
    type: "commitment-completed",
    prominence: "standard",
    title: "c-187 INITIAL SCOPE DEPARTURE",
    descriptor:
      "First commitment to scope around d-5 with non-Redis approaches.",
    links: [
      { type: "commitment", id: "c-187" },
      { type: "decision", id: "d-5" },
    ],
    arc: "d-5-drift",
    arc_position: "first",
  },

  // — Earlier (older buckets) —
  {
    id: "evt-q1-pricing-reset",
    timestamp: isoAt(-58, 9, 0),
    type: "decision-superseded",
    prominence: "major",
    title: "PRICING v3 SUPERSEDES v2",
    descriptor:
      "Tiered pricing replaces flat per-seat. Customer-facing rollout planned.",
    substrate_voice:
      "Pricing reset arc concluded. The contestation that started in February resolved here.",
    arc: "pricing-reset",
    arc_position: "last",
  },
  {
    id: "evt-q1-hiring-resolved",
    timestamp: isoAt(-65, 10, 0),
    type: "arc-resolved",
    prominence: "standard",
    title: "ARC RESOLVED: Q1 HIRING PUSH",
    descriptor: "Engineering director hired; Q1 hiring goals met.",
    arc: "q1-hiring",
    arc_position: "last",
  },
];

export const SAMPLE_PREDICTIONS: Prediction[] = [
  {
    id: "p-44",
    made_on: isoAt(-6, 11, 0),
    domain: "patterns",
    prediction_text: "Audit logs will become structural priority.",
    confidence: 0.87,
    reasoning_at_time:
      "Five touchpoints across customer asks and internal mentions in three weeks. Pattern threshold reached.",
    status: "pending",
  },
  {
    id: "p-43",
    made_on: isoAt(-9, 8, 0),
    domain: "personnel",
    prediction_text: "Sarah's cluster signals load issue.",
    confidence: 0.71,
    status: "pending",
    reasoning_at_time:
      "Three slipped commitments owned by Sarah in two weeks. Historically, this density correlates with capacity issues.",
  },
  {
    id: "p-37",
    made_on: isoAt(-7, 8, 0),
    domain: "decisions",
    prediction_text: "d-5 will be reaffirmed if surfaced.",
    confidence: 0.82,
    status: "correct",
    resolved_on: isoAt(-1, 13, 42),
    reasoning_at_time:
      "d-5 was contested by 3 commitments quietly, but the underlying logic of the decision (Redis as rate limiter) had not been challenged substantively. Both makers (you and Marcus) were still active. My model: when a decision is explicitly surfaced for ratification, and no substantive critique has emerged, the makers reaffirm 78% of the time historically.",
    outcome_voice:
      "You and Marcus ratified d-5 yesterday, redirecting the three contradicting commitments. Resolution time: 4 days from surfacing to ratification. Faster than my mean (6 days for similar resolutions).",
    calibration_impact: { domain: "decisions", before: 0.84, after: 0.86 },
    links: [
      { type: "decision", id: "d-5" },
      { type: "event", id: "evt-d5-ratify" },
      { type: "arc", id: "d-5-drift" },
    ],
  },
  {
    id: "p-22",
    made_on: isoAt(-90, 9, 0),
    domain: "customer health",
    prediction_text: "Northwind would churn by end of Q1.",
    confidence: 0.61,
    status: "wrong",
    resolved_on: isoAt(-1, 16, 20),
    reasoning_at_time:
      "Tone shift in three consecutive customer success calls. Decreased login activity. Historical churn correlation: 0.62 for this pattern.",
    outcome_voice:
      "Northwind renewed for 24 months yesterday. I overweighted recent communication tone and underweighted contract milestone proximity. I've adjusted my customer-health weights.",
    calibration_impact: {
      domain: "customer health",
      before: 0.81,
      after: 0.78,
    },
  },
  {
    id: "p-31",
    made_on: isoAt(-22, 9, 0),
    domain: "patterns",
    prediction_text: "Analytics weight would drop further.",
    confidence: 0.81,
    status: "correct",
    resolved_on: isoAt(-22 + 15, 9, 0),
    outcome_voice:
      "Analytics rebuild was deprioritized one week after the prediction. Calibrated correctly.",
  },
  {
    id: "p-19",
    made_on: isoAt(-44, 10, 0),
    domain: "decisions",
    prediction_text: "Pricing v3 would supersede v2 within 30 days.",
    confidence: 0.74,
    status: "correct",
    resolved_on: isoAt(-58, 9, 0),
  },
  {
    id: "p-17",
    made_on: isoAt(-50, 10, 0),
    domain: "personnel",
    prediction_text: "Eng director hire would close in Q1.",
    confidence: 0.55,
    status: "correct",
    resolved_on: isoAt(-65, 10, 0),
  },
  {
    id: "p-12",
    made_on: isoAt(-72, 10, 0),
    domain: "personnel",
    prediction_text: "Mid-year planning would slip a sprint.",
    confidence: 0.58,
    status: "wrong",
    resolved_on: isoAt(-30, 10, 0),
    outcome_voice:
      "I underestimated finance's bandwidth. The mid-year cycle held its dates.",
  },
  {
    id: "p-9",
    made_on: isoAt(-40, 10, 0),
    domain: "patterns",
    prediction_text: "Multi-tenancy concerns would surface from enterprise customers.",
    confidence: 0.78,
    status: "correct",
    resolved_on: isoAt(-25, 10, 0),
  },
  {
    id: "p-3",
    made_on: isoAt(-100, 9, 0),
    domain: "customer health",
    prediction_text: "Globex usage would dip during contract negotiation.",
    confidence: 0.66,
    status: "correct",
    resolved_on: isoAt(-80, 10, 0),
  },
];

export const SAMPLE_ARCS: Arc[] = [
  {
    id: "audit-logs",
    name: "Audit logs pattern",
    status: "open",
    started: "2026-04-08",
    narrative:
      "This arc opened when Northwind asked about per-user audit visibility. Two more customers asked similar questions in the next two weeks. Internally, security review prep triggered another mention. Today the pattern crossed my action threshold and I called it explicitly. Whether it becomes a structural priority depends on you.",
    events: ["evt-pattern-audit"],
  },
  {
    id: "northwind",
    name: "Northwind",
    status: "resolved",
    started: "2026-04-14",
    ended: "2026-04-28",
    narrative:
      "Northwind's tone shifted in early April — three customer success calls in a row had a different temperature. I flagged a churn-risk arc. Outreach was scheduled. The contract milestone hit on schedule and they renewed. My initial 61% churn confidence resolved wrong; I've adjusted.",
    events: ["evt-arc-opened-northwind", "evt-northwind-renewed"],
  },
  {
    id: "sarah-cluster",
    name: "Sarah cluster",
    status: "open",
    started: "2026-04-22",
    narrative:
      "Three commitments owned by Sarah have slipped in two weeks. The pattern is consistent with capacity load, not skill or motivation. I'm watching whether the next two weeks resolve or escalate.",
    events: [],
  },
  {
    id: "d-5-drift",
    name: "d-5 drift",
    status: "resolved",
    started: "2026-03-28",
    ended: "2026-04-28",
    narrative:
      "This arc started on March 28 when c-187 was scoped using non-Redis approaches, contradicting d-5. By April 14, two more commitments had emerged with similar contradictions. I surfaced this as a Today drift card on April 22. You and Marcus ratified the original Redis call yesterday, redirecting the contradicting commitments. The arc is closed; my prediction (high confidence the decision would be reaffirmed) resolved correctly.",
    events: [
      "evt-c187-scope-departure",
      "evt-c203-contradiction",
      "evt-c211-contradiction",
      "evt-pattern-drift-cluster",
      "evt-d5-surfaced-today",
      "evt-d5-ratify",
    ],
  },
  {
    id: "pricing-reset",
    name: "Pricing reset",
    status: "resolved",
    started: "2026-02-04",
    ended: "2026-03-02",
    narrative:
      "A month-long contestation around v2 pricing concluded with v3's tiered structure. Initial misalignment between sales and finance was the slowest part to resolve.",
    events: ["evt-q1-pricing-reset"],
  },
  {
    id: "q1-hiring",
    name: "Q1 hiring push",
    status: "resolved",
    started: "2026-01-04",
    ended: "2026-02-23",
    narrative:
      "Eight roles closed in eight weeks. The arc compressed once the engineering director was hired and could backfill independently.",
    events: ["evt-q1-hiring-resolved"],
  },
];

export const SAMPLE_CALIBRATION: CalibrationSummary = {
  overall: 0.81,
  domains: [
    { name: "patterns", correct: 10, total: 11, score: 0.91 },
    { name: "decisions", correct: 6, total: 7, score: 0.86 },
    { name: "customer health", correct: 7, total: 9, score: 0.78 },
    { name: "predictions", correct: 14, total: 19, score: 0.74 },
    { name: "personnel", correct: 3, total: 6, score: 0.5 },
  ],
  trend: {
    direction: "improving",
    from_score: 0.74,
    from_date: "2026-02-01",
    to_score: 0.81,
    to_date: "2026-04-29",
  },
};

export const SAMPLE_LAYER_COUNTS: LayerStripCounts = {
  chronicle: { events: SAMPLE_EVENTS.length, period_label: "this period" },
  predictions: { calibration: 0.81, correct: 11, total: 14 },
  arcs: {
    active: SAMPLE_ARCS.filter((a) => a.status === "open").length,
    resolved: SAMPLE_ARCS.filter((a) => a.status === "resolved").length,
  },
};

// Marked-up shape statement for the Chronicle narrative band.
export const CHRONICLE_PERIOD_STATEMENT: ShapeToken[] = [
  { kind: "text", text: "It's been a quarter of consolidation. The " },
  {
    kind: "ref",
    ref: {
      type: "arc",
      id: "pricing-reset",
      text: "enterprise-tier priority shift",
    },
  },
  {
    kind: "text",
    text: " in early April reorganized about a third of active commitments. ",
  },
  { kind: "ref", ref: { type: "arc", id: "northwind", text: "Northwind" } },
  { kind: "text", text: " stabilized after the March scare. " },
  { kind: "ref", ref: { type: "decision", id: "d-5", text: "d-5" } },
  {
    kind: "text",
    text: " was the most contested decision but resolved yesterday.",
  },
];

export const PREDICTIONS_NARRATIVE_STATEMENT: ShapeToken[] = [
  {
    kind: "text",
    text:
      "Calibration is improving slowly — 0.81 overall, up from 0.74 in February. My weakest domain remains personnel (3 of 6 historically). My strongest is patterns (10 of 11 in the last quarter).",
  },
];

export const ARCS_NARRATIVE_STATEMENT: ShapeToken[] = [
  {
    kind: "text",
    text:
      "Three arcs are still open: audit logs (1 event, started Apr 8), Northwind closed yesterday, and the Sarah cluster (3 events, started Apr 22). Two arcs resolved this quarter — d-5 drift and the pricing reset.",
  },
];
