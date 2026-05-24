// Mock fixtures for the Forecasts page (spec v1.0). Used by the Vite
// mock-server plugin when `USE_MOCK=1` so the UI renders end-to-end
// without a backend.
//
// Shapes mirror services/forecasts/page.py exactly.

import type {
  AccuracyResponse,
  ForecastAskRequest,
  ForecastAskResponse,
  ForecastDetail,
  ForecastsPagePayload,
  PatternCard,
} from "./forecasts-types";

const NOW = () => new Date();
const isoIn = (days: number) =>
  new Date(NOW().getTime() + days * 86_400_000).toISOString();
const isoAgo = (days: number) => isoIn(-days);

const BEACON_ID = "00000000-0000-0000-0000-000000000101";
const ENG_ID = "00000000-0000-0000-0000-000000000102";
const Q3_ID = "00000000-0000-0000-0000-000000000103";
const ICP_ID = "00000000-0000-0000-0000-000000000104";
const PRICING_ID = "00000000-0000-0000-0000-000000000105";
const PARTNER_ID = "00000000-0000-0000-0000-000000000106";

const beaconDetail: ForecastDetail = {
  id: BEACON_ID,
  statement: "Beacon renewal risk likely to increase",
  domain: "customers_revenue",
  category: "customer_risk",
  severity: "critical",
  confidence: 0.78,
  confidence_delta: 0.13,
  confidence_series: {
    points: [
      { timestamp: isoAgo(6), confidence: 0.65 },
      { timestamp: isoAgo(5), confidence: 0.68 },
      { timestamp: isoAgo(4), confidence: 0.7 },
      { timestamp: isoAgo(3), confidence: 0.72 },
      { timestamp: isoAgo(2), confidence: 0.74 },
      { timestamp: isoAgo(1), confidence: 0.76 },
      { timestamp: isoAgo(0), confidence: 0.78 },
    ],
    current: 0.78,
    delta_window_days: 7,
    delta: 0.13,
  },
  resolution_date: isoIn(2),
  why_this_forecast:
    "Renewal risk increases if Salesforce sync issues continue. Anchor accounts are raising reliability concerns.",
  driving_patterns: [
    {
      id: "anchor-reliability",
      title: "Anchor accounts reporting reliability issues",
      status: "strengthening",
      supported_forecast_count: 3,
      source_coverage: ["Support", "CRM", "Email"],
    },
    {
      id: "champion-fade",
      title: "Champion response gaps on escalations",
      status: "strengthening",
      supported_forecast_count: 2,
      source_coverage: ["Slack", "Email"],
    },
  ],
  leading_indicators: [
    {
      id: "ind-sync",
      label: "Sync failures",
      value_label: "↑ 42%",
      direction: "up",
      severity: "negative",
      timeframe: "Last 7 days",
      sparkline: [0.4, 0.45, 0.5, 0.6, 0.66, 0.74, 0.82],
    },
    {
      id: "ind-sent",
      label: "Renewal sentiment",
      value_label: "Negative",
      direction: "down",
      severity: "negative",
      timeframe: "Last 14 days",
      sparkline: [0.6, 0.55, 0.5, 0.4, 0.35, 0.3, 0.28],
    },
    {
      id: "ind-tickets",
      label: "Support tickets",
      value_label: "↑ 33%",
      direction: "up",
      severity: "negative",
      timeframe: "Last 7 days",
      sparkline: [0.3, 0.32, 0.42, 0.5, 0.55, 0.6, 0.66],
    },
    {
      id: "ind-resp",
      label: "Owner response time",
      value_label: "2.4×",
      direction: "up",
      severity: "negative",
      timeframe: "Last 7 days",
      sparkline: [0.2, 0.3, 0.4, 0.55, 0.65, 0.7, 0.78],
    },
  ],
  would_change_if: [
    { id: "f-1", text: "No new sync failures for 7 business days.", observable: true, timeframe: "7 days", status: "unmet" },
    { id: "f-2", text: "Account owner confirms reporting restored.", observable: true, timeframe: "3 days", status: "unmet" },
    { id: "f-3", text: "Renewal sentiment returns neutral or positive.", observable: true, timeframe: "7 days", status: "unmet" },
  ],
  intervention_levers: [
    {
      id: "lv-1",
      label: "Escalate sync owner",
      expected_effect: "Owner-gap risk decreases",
      action_type: "create_proposed_change",
    },
    {
      id: "lv-2",
      label: "Increase account touchpoints",
      expected_effect: "Renewal sentiment may shift positive",
      action_type: "create_proposed_change",
    },
    {
      id: "lv-3",
      label: "Open in Model",
      expected_effect: "Trace anchor-account context",
      action_type: "open_model",
    },
  ],
  related_context: {
    model_links: [
      { label: "Customers & Revenue → Beacon", href: "/model?focus=customers_revenue" },
      { label: "Systems & Capacity → Salesforce Sync", href: "/model?focus=systems_capacity" },
    ],
    today_links: [{ label: "Escalate customer risk", proposed_change_id: "dd-beacon-1" }],
    ledger_links: [{ label: "3 similar risks resolved" }],
  },
  evidence_summary: {
    signal_count: 4,
    quality: "strong",
    sources: [
      { label: "salesforce", strength: "strong", count: 2 },
      { label: "slack", strength: "moderate", count: 1 },
      { label: "github", strength: "moderate", count: 1 },
    ],
  },
  target_label: "Beacon",
  impact: { label: "$1.2M ARR", value: 1_200_000, unit: "ARR" },
};

const engDetail: ForecastDetail = {
  id: ENG_ID,
  statement: "Engineering capacity will exceed 90%",
  domain: "systems_capacity",
  category: "capacity",
  severity: "high",
  confidence: 0.72,
  confidence_delta: 0.06,
  confidence_series: {
    points: [
      { timestamp: isoAgo(6), confidence: 0.66 },
      { timestamp: isoAgo(5), confidence: 0.67 },
      { timestamp: isoAgo(4), confidence: 0.68 },
      { timestamp: isoAgo(3), confidence: 0.69 },
      { timestamp: isoAgo(2), confidence: 0.71 },
      { timestamp: isoAgo(1), confidence: 0.72 },
      { timestamp: isoAgo(0), confidence: 0.72 },
    ],
    current: 0.72,
    delta_window_days: 7,
    delta: 0.06,
  },
  resolution_date: isoIn(6),
  why_this_forecast:
    "Sustained utilization likely to hit saturation as integration commitments stack into the same sprint.",
  driving_patterns: [
    {
      id: "engineering-cycle-time",
      title: "Engineering cycle time increasing",
      status: "strengthening",
      supported_forecast_count: 2,
      source_coverage: ["Sprint", "Velocity"],
    },
    {
      id: "oncall-load",
      title: "On-call rotation under-staffed",
      status: "strengthening",
      supported_forecast_count: 1,
      source_coverage: ["Pager", "Schedule"],
    },
  ],
  leading_indicators: [
    { id: "ind-comm", label: "Active commitments", value_label: "+3", direction: "up", severity: "negative", timeframe: "This sprint", sparkline: [0.3, 0.35, 0.4, 0.5, 0.6, 0.7, 0.78] },
    { id: "ind-oncall", label: "On-call load", value_label: "4/6 weeks", direction: "up", severity: "negative", timeframe: "Last 6 weeks", sparkline: [0.3, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7] },
  ],
  would_change_if: [
    { id: "f-1", text: "Two commitments are explicitly deferred or re-staffed before May 22.", observable: true, timeframe: "7 days", status: "unmet" },
  ],
  intervention_levers: [
    { id: "lv-1", label: "Pause net-new platform commitments", expected_effect: "Capacity decreases ~8%", action_type: "create_proposed_change" },
    { id: "lv-2", label: "Re-staff on-call rotation", expected_effect: "On-call load returns within target", action_type: "create_proposed_change" },
  ],
  related_context: { model_links: [{ label: "Systems & Capacity → Engineering", href: "/model?focus=systems_capacity" }], today_links: [], ledger_links: [] },
  evidence_summary: { signal_count: 3, quality: "moderate", sources: [{ label: "sprint_planning", strength: "strong", count: 1 }, { label: "oncall_rotation", strength: "moderate", count: 1 }] },
  target_label: "Engineering",
  impact: { label: "92% capacity", value: 92, unit: "other" },
};

export const FORECASTS_PAGE_FIXTURE: ForecastsPagePayload = {
  header: {
    active_forecast_count: 6,
    resolving_soon_count: 2,
    accelerating_pattern_count: 4,
    calibrated_accuracy: 0.71,
    horizon_days: 90,
    last_updated_at: NOW().toISOString(),
  },
  foresight_brief: {
    statement:
      "Engineering capacity and anchor-account reliability are the two futures most likely to move this month.",
    what_changed: [
      { id: "wc-1", label: "Beacon renewal risk increased", direction: "up", severity: "critical" },
      { id: "wc-2", label: "Engineering capacity forecast moved 88% → 92%", direction: "up", severity: "high" },
      { id: "wc-3", label: "Pricing-owner gap affecting Q3 delivery", direction: "up", severity: "medium" },
    ],
    resolves_soon: [
      { forecast_id: BEACON_ID, label: "Beacon renewal risk", resolution_date: isoIn(2) },
      { forecast_id: ENG_ID, label: "Engineering capacity >90%", resolution_date: isoIn(6) },
      { forecast_id: PRICING_ID, label: "Pricing-owner decision", resolution_date: isoIn(9) },
    ],
    interventions: [
      { id: "iv-1", label: "Assign sync escalation owner", related_forecast_id: BEACON_ID, action_type: "create_delta" },
      { id: "iv-2", label: "Pause net-new platform commitments", related_forecast_id: ENG_ID, action_type: "create_delta" },
      { id: "iv-3", label: "Resolve pricing ownership", related_forecast_id: PRICING_ID, action_type: "create_delta" },
    ],
  },
  horizon: {
    domains: [
      {
        id: "customers_revenue",
        label: "Customers & Revenue",
        cells: [
          {
            horizon_id: "next_14_days",
            forecasts: [
              {
                id: BEACON_ID,
                statement: "Beacon renewal risk likely to increase",
                domain: "customers_revenue",
                horizon: "next_14_days",
                confidence: 0.78,
                confidence_delta: 0.13,
                resolution_date: isoIn(2),
                impact: { label: "$1.2M ARR", value: 1_200_000, unit: "ARR" },
                top_driver: "sync failures",
                trend: "up",
                severity: "critical",
                intervention_available: true,
                sparkline: [0.55, 0.6, 0.65, 0.68, 0.72, 0.75, 0.78],
              },
            ],
            hidden_count: 0,
          },
          { horizon_id: "days_15_45", forecasts: [], hidden_count: 0 },
          {
            horizon_id: "days_46_90",
            forecasts: [
              {
                id: PARTNER_ID,
                statement: "Design partner health will remain weak",
                domain: "customers_revenue",
                horizon: "days_46_90",
                confidence: 0.46,
                resolution_date: isoIn(34),
                impact: null,
                top_driver: "Engagement composite",
                trend: "down",
                severity: "medium",
                intervention_available: true,
                sparkline: [0.5, 0.48, 0.47, 0.46, 0.46, 0.46, 0.46],
              },
            ],
            hidden_count: 0,
          },
        ],
      },
      {
        id: "commitments_delivery",
        label: "Commitments & Delivery",
        cells: [
          { horizon_id: "next_14_days", forecasts: [], hidden_count: 0 },
          {
            horizon_id: "days_15_45",
            forecasts: [
              {
                id: Q3_ID,
                statement: "Q3 delivery commitments at risk",
                domain: "commitments_delivery",
                horizon: "days_15_45",
                confidence: 0.65,
                confidence_delta: 0,
                resolution_date: isoIn(19),
                impact: { label: "$480K ARR", value: 480_000, unit: "ARR" },
                top_driver: "Blocked dependencies",
                trend: "up",
                severity: "high",
                intervention_available: true,
                sparkline: [0.5, 0.55, 0.58, 0.6, 0.62, 0.64, 0.65],
              },
            ],
            hidden_count: 0,
          },
          { horizon_id: "days_46_90", forecasts: [], hidden_count: 0 },
        ],
      },
      {
        id: "systems_capacity",
        label: "Systems & Capacity",
        cells: [
          {
            horizon_id: "next_14_days",
            forecasts: [
              {
                id: ENG_ID,
                statement: "Engineering capacity will exceed 90%",
                domain: "systems_capacity",
                horizon: "next_14_days",
                confidence: 0.72,
                confidence_delta: 0.06,
                resolution_date: isoIn(6),
                impact: { label: "92% capacity", value: 92, unit: "other" },
                top_driver: "Active commitments",
                trend: "up",
                severity: "high",
                intervention_available: true,
                sparkline: [0.4, 0.5, 0.55, 0.6, 0.65, 0.7, 0.72],
              },
            ],
            hidden_count: 0,
          },
          { horizon_id: "days_15_45", forecasts: [], hidden_count: 0 },
          { horizon_id: "days_46_90", forecasts: [], hidden_count: 0 },
        ],
      },
      {
        id: "people_ownership",
        label: "People & Ownership",
        cells: [
          { horizon_id: "next_14_days", forecasts: [], hidden_count: 0 },
          {
            horizon_id: "days_15_45",
            forecasts: [
              {
                id: PRICING_ID,
                statement: "Pricing decision will continue to block roadmap",
                domain: "people_ownership",
                horizon: "days_15_45",
                confidence: 0.55,
                resolution_date: isoIn(28),
                impact: null,
                top_driver: "Age open",
                trend: "up",
                severity: "medium",
                intervention_available: true,
                sparkline: [0.4, 0.45, 0.48, 0.5, 0.52, 0.54, 0.55],
              },
            ],
            hidden_count: 0,
          },
          { horizon_id: "days_46_90", forecasts: [], hidden_count: 0 },
        ],
      },
      {
        id: "finance_capital",
        label: "Finance & Capital",
        cells: [
          { horizon_id: "next_14_days", forecasts: [], hidden_count: 0 },
          { horizon_id: "days_15_45", forecasts: [], hidden_count: 0 },
          {
            horizon_id: "days_46_90",
            forecasts: [
              {
                id: ICP_ID,
                statement: "ICP score will decline below 65",
                domain: "finance_capital",
                horizon: "days_46_90",
                confidence: 0.58,
                resolution_date: isoIn(25),
                impact: null,
                top_driver: "Pipeline ICP avg",
                trend: "down",
                severity: "medium",
                intervention_available: true,
                sparkline: [0.7, 0.68, 0.66, 0.64, 0.62, 0.6, 0.58],
              },
            ],
            hidden_count: 0,
          },
        ],
      },
    ],
    horizons: [
      { id: "next_14_days", label: "Next 14 days", start_day: 0, end_day: 14 },
      { id: "days_15_45", label: "15–45 days", start_day: 15, end_day: 45 },
      { id: "days_46_90", label: "46–90 days", start_day: 46, end_day: 90 },
    ],
  },
  selected_forecast_id: BEACON_ID,
  forecast_details_by_id: { [BEACON_ID]: beaconDetail },
  patterns: [],
  accuracy: {
    period: "last_30_days",
    calibrated_accuracy: 0.71,
    resolved_true: null,
    resolved_false: null,
    pending: 6,
    avg_calibration_error_pp: 4,
    trend: [],
  },
  modes: { default: "horizon", available: ["horizon", "patterns", "scenarios", "accuracy"] },
};

export const PATTERNS_FIXTURE: PatternCard[] = [
  {
    id: "anchor-reliability",
    title: "Anchor accounts reporting reliability issues",
    status: "strengthening",
    supported_forecast_count: 3,
    sources: ["Support", "CRM", "Email"],
    related_forecast_ids: [BEACON_ID, PARTNER_ID, Q3_ID],
    movement: "up",
  },
  {
    id: "engineering-cycle-time",
    title: "Engineering cycle time increasing",
    status: "strengthening",
    supported_forecast_count: 2,
    sources: ["Sprint", "Velocity"],
    related_forecast_ids: [ENG_ID, Q3_ID],
    movement: "up",
  },
  {
    id: "owner-gaps",
    title: "Account-owner response gaps",
    status: "strengthening",
    supported_forecast_count: 2,
    sources: ["Decisions", "Slack"],
    related_forecast_ids: [PRICING_ID, BEACON_ID],
    movement: "up",
  },
  {
    id: "icp-scoring-demand",
    title: "ICP scoring requests rising across enterprise",
    status: "emerging",
    supported_forecast_count: 1,
    sources: ["Pipeline", "Sales"],
    related_forecast_ids: [ICP_ID],
    movement: "up",
  },
  {
    id: "partner-engagement-fade",
    title: "Design partner engagement weakening",
    status: "weakening",
    supported_forecast_count: 1,
    sources: ["Engagement", "Feedback"],
    related_forecast_ids: [PARTNER_ID],
    movement: "down",
  },
];

FORECASTS_PAGE_FIXTURE.patterns = PATTERNS_FIXTURE;

export const ACCURACY_FIXTURE: AccuracyResponse = {
  bins: [
    { bin_label: "50-60", predicted_rate: 0.55, observed_hit_rate: 0.62, n_resolved: 6 },
    { bin_label: "60-70", predicted_rate: 0.65, observed_hit_rate: 0.71, n_resolved: 9 },
    { bin_label: "70-80", predicted_rate: 0.75, observed_hit_rate: 0.79, n_resolved: 11 },
    { bin_label: "80-90", predicted_rate: 0.85, observed_hit_rate: 0.82, n_resolved: 5 },
    { bin_label: "90-100", predicted_rate: 0.95, observed_hit_rate: null, n_resolved: 1 },
  ],
  recent_resolutions: [
    { id: "00000000-0000-0000-0000-000000000201", statement: "Salesforce sync stability will degrade", category: "capacity", confidence: 0.74, outcome: "true", resolution_timeliness: "on_time", resolved_at: isoAgo(14), resolution_at: isoAgo(14) },
    { id: "00000000-0000-0000-0000-000000000202", statement: "Q2 hiring target met for senior backend", category: "capacity", confidence: 0.68, outcome: "true", resolution_timeliness: "early", resolved_at: isoAgo(28), resolution_at: isoAgo(30) },
    { id: "00000000-0000-0000-0000-000000000203", statement: "Mid-market expansion deal will close in March", category: "customer_risk", confidence: 0.62, outcome: "false", resolution_timeliness: "late", resolved_at: isoAgo(42), resolution_at: isoAgo(45) },
  ],
  calibration_summary: {
    value: 0.71,
    delta_vs_last_week: 0.04,
    n_resolved_total: 32,
  },
};

const DETAIL_INDEX: Record<string, ForecastDetail> = {
  [BEACON_ID]: beaconDetail,
  [ENG_ID]: engDetail,
};

export function detailFor(id: string): ForecastDetail | null {
  return DETAIL_INDEX[id] ?? null;
}

export function askFixture(body: ForecastAskRequest): ForecastAskResponse {
  const prompt = (body.prompt ?? "").toLowerCase();
  if (prompt.includes("what if") || prompt.includes("scenario")) {
    return {
      type: "scenario_analysis",
      title: `Scenario: ${body.prompt}`,
      body: "If the intervention is confirmed within 48h, confidence could move from 78% to ~63%. Tradeoffs depend on adjacent commitments.",
      evidence_used: ["Open sync errors", "Champion replies"],
      missing_context: ["No recent Beacon call transcript"],
      actions: [
        { label: "Save scenario", type: "save_scenario" },
        { label: "Create Proposed Change", type: "create_proposed_change" },
      ],
    };
  }
  if (prompt.includes("falsif") || prompt.includes("change")) {
    return {
      type: "falsifier_explanation",
      title: "What would change Fyralis' mind",
      body: "Fyralis will revise this forecast if any of these become observable:\n• No new sync failures for 7 business days.\n• Account owner confirms reporting restored.\n• Renewal sentiment returns neutral or positive.",
      actions: [],
    };
  }
  return {
    type: "forecast_explanation",
    title: "Why this forecast moved",
    body: "Confidence on 'Beacon renewal risk' is 78%. It moved because sync errors are up 42% and champion replies dropped by 3.",
    evidence_used: ["Open sync errors +42%", "Champion replies -3"],
    actions: [{ label: "Open in Model", type: "open_model" }],
  };
}
