// Forecasts page v1.0 spec — wire contracts.
// Source of truth: fyralis_forecasts_page_implementation_complete_spec_v1.md §30
// Backend: services/forecasts/router.py + services/forecasts/page.py.

// -----------------------------------------------------------------------
// Primitives
// -----------------------------------------------------------------------

export type ForecastDomainId =
  | "customers_revenue"
  | "commitments_delivery"
  | "systems_capacity"
  | "people_ownership"
  | "finance_capital";

export type ForecastCategory =
  | "customer_risk"
  | "capacity"
  | "delivery"
  | "strategy"
  | "decision"
  | "pricing"
  | "partner";

export type ForecastHorizonId =
  | "next_14_days"
  | "days_15_45"
  | "days_46_90";

export type ForecastSeverity =
  | "critical"
  | "high"
  | "medium"
  | "low"
  | "opportunity";

export type ForecastTrend = "up" | "down" | "flat" | "volatile";

export type ForecastMode = "horizon" | "patterns" | "scenarios" | "accuracy";

export type FalsifierStatus = "unmet" | "partially_met" | "met";

export type PatternStatus =
  | "emerging"
  | "strengthening"
  | "stable"
  | "weakening"
  | "resolved"
  | "archived";

export type InterventionActionType =
  | "view_today"
  | "create_proposed_change"
  | "open_model"
  | "ask"
  | "create_delta"
  | "open_today"
  | "save_scenario";

// -----------------------------------------------------------------------
// Page payload — GET /v1/forecasts/page
// -----------------------------------------------------------------------

export interface ForecastsPagePayload {
  header: ForecastsHeaderData;
  foresight_brief: ForesightBriefData;
  horizon: ForecastHorizonData;
  selected_forecast_id: string | null;
  forecast_details_by_id: Record<string, ForecastDetail>;
  patterns: PatternCard[];
  accuracy: ForecastAccuracySummary;
  modes: {
    default: ForecastMode;
    available: ForecastMode[];
  };
}

export interface ForecastsHeaderData {
  active_forecast_count: number;
  resolving_soon_count: number;
  accelerating_pattern_count: number;
  calibrated_accuracy: number | null;
  horizon_days: number;
  last_updated_at: string;
}

export interface ForesightBriefData {
  statement: string;
  what_changed: BriefChangeItem[];
  resolves_soon: BriefResolutionItem[];
  interventions: BriefInterventionItem[];
}

export interface BriefChangeItem {
  id: string;
  label: string;
  direction?: ForecastTrend;
  severity?: ForecastSeverity;
}

export interface BriefResolutionItem {
  forecast_id: string;
  label: string;
  resolution_date: string;
}

export interface BriefInterventionItem {
  id: string;
  label: string;
  related_forecast_id?: string;
  action_type?: InterventionActionType;
}

// -----------------------------------------------------------------------
// Horizon matrix
// -----------------------------------------------------------------------

export interface ForecastHorizonData {
  domains: ForecastDomainRow[];
  horizons: ForecastHorizonColumn[];
}

export interface ForecastDomainRow {
  id: ForecastDomainId;
  label: string;
  cells: ForecastHorizonCell[];
}

export interface ForecastHorizonColumn {
  id: ForecastHorizonId;
  label: string;
  start_day: number;
  end_day: number;
}

export interface ForecastHorizonCell {
  horizon_id: ForecastHorizonId;
  forecasts: ForecastSummaryCard[];
  hidden_count: number;
}

export interface ForecastSummaryCard {
  id: string;
  statement: string;
  domain: ForecastDomainId;
  horizon: ForecastHorizonId;
  confidence: number;
  confidence_delta?: number | null;
  resolution_date?: string | null;
  impact?: { label: string; value?: number; unit?: string } | null;
  top_driver?: string | null;
  trend: ForecastTrend;
  severity?: ForecastSeverity;
  intervention_available?: boolean;
  sparkline?: number[];
}

// -----------------------------------------------------------------------
// Forecast detail — GET /v1/forecasts/detail/{id}
// -----------------------------------------------------------------------

export interface ForecastDetail {
  id: string;
  statement: string;
  domain: ForecastDomainId;
  category: ForecastCategory;
  severity: ForecastSeverity;
  confidence: number;
  confidence_delta?: number | null;
  confidence_series: ConfidenceSeries;
  resolution_date?: string | null;
  resolution_window?: { start: string; end: string } | null;
  why_this_forecast: string;
  driving_patterns: DrivingPattern[];
  leading_indicators: LeadingIndicator[];
  would_change_if: Falsifier[];
  intervention_levers: InterventionLever[];
  related_context: RelatedContext;
  evidence_summary?: EvidenceSummary;
  target_label?: string | null;
  impact?: { label: string; value?: number; unit?: string } | null;
}

export interface ConfidenceSeries {
  points: { timestamp: string; confidence: number }[];
  current: number;
  delta_window_days: number;
  delta: number;
}

export interface DrivingPattern {
  id: string;
  title: string;
  status: PatternStatus;
  supported_forecast_count: number;
  source_coverage?: string[];
}

export interface LeadingIndicator {
  id: string;
  label: string;
  value_label: string;
  direction: ForecastTrend;
  severity?: "positive" | "neutral" | "negative";
  timeframe?: string;
  sparkline?: number[];
}

export interface Falsifier {
  id: string;
  text: string;
  observable: boolean;
  timeframe?: string | null;
  status?: FalsifierStatus;
}

export interface InterventionLever {
  id: string;
  label: string;
  expected_effect?: string | null;
  action_type: InterventionActionType;
  related_object_id?: string | null;
}

export interface RelatedContext {
  model_links: { label: string; href: string }[];
  today_links: { label: string; proposed_change_id?: string }[];
  ledger_links: { label: string; event_id?: string }[];
}

export interface EvidenceSummary {
  signal_count: number;
  quality: "weak" | "partial" | "moderate" | "strong";
  sources: {
    label: string;
    strength: "weak" | "partial" | "moderate" | "strong";
    count?: number;
  }[];
}

// -----------------------------------------------------------------------
// Patterns — GET /v1/forecasts/patterns
// -----------------------------------------------------------------------

export interface PatternCard {
  id: string;
  title: string;
  status: PatternStatus;
  supported_forecast_count: number;
  sources: string[];
  related_forecast_ids: string[];
  confidence?: number | null;
  movement?: ForecastTrend;
}

export interface PatternsResponse {
  patterns: PatternCard[];
  count: number;
}

// -----------------------------------------------------------------------
// Accuracy — reuses existing /v1/forecasts/accuracy shape + summary
// -----------------------------------------------------------------------

export interface ForecastAccuracySummary {
  period: "last_30_days" | "last_90_days" | "all_time";
  calibrated_accuracy: number | null;
  resolved_true: number | null;
  resolved_false: number | null;
  pending: number;
  avg_calibration_error_pp: number | null;
  trend: number[];
}

export interface AccuracyBin {
  bin_label: string;
  predicted_rate: number;
  observed_hit_rate: number | null;
  n_resolved: number;
}

export interface AccuracyRecentResolution {
  id: string;
  statement: string;
  category: ForecastCategory;
  confidence: number;
  outcome: "true" | "false" | "partial";
  resolution_timeliness: "early" | "on_time" | "late" | null;
  resolved_at: string;
  resolution_at: string;
}

export interface AccuracyResponse {
  bins: AccuracyBin[];
  recent_resolutions: AccuracyRecentResolution[];
  calibration_summary: {
    value: number | null;
    delta_vs_last_week: number | null;
    n_resolved_total: number;
  };
}

// -----------------------------------------------------------------------
// Ask Fyralis — POST /v1/forecasts/ask
// -----------------------------------------------------------------------

export interface ForecastAskRequest {
  page?: "forecasts";
  mode?: ForecastMode;
  selected_forecast_id?: string | null;
  selected_pattern_id?: string | null;
  prompt: string;
  visible_forecast_ids?: string[];
  horizon_days?: number;
}

export type ForecastAskResponseType =
  | "forecast_explanation"
  | "scenario_analysis"
  | "falsifier_explanation"
  | "pattern_trace"
  | "intervention_comparison"
  | "accuracy_reference";

export interface ForecastAskResponseAction {
  label: string;
  type: InterventionActionType;
  payload?: unknown;
}

export interface ForecastAskResponse {
  type: ForecastAskResponseType;
  title: string;
  body: string;
  evidence_used?: string[];
  missing_context?: string[];
  actions?: ForecastAskResponseAction[];
}

// -----------------------------------------------------------------------
// Create scenario — POST /v1/forecasts/
// -----------------------------------------------------------------------

export interface CreateScenarioBody {
  statement: string;
  category: ForecastCategory;
  confidence: number;
  resolution_at: string;
  rationale?: string;
  target_label?: string;
  falsification_condition?: string;
  impact?: Record<string, unknown>;
  key_drivers?: { label: string; delta_label?: string; direction?: ForecastTrend }[];
}

// Legacy PredictionRow shape (still returned by POST /). Kept here so
// the create-scenario response continues to type-check.
export interface PredictionRow {
  id: string;
  tenant_id: string;
  status: "active" | "resolved" | "superseded";
  statement: string;
  rationale: string | null;
  category: ForecastCategory;
  target_label: string | null;
  confidence: number;
  resolution_at: string;
  resolved_at: string | null;
  outcome: "true" | "false" | "partial" | null;
}
