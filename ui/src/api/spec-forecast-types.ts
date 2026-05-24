// Spec-aligned Forecast view model (spec §7). Wraps the existing
// PredictionRow with spec sections: leading indicators, confidence
// movement, falsification condition, intervention links, evidence
// trace + context gaps.

import type { ContextGap, EvidenceTrace, SourceCoverage } from "./trust-types";
import type { EntityRef, ID } from "./common-types";

export type SpecForecastStatus =
  | "active"
  | "changing"
  | "intervention_available"
  | "resolved_true"
  | "resolved_false"
  | "partially_true"
  | "inconclusive"
  | "superseded"
  | "expired";

export type SpecForecastDomain =
  | "customer"
  | "revenue"
  | "delivery"
  | "capacity"
  | "strategy"
  | "risk";

export interface LeadingIndicator {
  label: string;
  movement?: "rising" | "falling" | "steady";
  detail?: string;
}

export interface SpecForecast {
  id: ID;
  statement: string;
  domain: SpecForecastDomain;
  status: SpecForecastStatus;
  confidence: number;              // 0..1
  confidencePrevious?: number;     // 0..1, for movement display
  resolutionDate?: string;
  leadingIndicators: LeadingIndicator[];
  evidenceTrace: EvidenceTrace;
  sourceCoverage: SourceCoverage[];
  contextGaps: ContextGap[];
  falsificationCondition?: string;
  // Cross-links.
  relatedThreadId?: ID;
  relatedThreadTitle?: string;
  relatedDeltaId?: ID;
  interventionLabel?: string;
  // Outcome (resolved tab).
  outcome?: string;
  outcomeNote?: string;
  calibrationImpact?: number;
  // Severity rail color hint.
  severityHint?: "info" | "forecast" | "review" | "authority" | "critical";
}
