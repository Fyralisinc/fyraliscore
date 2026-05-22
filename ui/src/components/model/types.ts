// Shared types for the Model page components. Kept local to the
// `model/` folder so cross-cutting concerns (band ordering, filter
// shape, layout config) live in one place.

import type { MapBand } from "@/api/map-types";

export const BAND_ORDER: MapBand[] = [
  "goal",
  "commitment",
  "decision",
  "risk",
  "customer",
];

export const BAND_LABELS: Record<MapBand, string> = {
  goal: "GOALS",
  commitment: "COMMITMENTS",
  decision: "DECISIONS",
  risk: "CONSTRAINTS / RISKS",
  customer: "CUSTOMER IMPACT",
};

export const BAND_TYPE_LABELS: Record<MapBand, string> = {
  goal: "GOAL",
  commitment: "COMMITMENT",
  decision: "DECISION",
  risk: "RISK",
  customer: "CUSTOMER",
};

export type ShowFilters = Record<MapBand, boolean>;
export type StatusFilters = {
  active: boolean;
  blocked: boolean;
  contested: boolean;
};

export type LensId =
  | "company"
  | "commitments"
  | "decisions"
  | "customers"
  | "teams"
  | "risks"
  | "owners"
  | "predictions";

export const DEFAULT_SHOW: ShowFilters = {
  goal: true,
  commitment: true,
  decision: true,
  risk: true,
  customer: true,
};

export const DEFAULT_STATUS: StatusFilters = {
  active: true,
  blocked: true,
  contested: true,
};

export type ViewMode = "map" | "table" | "timeline";
