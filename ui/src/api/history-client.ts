// HTTP client for the History page surface.
// Endpoint backed by services.history.aggregator + gateway /v1/history.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  Arc,
  CalibrationSummary,
  HistoryEvent,
  LayerStripCounts,
  Prediction,
  ShapeToken,
} from "@/components/history/types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

export type HistoryPeriod = "7d" | "30d" | "90d" | "365d" | "all";

export type HistoryResponse = {
  events: HistoryEvent[];
  predictions: Prediction[];
  arcs: Arc[];
  calibration: CalibrationSummary;
  layer_counts: LayerStripCounts;
  chronicle_statement: ShapeToken[];
  predictions_statement: ShapeToken[];
  arcs_statement: ShapeToken[];
  period: HistoryPeriod;
};

async function request<T>(
  path: string,
  signal?: AbortSignal
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: {
      "content-type": "application/json",
      ...getAuthHeader(),
    },
    signal,
  });
  if (!res.ok) {
    if (res.status === 401) handleAuthFailure();
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

export function getHistory(
  period: HistoryPeriod = "90d",
  signal?: AbortSignal
): Promise<HistoryResponse> {
  return request<HistoryResponse>(
    `/v1/history?period=${encodeURIComponent(period)}`,
    signal
  );
}

export { ApiError };
