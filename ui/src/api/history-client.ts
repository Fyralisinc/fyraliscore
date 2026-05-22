// HTTP client for the Ledger surface.
// Backed by services.history.aggregator + summary; gateway routes
// /v1/history (with surface=ledger + types filter) and /v1/history/summary.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  LedgerEvent,
  LedgerEventType,
  LedgerSummary,
} from "./history-types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

export type HistoryPeriod = "7d" | "30d" | "90d" | "365d" | "all";

export type LedgerHistoryResponse = {
  events: LedgerEvent[];
  period: HistoryPeriod;
  types?: LedgerEventType[];
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

export function getLedgerHistory(
  options: { period?: HistoryPeriod; types?: LedgerEventType[] } = {},
  signal?: AbortSignal
): Promise<LedgerHistoryResponse> {
  const period = options.period ?? "30d";
  const params = new URLSearchParams();
  params.set("period", period);
  params.set("surface", "ledger");
  if (options.types && options.types.length > 0) {
    params.set("types", options.types.join(","));
  }
  return request<LedgerHistoryResponse>(
    `/v1/history?${params.toString()}`,
    signal
  );
}

export function getHistorySummary(
  options: { range_days?: number } = {},
  signal?: AbortSignal
): Promise<LedgerSummary> {
  const range = options.range_days ?? 30;
  return request<LedgerSummary>(
    `/v1/history/summary?range_days=${encodeURIComponent(range)}`,
    signal
  );
}

export { ApiError };
