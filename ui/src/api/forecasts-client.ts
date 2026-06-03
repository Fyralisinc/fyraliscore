// HTTP client for the Forecasts page v1.0 spec endpoints.
// Backend: services/forecasts/router.py. Endpoints are mounted under
// /api when running through the Vite dev proxy.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import { API_ROUTES } from "./routes";
import type {
  AccuracyResponse,
  CreateScenarioBody,
  ForecastAskRequest,
  ForecastAskResponse,
  ForecastDetail,
  ForecastsPagePayload,
  PatternsResponse,
  PredictionRow,
} from "./forecasts-types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

async function request<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...getAuthHeader(),
      ...((init?.headers as Record<string, string> | undefined) ?? {}),
    },
    signal,
  });
  if (!res.ok) {
    if (res.status === 401) handleAuthFailure();
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

export function getForecastsPage(
  horizonDays = 90,
  signal?: AbortSignal,
): Promise<ForecastsPagePayload> {
  return request<ForecastsPagePayload>(
    API_ROUTES.forecasts.page(horizonDays),
    undefined,
    signal,
  );
}

export function getForecastDetail(
  id: string,
  signal?: AbortSignal,
): Promise<ForecastDetail> {
  return request<ForecastDetail>(
    API_ROUTES.forecasts.detail(id),
    undefined,
    signal,
  );
}

export function getPatterns(
  signal?: AbortSignal,
): Promise<PatternsResponse> {
  return request<PatternsResponse>(
    API_ROUTES.forecasts.patterns,
    undefined,
    signal,
  );
}

export function askForecasts(
  body: ForecastAskRequest,
  signal?: AbortSignal,
): Promise<ForecastAskResponse> {
  return request<ForecastAskResponse>(
    API_ROUTES.forecasts.ask,
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
}

export function getAccuracy(
  rangeDays = 180,
  signal?: AbortSignal,
): Promise<AccuracyResponse> {
  return request<AccuracyResponse>(
    API_ROUTES.forecasts.accuracy(rangeDays),
    undefined,
    signal,
  );
}

export function createScenario(
  body: CreateScenarioBody,
  signal?: AbortSignal,
): Promise<PredictionRow> {
  return request<PredictionRow>(
    API_ROUTES.forecasts.createScenario,
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
}

export { ApiError };
