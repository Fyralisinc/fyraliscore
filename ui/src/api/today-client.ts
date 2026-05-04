// HTTP client for the Today page surface.
// Endpoints live under /api when running against the Vite dev server
// (see vite.config.ts proxy). The mock-server.ts plugin serves these
// against the fixture in src/api/today-mock.ts.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  TodayResponse,
  TriageRequest,
  TriageResponse,
  WatchResponse,
} from "./today-types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

async function request<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal
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

export function getToday(signal?: AbortSignal): Promise<TodayResponse> {
  return request<TodayResponse>("/v1/today", undefined, signal);
}

export function postTriage(
  recommendationId: string,
  body: TriageRequest,
  signal?: AbortSignal
): Promise<TriageResponse> {
  // The gateway splits the action surface in two:
  //   /act      — applies the recommendation's proposed_change
  //   /triage   — hold | route | snooze | dismiss (no Acts mutation)
  // Route by action so the UI can issue the right call uniformly.
  if (body.action === "act") {
    const actBody: { notes?: string; selected_path_id?: string } = {};
    if (body.notes) actBody.notes = body.notes;
    if (body.selected_path_id) actBody.selected_path_id = body.selected_path_id;
    return request<TriageResponse>(
      `/v1/recommendations/${recommendationId}/act`,
      { method: "POST", body: JSON.stringify(actBody) },
      signal
    );
  }
  if (body.action === "dismiss") {
    return request<TriageResponse>(
      `/v1/recommendations/${recommendationId}/dismiss`,
      { method: "POST", body: JSON.stringify({ reason: body.reason ?? body.notes ?? "user dismissed" }) },
      signal
    );
  }
  return request<TriageResponse>(
    `/v1/recommendations/${recommendationId}/triage`,
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

export function postWatch(
  recommendationId: string,
  predicate: string,
  signal?: AbortSignal
): Promise<WatchResponse> {
  return request<WatchResponse>(
    `/v1/recommendations/${recommendationId}/watch`,
    { method: "POST", body: JSON.stringify({ predicate }) },
    signal
  );
}

export function deleteWatch(
  recommendationId: string,
  signal?: AbortSignal
): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(
    `/v1/recommendations/${recommendationId}/watch`,
    { method: "DELETE" },
    signal
  );
}

export function postRename(
  newName: string,
  signal?: AbortSignal
): Promise<{ ok: boolean; name: string }> {
  return request("/v1/today/brand", {
    method: "POST",
    body: JSON.stringify({ name: newName }),
    signal,
  });
}

export { ApiError };
