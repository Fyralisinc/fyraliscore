// HTTP client for the Decision Deltas surface.
// Backend: services/decision_deltas/router.py (mounted at /v1/decision_deltas).
// Endpoints served by the Vite dev proxy (or mock-server.ts in USE_MOCK mode).

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  AddContextBody,
  ContestBody,
  DecisionDelta,
  DelegateBody,
  ListDeltasParams,
  ListDeltasResponse,
  MutationResponse,
} from "./decision-deltas-types";

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

function buildQuery(params: ListDeltasParams | undefined): string {
  if (!params) return "";
  const qp = new URLSearchParams();
  if (params.status !== undefined) {
    if (Array.isArray(params.status)) {
      // Backend expects a single value per call; pass the first.
      // (The repo accepts an iterable, but the HTTP layer reads only one.)
      if (params.status.length > 0) qp.set("status", params.status[0]);
    } else {
      qp.set("status", params.status);
    }
  }
  if (params.category) qp.set("category", params.category);
  if (params.target_kind) qp.set("target_kind", params.target_kind);
  if (params.target_id) qp.set("target_id", params.target_id);
  if (params.limit != null) qp.set("limit", String(params.limit));
  const s = qp.toString();
  return s ? `?${s}` : "";
}

export function listDeltas(
  params?: ListDeltasParams,
  signal?: AbortSignal
): Promise<ListDeltasResponse> {
  return request<ListDeltasResponse>(
    `/v1/decision_deltas/${buildQuery(params)}`,
    undefined,
    signal
  );
}

export function getDelta(
  id: string,
  signal?: AbortSignal
): Promise<DecisionDelta> {
  return request<DecisionDelta>(
    `/v1/decision_deltas/${encodeURIComponent(id)}`,
    undefined,
    signal
  );
}

export function acceptDelta(
  id: string,
  signal?: AbortSignal
): Promise<MutationResponse> {
  return request<MutationResponse>(
    `/v1/decision_deltas/${encodeURIComponent(id)}/accept`,
    { method: "POST", body: JSON.stringify({}) },
    signal
  );
}

export function delegateDelta(
  id: string,
  body: DelegateBody,
  signal?: AbortSignal
): Promise<MutationResponse> {
  return request<MutationResponse>(
    `/v1/decision_deltas/${encodeURIComponent(id)}/delegate`,
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

export function contestDelta(
  id: string,
  body: ContestBody,
  signal?: AbortSignal
): Promise<MutationResponse> {
  return request<MutationResponse>(
    `/v1/decision_deltas/${encodeURIComponent(id)}/contest`,
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

export function addContext(
  id: string,
  body: AddContextBody,
  signal?: AbortSignal
): Promise<MutationResponse> {
  return request<MutationResponse>(
    `/v1/decision_deltas/${encodeURIComponent(id)}/add_context`,
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

export { ApiError };
