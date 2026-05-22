// HTTP client for the Today page v2 (services/gateway/today_routes.py).
// Endpoints live under /api/today/* in dev (Vite proxy) and prod (nginx).
// The mock-server intercept (USE_MOCK=1) is handled separately.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  ApplyResult,
  CorrectionBody,
  CorrectionResult,
  DecisionDelta,
  DelegateBody,
  DelegateResult,
  EvidenceResponse,
  TodayPageData,
} from "./today-page-types";

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
    if (res.status === 409) {
      // Return body so caller can render "requires_refresh" path.
      const body = await safeJson(res);
      const err = new ApiError(`409 ${res.statusText}`, 409);
      (err as ApiError & { body?: unknown }).body = body;
      throw err;
    }
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  if (res.status === 204) return undefined as unknown as T;
  return (await res.json()) as T;
}

async function safeJson(res: Response): Promise<unknown> {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

export function getTodayPage(
  signal?: AbortSignal,
  since?: string,
): Promise<TodayPageData> {
  const qp = since ? `?since=${encodeURIComponent(since)}` : "";
  return request<TodayPageData>(`/today${qp}`, undefined, signal);
}

export function getDeltaDetail(
  deltaId: string,
  signal?: AbortSignal,
): Promise<DecisionDelta> {
  return request<DecisionDelta>(
    `/today/deltas/${encodeURIComponent(deltaId)}`,
    undefined,
    signal,
  );
}

export function getDeltaEvidence(
  deltaId: string,
  signal?: AbortSignal,
): Promise<EvidenceResponse> {
  return request<EvidenceResponse>(
    `/today/deltas/${encodeURIComponent(deltaId)}/evidence`,
    undefined,
    signal,
  );
}

export function applyDelta(
  deltaId: string,
  signal?: AbortSignal,
): Promise<ApplyResult> {
  return request<ApplyResult>(
    `/today/deltas/${encodeURIComponent(deltaId)}/apply`,
    { method: "POST", body: JSON.stringify({}) },
    signal,
  );
}

export function delegateDelta(
  deltaId: string,
  body: DelegateBody,
  signal?: AbortSignal,
): Promise<DelegateResult> {
  return request<DelegateResult>(
    `/today/deltas/${encodeURIComponent(deltaId)}/delegate`,
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
}

export function submitCorrection(
  deltaId: string,
  body: CorrectionBody,
  signal?: AbortSignal,
): Promise<CorrectionResult> {
  return request<CorrectionResult>(
    `/today/deltas/${encodeURIComponent(deltaId)}/correction`,
    { method: "POST", body: JSON.stringify(body) },
    signal,
  );
}

export { ApiError };
