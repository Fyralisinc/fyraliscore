// Spec-aligned HTTP clients. Each endpoint tries the backend first and
// gracefully falls back to the mock fixtures when:
//   - USE_MOCK=1 is set at build time (CI, demo build)
//   - the backend returns 404 (endpoint not yet wired)
//   - the response is empty / malformed (legacy tenant)
//
// This keeps the demo working while backend endpoints come online.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  ListLedgerEventsParams,
  ListLedgerEventsResponse,
  SpecLedgerEvent,
} from "./ledger-event-types";
import type {
  ListThreadsParams,
  ListThreadsResponse,
  OperatingThread,
  RecentModelChange,
} from "./operating-thread-types";
import type { ListSpecDeltasResponse, SpecDelta } from "./spec-delta-types";
import type { SpecForecast } from "./spec-forecast-types";
import {
  RECENT_MODEL_CHANGES_FIXTURE,
  SPEC_DELTAS_RESPONSE,
  SPEC_FORECASTS_FIXTURE,
  SPEC_LEDGER_RESPONSE,
  SPEC_THREADS_FIXTURE,
  SPEC_THREADS_RESPONSE,
} from "./spec-mocks";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";
const FORCE_MOCK = (import.meta.env.VITE_USE_MOCK ?? "") === "1";

async function tryRequest<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal
): Promise<T | null> {
  if (FORCE_MOCK) return null;
  try {
    const res = await fetch(`${BASE}${path}`, {
      ...init,
      headers: {
        "content-type": "application/json",
        ...getAuthHeader(),
        ...((init?.headers as Record<string, string> | undefined) ?? {}),
      },
      signal,
    });
    if (res.status === 401) {
      handleAuthFailure();
      throw new ApiError("401 Unauthorized", 401);
    }
    if (res.status === 404) return null;
    if (!res.ok) return null;
    return (await res.json()) as T;
  } catch (err) {
    if ((err as Error)?.name === "AbortError") throw err;
    return null;
  }
}

// ─── Operating Threads ────────────────────────────────────────────

export async function listOperatingThreads(
  params?: ListThreadsParams,
  signal?: AbortSignal
): Promise<ListThreadsResponse> {
  const qp = new URLSearchParams();
  if (params?.lens) qp.set("lens", params.lens);
  if (params?.search) qp.set("q", params.search);
  if (params?.status && params.status.length > 0) {
    qp.set("status", params.status.join(","));
  }
  const q = qp.toString();
  const data = await tryRequest<ListThreadsResponse>(
    `/v1/spec/operating_threads/${q ? `?${q}` : ""}`,
    undefined,
    signal
  );
  if (data && data.groups && data.groups.length > 0) return data;
  return SPEC_THREADS_RESPONSE;
}

export async function getOperatingThread(
  id: string,
  signal?: AbortSignal
): Promise<OperatingThread | null> {
  const data = await tryRequest<OperatingThread>(
    `/v1/spec/operating_threads/${encodeURIComponent(id)}`,
    undefined,
    signal
  );
  if (data) return data;
  return SPEC_THREADS_FIXTURE.find((t) => t.id === id) ?? null;
}

export async function listRecentModelChanges(
  signal?: AbortSignal
): Promise<RecentModelChange[]> {
  const data = await tryRequest<{ items: RecentModelChange[] }>(
    `/v1/spec/operating_threads/recent_changes`,
    undefined,
    signal
  );
  if (data && data.items && data.items.length > 0) return data.items;
  return RECENT_MODEL_CHANGES_FIXTURE;
}

// ─── Decision Deltas (spec view) ─────────────────────────────────

export async function listSpecDeltas(
  signal?: AbortSignal
): Promise<ListSpecDeltasResponse> {
  const data = await tryRequest<ListSpecDeltasResponse>(
    `/v1/spec/decision_deltas/`,
    undefined,
    signal
  );
  if (data && data.deltas && data.deltas.length > 0) return data;
  return SPEC_DELTAS_RESPONSE;
}

export async function getSpecDelta(
  id: string,
  signal?: AbortSignal
): Promise<SpecDelta | null> {
  const data = await tryRequest<SpecDelta>(
    `/v1/spec/decision_deltas/${encodeURIComponent(id)}`,
    undefined,
    signal
  );
  if (data) return data;
  return SPEC_DELTAS_RESPONSE.deltas.find((d) => d.id === id) ?? null;
}

// Mutations — these always try the backend; on success they return the
// updated SpecDelta. On failure, the caller decides whether to roll
// back the optimistic store change.
type Mutation = "accept" | "delegate" | "contest" | "snooze" | "add_context";

export async function mutateSpecDelta(
  id: string,
  op: Mutation,
  body?: Record<string, unknown>,
  signal?: AbortSignal
): Promise<SpecDelta | null> {
  const path = `/v1/spec/decision_deltas/${encodeURIComponent(id)}/${op}`;
  try {
    const res = await fetch(`${BASE}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        ...getAuthHeader(),
      },
      body: JSON.stringify(body ?? {}),
      signal,
    });
    if (res.status === 401) {
      handleAuthFailure();
      return null;
    }
    if (res.status === 404) return null;
    if (!res.ok) return null;
    const json = (await res.json()) as { delta?: SpecDelta };
    return json.delta ?? null;
  } catch {
    return null;
  }
}

// ─── Forecasts (spec) ────────────────────────────────────────────

export async function listSpecForecasts(
  signal?: AbortSignal
): Promise<SpecForecast[]> {
  const data = await tryRequest<{ items: SpecForecast[] }>(
    `/v1/spec/forecasts/`,
    undefined,
    signal
  );
  if (data && data.items && data.items.length > 0) return data.items;
  return SPEC_FORECASTS_FIXTURE;
}

export async function getSpecForecast(
  id: string,
  signal?: AbortSignal
): Promise<SpecForecast | null> {
  const data = await tryRequest<SpecForecast>(
    `/v1/spec/forecasts/${encodeURIComponent(id)}`,
    undefined,
    signal
  );
  if (data) return data;
  return SPEC_FORECASTS_FIXTURE.find((f) => f.id === id) ?? null;
}

// ─── Ledger (unified) ────────────────────────────────────────────

export async function listLedgerEvents(
  params?: ListLedgerEventsParams,
  signal?: AbortSignal
): Promise<ListLedgerEventsResponse> {
  const qp = new URLSearchParams();
  if (params?.kinds && params.kinds.length > 0) qp.set("kinds", params.kinds.join(","));
  if (params?.categories && params.categories.length > 0) qp.set("categories", params.categories.join(","));
  if (params?.search) qp.set("q", params.search);
  if (params?.threadId) qp.set("thread_id", params.threadId);
  if (params?.limit) qp.set("limit", String(params.limit));
  if (params?.highImpactOnly) qp.set("high_impact_only", "1");
  const q = qp.toString();
  const data = await tryRequest<ListLedgerEventsResponse>(
    `/v1/spec/ledger_events/${q ? `?${q}` : ""}`,
    undefined,
    signal
  );
  if (data && data.events && data.events.length > 0) {
    // Apply client-side filtering if backend didn't.
    return {
      ...data,
      events: filterEvents(data.events, params),
    };
  }
  return {
    ...SPEC_LEDGER_RESPONSE,
    events: filterEvents(SPEC_LEDGER_RESPONSE.events, params),
  };
}

function filterEvents(
  events: SpecLedgerEvent[],
  params?: ListLedgerEventsParams
): SpecLedgerEvent[] {
  let xs = events.slice();
  if (params?.kinds && params.kinds.length > 0) {
    xs = xs.filter((e) => params.kinds!.includes(e.kind));
  }
  if (params?.categories && params.categories.length > 0) {
    xs = xs.filter((e) => params.categories!.includes(e.category));
  }
  if (params?.threadId) {
    xs = xs.filter((e) => e.affectedThreadId === params.threadId);
  }
  if (params?.search) {
    const q = params.search.toLowerCase();
    xs = xs.filter((e) =>
      [e.summary, e.body ?? "", e.actor?.label ?? ""]
        .join(" ")
        .toLowerCase()
        .includes(q)
    );
  }
  return xs;
}
