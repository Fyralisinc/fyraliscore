// Thin fetch wrapper around the Company OS CEO view HTTP endpoints.
// Contract source: CONTRACTS.md §1.1–1.3. Endpoints live under /api when
// running against the Vite dev server (see vite.config.ts proxy).

import type {
  AskRequest,
  AskResponse,
  HomeResponse,
  TurnActionRequest,
  TurnActionResponse,
} from "./types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

export class ApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "content-type": "application/json" },
    signal,
    ...init,
  });
  if (!res.ok) {
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

export function getHome(signal?: AbortSignal): Promise<HomeResponse> {
  return request<HomeResponse>("/view/ceo/home", undefined, signal);
}

export function postAsk(
  body: AskRequest,
  signal?: AbortSignal
): Promise<AskResponse> {
  return request<AskResponse>(
    "/view/ceo/ask",
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

export function postTurnAction(
  body: TurnActionRequest,
  signal?: AbortSignal
): Promise<TurnActionResponse> {
  return request<TurnActionResponse>(
    "/view/ceo/turn-action",
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}
