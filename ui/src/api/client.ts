// Thin fetch wrapper around the Company OS CEO view HTTP endpoints.
// Contract source: CONTRACTS.md §1.1–1.3. Endpoints live under /api when
// running against the Vite dev server (see vite.config.ts proxy).

import { getAuthHeader, handleAuthFailure } from "./auth";
import { API_ROUTES } from "./routes";
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

export function getHome(signal?: AbortSignal): Promise<HomeResponse> {
  return request<HomeResponse>(API_ROUTES.ceo.home, undefined, signal);
}

export function postAsk(
  body: AskRequest,
  signal?: AbortSignal
): Promise<AskResponse> {
  return request<AskResponse>(
    API_ROUTES.ceo.ask,
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

// Driftwood revision: card-scoped probe endpoints.
// GET   /v1/cards/{card_id}/conversation     → CardConversation
// POST  /v1/cards/{card_id}/probe            → ProbeResponse
// DELETE /v1/cards/{card_id}/conversation    → 204 (clear)
import type {
  CardConversation,
  ProbeRequest,
  ProbeResponse,
} from "./today-types";

export function getCardConversation(
  cardId: string,
  signal?: AbortSignal
): Promise<CardConversation> {
  return request<CardConversation>(
    API_ROUTES.cards.conversation(cardId),
    undefined,
    signal
  );
}

export function postCardProbe(
  cardId: string,
  body: ProbeRequest,
  signal?: AbortSignal
): Promise<ProbeResponse> {
  return request<ProbeResponse>(
    API_ROUTES.cards.probe(cardId),
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}

export function clearCardConversation(
  cardId: string,
  signal?: AbortSignal
): Promise<{ ok: boolean }> {
  return request<{ ok: boolean }>(
    API_ROUTES.cards.conversation(cardId),
    { method: "DELETE" },
    signal
  );
}

export function postTurnAction(
  body: TurnActionRequest,
  signal?: AbortSignal
): Promise<TurnActionResponse> {
  return request<TurnActionResponse>(
    API_ROUTES.ceo.turnAction,
    { method: "POST", body: JSON.stringify(body) },
    signal
  );
}
