// HTTP helpers for the demo simulator surface.

import { API_ROUTES } from "./routes";

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "/api";

// Backend's suggested-signal entries are flat: each tab's items have
// channel-specific keys at the top level (e.g. Slack → channel_name,
// author_label, text; Email → from_label, to_label, subject, body).
// `payload`/`channel` are kept only so the legacy Custom tab still
// works.
export type SuggestedSignalItem = {
  label: string;
  payload?: Record<string, unknown>;
  channel?: string;
  // Slack
  channel_name?: string;
  author_label?: string;
  text?: string;
  // Email
  from_label?: string;
  to_label?: string;
  subject?: string;
  body?: string;
  // GitHub
  repo?: string;
  event_type?: string;
  title?: string;
  // Calendar
  attendees_labels?: string[];
  minutes_ago?: number;
  // Stripe
  customer_label?: string;
  amount_usd?: number;
};

export type SuggestedSignals = {
  company_id: string;
  tabs: {
    slack?: SuggestedSignalItem[];
    email?: SuggestedSignalItem[];
    github?: SuggestedSignalItem[];
    calendar?: SuggestedSignalItem[];
    stripe?: SuggestedSignalItem[];
    custom?: SuggestedSignalItem[];
  };
};

export type InjectResponse = {
  observation_id: string;
  deduped: boolean;
  trigger_queue_id?: string | null;
};

export type SessionInfo = {
  id: string;
  tenant_id?: string;
  total_cost_usd?: number;
  signals_injected?: number;
  actions_taken?: number;
  ended_at?: string | null;
  [k: string]: unknown;
};

class DemoApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function authedRequest<T>(
  path: string,
  token: string,
  init?: RequestInit
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      Authorization: `Bearer ${token}`,
      ...(init?.headers ?? {}),
    },
  });
  if (!res.ok) {
    throw new DemoApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

export function getSuggestedSignals(token: string): Promise<SuggestedSignals> {
  return authedRequest<SuggestedSignals>(
    API_ROUTES.demo.simulatorSuggested,
    token
  );
}

export function injectSignal(
  token: string,
  channel: string,
  payload: Record<string, unknown>
): Promise<InjectResponse> {
  return authedRequest<InjectResponse>(
    API_ROUTES.demo.simulatorInject,
    token,
    {
      method: "POST",
      body: JSON.stringify({ channel, payload }),
    }
  );
}

export function getSession(
  token: string,
  sessionId: string
): Promise<SessionInfo> {
  return authedRequest<SessionInfo>(
    API_ROUTES.demo.session(sessionId),
    token
  );
}

export function endSession(
  token: string,
  sessionId: string
): Promise<{ ended: true }> {
  return authedRequest<{ ended: true }>(
    API_ROUTES.demo.endSession(sessionId),
    token,
    { method: "POST" }
  );
}

export function resetSession(
  token: string,
  sessionId: string
): Promise<{ reset: true }> {
  return authedRequest<{ reset: true }>(
    API_ROUTES.demo.resetSession(sessionId),
    token,
    { method: "POST" }
  );
}

export { DemoApiError };
