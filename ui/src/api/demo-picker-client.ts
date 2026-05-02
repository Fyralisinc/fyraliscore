// HTTP helpers for the demo picker + session orchestration surface.
// Endpoints live under /v1/demo and (for /companies, /sessions/start)
// are public; the rest require the Authorization header populated by
// `auth.ts` after a session has started.

import { ApiError } from "./client";
import { getAuthHeader } from "./auth";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

export type DemoCompany = {
  company_id: string;
  name: string;
  tagline: string;
  description: string;
};

export type DemoStartResponse = {
  session_id: string;
  tenant_id: string;
  auth_token: string;
  auth_token_expires_at: string;
  ceo_actor_id: string;
  company_id: string;
};

export type DemoSessionInfo = {
  session_id: string;
  tenant_id: string;
  company_id: string;
  total_cost_usd: number;
  cost_cap_usd: number;
  signals_injected: number;
  ended_at: string | null;
};

async function request<T>(
  path: string,
  init?: RequestInit & { authed?: boolean }
): Promise<T> {
  const { authed, ...rest } = init ?? {};
  const headers: Record<string, string> = {
    "content-type": "application/json",
    ...(rest.headers as Record<string, string> | undefined),
  };
  if (authed) Object.assign(headers, getAuthHeader());
  const res = await fetch(`${BASE}${path}`, { ...rest, headers });
  if (!res.ok) {
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

export async function listDemoCompanies(): Promise<DemoCompany[]> {
  const data = await request<{ items: DemoCompany[] }>("/v1/demo/companies");
  return data.items;
}

export function startDemoSession(
  companyId: string
): Promise<DemoStartResponse> {
  return request<DemoStartResponse>("/v1/demo/sessions/start", {
    method: "POST",
    body: JSON.stringify({ company_id: companyId }),
  });
}

export function endDemoSession(
  sessionId: string
): Promise<{ ended: boolean }> {
  return request<{ ended: boolean }>(
    `/v1/demo/sessions/${sessionId}/end`,
    { method: "POST", authed: true }
  );
}

export function resetDemoSession(
  sessionId: string
): Promise<{ reset: boolean }> {
  return request<{ reset: boolean }>(
    `/v1/demo/sessions/${sessionId}/reset`,
    { method: "POST", authed: true }
  );
}

export function getDemoSession(sessionId: string): Promise<DemoSessionInfo> {
  return request<DemoSessionInfo>(`/v1/demo/sessions/${sessionId}`, {
    authed: true,
  });
}

// LocalStorage keys used by the picker + session wrapper.
export const DEMO_LS_KEYS = {
  authToken: "demoAuthToken",
  sessionId: "demoSessionId",
  ceoActorId: "demoCeoActorId",
  tenantId: "demoTenantId",
  companyId: "demoCompanyId",
} as const;

export function saveDemoSession(s: DemoStartResponse): void {
  localStorage.setItem(DEMO_LS_KEYS.authToken, s.auth_token);
  localStorage.setItem(DEMO_LS_KEYS.sessionId, s.session_id);
  localStorage.setItem(DEMO_LS_KEYS.ceoActorId, s.ceo_actor_id);
  localStorage.setItem(DEMO_LS_KEYS.tenantId, s.tenant_id);
  localStorage.setItem(DEMO_LS_KEYS.companyId, s.company_id);
}

export function clearDemoSession(): void {
  for (const key of Object.values(DEMO_LS_KEYS)) {
    localStorage.removeItem(key);
  }
}
