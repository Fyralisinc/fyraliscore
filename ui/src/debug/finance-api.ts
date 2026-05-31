// Thin fetch helper for the /finance/* testing control plane on the gateway.
// Same /api prefix (vite proxy) + X-Tenant-Id header convention as debug/api.ts.

const BASE = "/api/finance";

export const DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001";

export function financeTenant(): string {
  try {
    return localStorage.getItem("financeTenantId") || DEFAULT_TENANT_ID;
  } catch {
    return DEFAULT_TENANT_ID;
  }
}

export function setFinanceTenant(tid: string): void {
  try {
    localStorage.setItem("financeTenantId", tid);
  } catch {
    /* ignore */
  }
}

function headers(): Record<string, string> {
  return { "content-type": "application/json", "X-Tenant-Id": financeTenant() };
}

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { ...init, headers: headers() });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 240)}`);
  }
  return res.json() as Promise<T>;
}

export type FinanceSource = "mercury" | "quickbooks";

export type SourceInfo = { source: FinanceSource; channel: string; label: string };

export type InstallResult = {
  source: string;
  installation_id: string;
  sub_resources: number;
  webhook_secret_registered: boolean;
  message: string;
};

export type BackfillResult = {
  source: string;
  records: number;
  ingested: number;
  deduped: number;
  message: string;
  results: { observation_id: string; external_id: string; deduped: boolean; kind: string }[];
};

export type LiveResult = {
  source: string;
  delivered_via: "webhook" | "inline_fallback";
  webhook_status: number | null;
  payload_kind: string;
};

export type StatusResult = {
  source: string;
  channel: string;
  installed: boolean;
  install: Record<string, unknown> | null;
  sub_resources: { [k: string]: unknown }[];
  counts: { total: number; signal: number; state_change: number };
  recent: {
    id: string;
    kind: string;
    external_id: string;
    content_text: string;
    occurred_at: string | null;
    ingested_at: string | null;
  }[];
};

export const listSources = () => req<{ sources: SourceInfo[] }>("/sources");

export const install = (source: FinanceSource) =>
  req<InstallResult>(`/${source}/install`, { method: "POST", body: "{}" });

export const backfill = (source: FinanceSource, count: number, seed: number) =>
  req<BackfillResult>(`/${source}/backfill`, {
    method: "POST",
    body: JSON.stringify({ count, seed }),
  });

export const liveEmit = (source: FinanceSource, seq: number) =>
  req<LiveResult>(`/${source}/live/emit`, {
    method: "POST",
    body: JSON.stringify({ seq }),
  });

export const status = (source: FinanceSource) =>
  req<StatusResult>(`/${source}/status`);
