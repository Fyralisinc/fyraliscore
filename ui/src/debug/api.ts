// Thin fetch helper for the /debug/* endpoints on the gateway.
// Uses the same /api prefix (vite proxy) as the main CEO view.

const BASE = "/api/debug";

function tenantHeaders(): Record<string, string> {
  const tid = localStorage.getItem("demoTenantId");
  return tid ? { "X-Tenant-Id": tid } : {};
}

export async function dget<T = unknown>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const qs = params
    ? "?" +
      Object.entries(params)
        .filter(([, v]) => v !== undefined && v !== "")
        .map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`)
        .join("&")
    : "";
  const res = await fetch(`${BASE}${path}${qs}`, {
    headers: { "content-type": "application/json", ...tenantHeaders() },
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
  }
  return res.json();
}

export type Signal = {
  id: string;
  source_channel: string;
  source_actor_ref: string | null;
  kind: string | null;
  actor_id: string | null;
  occurred_at: string;
  content_text: string | null;
  run_count: number;
};

export type ThinkRun = {
  id: string;
  trigger_id: string;
  trigger_kind: string;
  started_at: string;
  ended_at: string | null;
  status: string;
  error: string | null;
  retrieval_model_count: number | null;
  retrieval_observation_count: number | null;
  llm_latency_ms: number | null;
  validation_error_count: number | null;
  ops_applied: unknown;
  cascade_depth: number | null;
};

export type ModelRow = {
  id: string;
  proposition_kind: string;
  status: string;
  confidence: number;
  confidence_at_assertion: number | null;
  confirmed_count: number | null;
  contested_count: number | null;
  proposition: Record<string, unknown>;
  born_from_event_id: string | null;
  last_confirmed_at: string | null;
  created_at: string;
};

export type Artifact = {
  id: string;
  stage: string;
  payload: unknown;
  captured_at: string;
};

export type SignalDetail = {
  observation: {
    id: string;
    source_channel: string;
    content_text: string | null;
    occurred_at: string;
    kind: string | null;
    content?: Record<string, unknown> | string;
    [k: string]: unknown;
  };
  triggers: {
    id: string;
    trigger_kind: string;
    trigger_subkind: string | null;
    enqueued_at: string;
    scheduled_for: string;
    attempts: number;
    locked_by: string | null;
    locked_at: string | null;
  }[];
  runs: ThinkRun[];
  artifacts: Artifact[];
  models_born: ModelRow[];
};

export type Stats = {
  stats: {
    observations: number;
    active_models: number;
    archived_models: number;
    commitments: number;
    goals: number;
    decisions: number;
    resources: number;
    think_runs: number;
    trigger_queue_depth: number;
    applied_triggers: number;
    artifacts: number;
    renders: number;
  };
  tenant_id: string;
};

export type RenderRow = {
  render_id: string;
  render_kind: string;
  outcome: string;
  llm_calls_count: number;
  llm_input_tokens_total: number;
  llm_output_tokens_total: number;
  llm_cost_usd: string | number;
  latency_total_ms: number;
  retry_count: number;
  flagged: boolean;
  model_name: string | null;
  computed_at: string;
};

export type CacheRow = {
  cache_key: string;
  cached_at: string;
  age_seconds: number;
  payload: unknown;
};
