import type {
  ControlPanelAccessGrantList,
  ControlPanelState
} from "./types";

export type ClientConfig = {
  apiBase: string;
  bearerToken: string;
};

export async function fetchDeployments(
  config: ClientConfig,
  customerId?: string
): Promise<ControlPanelAccessGrantList> {
  const query = new URLSearchParams();
  if (customerId?.trim()) {
    query.set("customer_id", customerId.trim());
  }
  const deployments = await getJson<ControlPanelAccessGrantList>(
    config,
    `/byoc/control-panel/deployments${withQuery(query)}`
  );
  assertScope(
    deployments.stored_scope,
    "sanitized_control_panel_access_metadata_only"
  );
  return deployments;
}

export async function fetchControlPanelState(
  config: ClientConfig,
  params: {
    deploymentId: string;
    customerId?: string;
    recentLimit: number;
  }
): Promise<ControlPanelState> {
  const query = new URLSearchParams({
    deployment_id: params.deploymentId,
    recent_limit: String(params.recentLimit)
  });
  if (params.customerId?.trim()) {
    query.set("customer_id", params.customerId.trim());
  }
  const state = await getJson<ControlPanelState>(
    config,
    `/byoc/control-panel/state${withQuery(query)}`
  );
  assertScope(state.stored_scope, "sanitized_control_panel_metadata_only");
  assertScope(state.overview.stored_scope, "sanitized_deployment_metadata_only");
  assertScope(
    state.product_health.stored_scope,
    "sanitized_product_health_metadata_only"
  );
  return state;
}

async function getJson<T>(config: ClientConfig, path: string): Promise<T> {
  const response = await fetch(`${trimTrailingSlash(config.apiBase)}${path}`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      ...(config.bearerToken.trim()
        ? { Authorization: `Bearer ${config.bearerToken.trim()}` }
        : {})
    }
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`HTTP ${response.status}: ${body || response.statusText}`);
  }
  return (await response.json()) as T;
}

function withQuery(query: URLSearchParams): string {
  const rendered = query.toString();
  return rendered ? `?${rendered}` : "";
}

function trimTrailingSlash(value: string): string {
  return value.trim().replace(/\/+$/, "");
}

function assertScope(observed: string, expected: string): void {
  if (observed !== expected) {
    throw new Error(`Unexpected BYOC stored scope: ${observed}`);
  }
}
