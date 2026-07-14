import type {
  ControlPanelAccessGrantList,
  ControlPanelState
} from "@/src/types";

export type ControlPanelClientConfig = {
  apiBase: string;
  bearerToken: string;
};

/** Safe, deployment-admin-only Figma OAuth setup metadata. */
export type FigmaDeploymentOAuthReadiness = {
  runtime_ready: boolean;
  source_enabled: boolean;
  checks: Record<string, boolean>;
  redirect_uri: string | null;
  ui_return_origin: string | null;
  required_scopes: string[];
  configured_scopes: string[];
  provider_console_url: string;
  recommended_app_mode: "private" | string;
  provider_app_registration_unverified: boolean;
  setup_checklist: string[];
};

export function defaultControlPanelApiBase() {
  return (
    process.env.NEXT_PUBLIC_FYRALIS_API_BASE?.trim() ||
    process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL?.trim() ||
    ""
  );
}

export async function fetchControlPanelDeployments(
  config: ControlPanelClientConfig,
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
  config: ControlPanelClientConfig,
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

/**
 * Reads the Figma app configuration contract from the gateway. The route is
 * tenant-admin gated, and this client intentionally maps only its safe
 * metadata fields—never arbitrary response fields or secret values.
 */
export async function fetchFigmaDeploymentOAuthReadiness(
  config: ControlPanelClientConfig,
): Promise<FigmaDeploymentOAuthReadiness> {
  const readiness = await getJson<FigmaDeploymentOAuthReadiness>(
    config,
    "/api/admin/integrations/figma/oauth/readiness",
  );
  return {
    runtime_ready: readiness.runtime_ready === true,
    source_enabled: readiness.source_enabled === true,
    checks: Object.fromEntries(
      Object.entries(readiness.checks ?? {}).map(([key, value]) => [
        key,
        value === true,
      ]),
    ),
    redirect_uri: safeString(readiness.redirect_uri),
    ui_return_origin: safeString(readiness.ui_return_origin),
    required_scopes: stringArray(readiness.required_scopes),
    configured_scopes: stringArray(readiness.configured_scopes),
    provider_console_url: safeString(readiness.provider_console_url) ?? "",
    recommended_app_mode:
      safeString(readiness.recommended_app_mode) ?? "private",
    provider_app_registration_unverified:
      readiness.provider_app_registration_unverified === true,
    setup_checklist: stringArray(readiness.setup_checklist),
  };
}

async function getJson<T>(
  config: ControlPanelClientConfig,
  path: string
): Promise<T> {
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

function safeString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function stringArray(value: unknown): string[] {
  return Array.isArray(value)
    ? value.filter((item): item is string => typeof item === "string" && Boolean(item.trim()))
    : [];
}

function assertScope(observed: string, expected: string): void {
  if (observed !== expected) {
    throw new Error(`Unexpected BYOC stored scope: ${observed}`);
  }
}
