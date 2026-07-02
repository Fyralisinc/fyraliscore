import { ONBOARDING_SNAPSHOT } from "../data/mock-data";
import type {
  Customer,
  OnboardingIntent,
  OnboardingSnapshot,
  SourceObservation
} from "../types";

export async function fetchOnboardingSnapshot(): Promise<OnboardingSnapshot> {
  return withRuntimeWorkspaceUrls(structuredClone(ONBOARDING_SNAPSHOT));
}

export async function createDesignPartnerOnboardingIntent(): Promise<OnboardingIntent> {
  return postJson<OnboardingIntent>(
    "/platform/onboarding/intents",
    {
      plan_code: "design_partner_byoc_pilot",
      procurement_channel: "design_partner",
      entrypoint: "get_fyralis"
    },
    () => mockIntent()
  );
}

export async function submitDesignPartnerIntake(
  intentId: string,
  customer: Customer
): Promise<OnboardingIntent> {
  try {
    return await postDesignPartnerIntake(intentId, customer);
  } catch (error) {
    if (!isOnboardingIntentNotFound(error)) {
      throw error;
    }
    const intent = await createDesignPartnerOnboardingIntent();
    return postDesignPartnerIntake(intent.intent_id, customer);
  }
}

type GatewayObservation = {
  id: string;
  kind: string;
  source_channel: string;
  occurred_at: string;
  content_text: string;
};

type GatewayObservationResponse = {
  items: GatewayObservation[];
  source?: string;
  stub?: boolean;
};

type SlackRehearsalApiStatus = {
  installed: boolean;
  installation: {
    installation_id: string;
    enabled: boolean;
    has_secret: boolean;
    installed_at: string;
  } | null;
  trigger_count: number;
  consumed_trigger_count: number;
  run_status_counts: Record<string, number>;
  shard_state_counts: Record<string, { count: number; observations_seen: number }>;
  observation_count: number;
  observations: GatewayObservation[];
  unresolved_failure_count: number;
  bearer_token?: string | null;
  session_expires_at?: string | null;
  next_action: string;
};

type SlackRehearsalPrepareApiResponse = {
  enabled: boolean;
  tenant_id: string;
  actor_id: string;
  gateway_api_base: string;
  provider_ingress_url: string;
  oauth_redirect_url: string;
  events_request_url: string;
  install_url: string;
  bearer_token: string;
  session_expires_at: string;
  state_expires_in_seconds: number;
  status: SlackRehearsalApiStatus;
};

export type SlackRehearsalStatus = {
  installed: boolean;
  installation: {
    installationId: string;
    enabled: boolean;
    hasSecret: boolean;
    installedAt: string;
  } | null;
  triggerCount: number;
  consumedTriggerCount: number;
  runStatusCounts: Record<string, number>;
  shardStateCounts: Record<string, { count: number; observationsSeen: number }>;
  observationCount: number;
  observations: SourceObservation[];
  unresolvedFailureCount: number;
  bearerToken?: string | null;
  sessionExpiresAt?: string | null;
  nextAction: string;
};

export type SlackRehearsalPrepareResponse = {
  enabled: boolean;
  tenantId: string;
  actorId: string;
  gatewayApiBase: string;
  providerIngressUrl: string;
  oauthRedirectUrl: string;
  eventsRequestUrl: string;
  installUrl: string;
  bearerToken: string;
  sessionExpiresAt: string;
  stateExpiresInSeconds: number;
  status: SlackRehearsalStatus;
};

export async function fetchGatewaySourceObservations({
  apiBase,
  bearerToken,
  sourceId,
  limit = 50
}: {
  apiBase?: string;
  bearerToken: string;
  sourceId: string;
  limit?: number;
}): Promise<SourceObservation[]> {
  const token = bearerToken.trim();
  if (!token) {
    throw new Error("Gateway bearer token is required.");
  }
  const resolvedApiBase = trimTrailingSlash(
    apiBase ??
      process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL ??
      process.env.NEXT_PUBLIC_FYRALIS_API_BASE ??
      browserOrigin() ??
      ""
  );
  if (!resolvedApiBase) {
    throw new Error("Gateway API base is required.");
  }

  const query = new URLSearchParams({
    limit: String(limit),
    source: sourceId
  });
  const response = await fetch(`${resolvedApiBase}/observations?${query}`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${token}`
    }
  });
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }

  const payload = (await response.json()) as GatewayObservationResponse;
  return payload.items
    .filter((item) => observationBelongsToSource(item, sourceId))
    .map((item) => gatewayObservationToSourceObservation(item, sourceId));
}

export async function prepareSlackRehearsal({
  apiBase
}: {
  apiBase?: string;
} = {}): Promise<SlackRehearsalPrepareResponse> {
  const resolvedApiBase = resolveGatewayApiBase(apiBase);
  const response = await fetch(
    `${resolvedApiBase}/platform/onboarding/slack/rehearsal/prepare`,
    {
      method: "POST",
      headers: {
        Accept: "application/json"
      }
    }
  );
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }
  return mapSlackRehearsalPrepare(
    (await response.json()) as SlackRehearsalPrepareApiResponse
  );
}

export async function fetchSlackRehearsalStatus({
  apiBase
}: {
  apiBase?: string;
} = {}): Promise<SlackRehearsalStatus> {
  const resolvedApiBase = resolveGatewayApiBase(apiBase);
  const response = await fetch(
    `${resolvedApiBase}/platform/onboarding/slack/rehearsal/status`,
    {
      method: "GET",
      headers: {
        Accept: "application/json"
      }
    }
  );
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }
  return mapSlackRehearsalStatus(
    (await response.json()) as SlackRehearsalApiStatus
  );
}

async function postJson<T>(
  path: string,
  body: unknown,
  fallback: () => T
): Promise<T> {
  const apiBase = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_API_BASE ?? ""
  );
  const allowLocalFallback = apiBase === "";
  try {
    const response = await fetch(`${apiBase}${path}`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify(body)
    });
    if (!response.ok) {
      throw new ApiError(response.status, await response.text());
    }
    return (await response.json()) as T;
  } catch (error) {
    if (allowLocalFallback) {
      return fallback();
    }
    throw error;
  }
}

function postDesignPartnerIntake(
  intentId: string,
  customer: Customer
): Promise<OnboardingIntent> {
  return postJson<OnboardingIntent>(
    `/platform/onboarding/intents/${intentId}/design-partner-intake`,
    {
      company_name: customer.company,
      setup_owner_email: customer.setupOwnerEmail,
      target_cloud: targetCloudToApi(customer.targetCloud)
    },
    () =>
      mockIntent({
        intentId,
        status: "workspace_created",
        companyName: customer.company,
        setupOwnerEmail: customer.setupOwnerEmail.toLowerCase(),
        targetCloud: targetCloudToApi(customer.targetCloud),
        customerId: `cus_${randomHex(8)}`,
        tenantId: randomUuid(),
        deploymentId: `dep_${randomHex(8)}`
      })
  );
}

function observationBelongsToSource(
  observation: GatewayObservation,
  sourceId: string
): boolean {
  const sourceChannel = observation.source_channel?.toLowerCase() ?? "";
  const normalized = sourceId.toLowerCase().replaceAll("-", "_");
  return (
    sourceChannel === normalized ||
    sourceChannel.startsWith(`${normalized}:`) ||
    sourceChannel.startsWith(`${sourceId.toLowerCase()}:`)
  );
}

function resolveGatewayApiBase(apiBase?: string): string {
  const resolvedApiBase = trimTrailingSlash(
    apiBase ??
      process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL ??
      process.env.NEXT_PUBLIC_FYRALIS_API_BASE ??
      browserOrigin() ??
      ""
  );
  if (!resolvedApiBase) {
    throw new Error("Gateway API base is required.");
  }
  return resolvedApiBase;
}

function mapSlackRehearsalPrepare(
  payload: SlackRehearsalPrepareApiResponse
): SlackRehearsalPrepareResponse {
  return {
    enabled: payload.enabled,
    tenantId: payload.tenant_id,
    actorId: payload.actor_id,
    gatewayApiBase: payload.gateway_api_base,
    providerIngressUrl: payload.provider_ingress_url,
    oauthRedirectUrl: payload.oauth_redirect_url,
    eventsRequestUrl: payload.events_request_url,
    installUrl: payload.install_url,
    bearerToken: payload.bearer_token,
    sessionExpiresAt: payload.session_expires_at,
    stateExpiresInSeconds: payload.state_expires_in_seconds,
    status: mapSlackRehearsalStatus(payload.status)
  };
}

function mapSlackRehearsalStatus(
  payload: SlackRehearsalApiStatus
): SlackRehearsalStatus {
  return {
    installed: payload.installed,
    installation: payload.installation
      ? {
          installationId: payload.installation.installation_id,
          enabled: payload.installation.enabled,
          hasSecret: payload.installation.has_secret,
          installedAt: payload.installation.installed_at
        }
      : null,
    triggerCount: payload.trigger_count,
    consumedTriggerCount: payload.consumed_trigger_count,
    runStatusCounts: payload.run_status_counts,
    shardStateCounts: Object.fromEntries(
      Object.entries(payload.shard_state_counts).map(([state, value]) => [
        state,
        {
          count: value.count,
          observationsSeen: value.observations_seen
        }
      ])
    ),
    observationCount: payload.observation_count,
    observations: payload.observations
      .filter((item) => observationBelongsToSource(item, "slack"))
      .map((item) => gatewayObservationToSourceObservation(item, "slack")),
    unresolvedFailureCount: payload.unresolved_failure_count,
    bearerToken: payload.bearer_token,
    sessionExpiresAt: payload.session_expires_at,
    nextAction: payload.next_action
  };
}

function gatewayObservationToSourceObservation(
  observation: GatewayObservation,
  sourceId: string
): SourceObservation {
  const title = summarizeTitle(observation.content_text, observation.source_channel);
  return {
    id: observation.id,
    sourceId,
    title,
    kind: mapGatewayObservationKind(observation.kind, observation.source_channel),
    occurredAt: observation.occurred_at,
    summary:
      observation.content_text?.trim() ||
      `Gateway observation from ${observation.source_channel}.`,
    evidencePath: `gateway:/observations/${observation.id}`,
    status: "landed",
    origin: "gateway",
    syncTrack: "mixed",
    sourceChannel: observation.source_channel
  };
}

function summarizeTitle(contentText: string, sourceChannel: string): string {
  const trimmed = contentText?.trim();
  if (!trimmed) {
    return sourceChannel;
  }
  return trimmed.length > 72 ? `${trimmed.slice(0, 69)}...` : trimmed;
}

function mapGatewayObservationKind(
  kind: string,
  sourceChannel: string
): SourceObservation["kind"] {
  const lowerKind = kind.toLowerCase();
  const lowerChannel = sourceChannel.toLowerCase();
  if (lowerChannel.startsWith("github:")) {
    return lowerKind.includes("deploy") ? "deployment" : "pull-request";
  }
  if (lowerChannel.startsWith("jira:")) {
    return "issue";
  }
  if (lowerChannel.startsWith("notion:")) {
    return "page";
  }
  if (
    lowerChannel.startsWith("slack:") ||
    lowerChannel.startsWith("discord:") ||
    lowerChannel.startsWith("telegram:")
  ) {
    return "message";
  }
  if (lowerKind.includes("deploy")) {
    return "deployment";
  }
  if (lowerKind.includes("page") || lowerKind.includes("document")) {
    return "page";
  }
  return "task";
}

class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: string
  ) {
    super(`HTTP ${status}: ${body}`);
  }
}

function isOnboardingIntentNotFound(error: unknown): boolean {
  if (!(error instanceof ApiError) || error.status !== 404) {
    return false;
  }
  try {
    const body = JSON.parse(error.body) as { detail?: { error?: string } };
    return body.detail?.error === "onboarding_intent_not_found";
  } catch {
    return error.body.includes("onboarding_intent_not_found");
  }
}

function targetCloudToApi(
  targetCloud: Customer["targetCloud"]
): OnboardingIntent["target_cloud"] {
  if (targetCloud !== "AWS") {
    return "aws";
  }
  return "aws";
}

function mockIntent(
  override: {
    intentId?: string;
    status?: OnboardingIntent["status"];
    customerId?: string | null;
    tenantId?: string | null;
    deploymentId?: string | null;
    companyName?: string | null;
    setupOwnerEmail?: string | null;
    targetCloud?: OnboardingIntent["target_cloud"];
  } = {}
): OnboardingIntent {
  const now = new Date().toISOString();
  return {
    schema_version: "fyralis.platform.onboarding_intent.v1",
    intent_id: override.intentId ?? `ofi_${randomHex(16)}`,
    plan_code: "design_partner_byoc_pilot",
    procurement_channel: "design_partner",
    entrypoint: "get_fyralis",
    status: override.status ?? "draft",
    customer_id: override.customerId ?? null,
    tenant_id: override.tenantId ?? null,
    deployment_id: override.deploymentId ?? null,
    company_name: override.companyName ?? null,
    setup_owner_email: override.setupOwnerEmail ?? null,
    target_cloud: override.targetCloud ?? null,
    created_at: now,
    updated_at: now,
    stored_scope: "sanitized_onboarding_metadata_only"
  };
}

function randomHex(bytes: number): string {
  const buffer = new Uint8Array(bytes);
  globalThis.crypto?.getRandomValues(buffer);
  if (buffer.every((value) => value === 0)) {
    return Array.from({ length: bytes * 2 }, () =>
      Math.floor(Math.random() * 16).toString(16)
    ).join("");
  }
  return Array.from(buffer, (value) => value.toString(16).padStart(2, "0")).join("");
}

function randomUuid(): string {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }
  return "00000000-0000-7000-8000-" + randomHex(6);
}

function trimTrailingSlash(value: string): string {
  return value.trim().replace(/\/+$/, "");
}

function withRuntimeWorkspaceUrls(
  snapshot: OnboardingSnapshot
): OnboardingSnapshot {
  const apiBase = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_API_BASE ?? ""
  );
  const localConsoleUrl = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_LOCAL_CONSOLE_URL ??
      browserOrigin() ??
      snapshot.workspace.localConsoleUrl
  );
  const providerIngressUrl = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL ??
      (apiBase || snapshot.workspace.providerIngressUrl)
  );
  return {
    ...snapshot,
    workspace: {
      ...snapshot.workspace,
      localConsoleUrl,
      providerIngressUrl
    }
  };
}

function browserOrigin(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  return window.location.origin;
}
