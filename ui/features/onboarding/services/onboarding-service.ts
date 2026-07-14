import { ONBOARDING_SNAPSHOT } from "../data/mock-data";
import type {
  Customer,
  OnboardingIntent,
  OnboardingSnapshot,
  SourceObservation,
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
      entrypoint: "get_fyralis",
    },
    () => mockIntent(),
  );
}

export async function submitDesignPartnerIntake(
  intentId: string,
  customer: Customer,
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

type SourceAccessApiSummary = {
  total: number;
  ready: number;
  missing_access: number;
  needs_admin: number;
  not_selected: number;
  unknown: number;
  selected: number;
  observed: number;
};

type SourceAccessApiResource = {
  source: string;
  installation_id: string;
  installation_name?: string | null;
  resource_kind: string;
  resource_id: string;
  display_name: string;
  parent_id?: string | null;
  parent_name?: string | null;
  visibility?: "public" | "private" | "unknown" | null;
  permission_status:
    "ready" | "missing_access" | "needs_admin" | "not_selected" | "unknown";
  selected: boolean;
  can_backfill: boolean;
  can_receive_live: boolean;
  last_probe_at?: string | null;
  last_observation_at?: string | null;
  observation_count: number;
  diagnostics?: Record<string, unknown>;
};

type SourceRehearsalApiStatus = {
  source: string;
  installed: boolean;
  installation: {
    installation_id: string;
    enabled: boolean;
    has_secret: boolean;
    installed_at: string;
    details?: Record<string, unknown>;
  } | null;
  installations?: Array<{
    installation_id: string;
    enabled: boolean;
    has_secret: boolean;
    installed_at: string;
    details?: Record<string, unknown>;
  }>;
  trigger_count: number;
  consumed_trigger_count: number;
  run_status_counts: Record<string, number>;
  shard_state_counts: Record<
    string,
    { count: number; observations_seen: number }
  >;
  observation_count: number;
  sync_started_at?: string | null;
  observations: GatewayObservation[];
  unresolved_failure_count: number;
  latest_failure?: string | null;
  bearer_token?: string | null;
  session_expires_at?: string | null;
  auto_connect_run?: SourceAutoConnectRunApi | null;
  access_summary?: SourceAccessApiSummary;
  access_resources?: SourceAccessApiResource[];
  access_next_actions?: string[];
  next_action: string;
};

type SourceRehearsalPrepareApiResponse = {
  enabled: boolean;
  source: string;
  tenant_id: string;
  actor_id: string;
  gateway_api_base: string;
  provider_ingress_url: string;
  oauth_redirect_url?: string | null;
  events_request_url?: string | null;
  install_url?: string | null;
  discord_access_mode?: SourceConnectAccessMode | null;
  discord_permissions?: string | null;
  provider_console_url?: string | null;
  authorization_mode: string;
  missing_configuration: string[];
  required_inputs: string[];
  optional_inputs?: string[];
  finalize_mode?: SourceFinalizeMode;
  automation_profile?: SourceAutomationProfileApi;
  browser_agent?: SourceBrowserAgentRecipeApi;
  browser_agent_run?: SourceBrowserAgentRunApi;
  bearer_token: string;
  session_expires_at: string;
  state_expires_in_seconds?: number | null;
  status: SourceRehearsalApiStatus;
};

type SourceAutoConnectApiState = {
  state: "connected" | "running" | "admin_gate" | "blocked" | "error";
  label: string;
  message: string;
  human_step_count: number;
  human_steps: SourceAutomationProfileApi["human_steps"];
  automated_actions: string[];
  browser_agent?: SourceBrowserAgentRecipeApi;
  browser_agent_run?: SourceBrowserAgentRunApi;
  automation_run?: SourceAutoConnectRunApi;
  install_url?: string | null;
};

type SourceAutoConnectApiResponse = SourceRehearsalPrepareApiResponse & {
  auto_connect: SourceAutoConnectApiState;
};

type SourceAutomationProfileApi = {
  automation_level: string;
  method: string;
  minimum_human_inputs: string[];
  optional_hints: string[];
  automated_actions: string[];
  human_steps: Array<{
    id: string;
    label: string;
    reason: string;
    can_agent_complete: boolean;
  }>;
  agent_discovery_target: string;
  post_connect_actions: string[];
  human_step_count: number;
};

type SourceBrowserAgentRecipeApi = {
  source: string;
  provider_console_url: string;
  settings_targets: string[];
  agent_collects: string[];
  agent_generates: string[];
  human_gates: string[];
  completion_checks: string[];
};

type SourceBrowserAgentRunActionApi = {
  id: string;
  owner: "fyralis_agent" | "provider_admin";
  status: string;
  label: string;
  reason?: string;
};

type SourceBrowserAgentRunGateApi = {
  id: string;
  label: string;
  reason: string;
  status: string;
  can_agent_complete: boolean;
};

type SourceBrowserAgentRunCheckApi = {
  name: string;
  status: string;
};

type SourceBrowserAgentRunApi = {
  schema_version: string;
  source: string;
  state: "connected" | "running" | "waiting_for_admin" | "blocked";
  launch_mode: string;
  can_start: boolean;
  handoff_url?: string | null;
  handoff_kind: string;
  provider_console_url?: string | null;
  oauth_redirect_url?: string | null;
  events_request_url?: string | null;
  settings_targets: string[];
  agent_collects: string[];
  agent_generates: string[];
  human_gates: SourceBrowserAgentRunGateApi[];
  completion_checks: SourceBrowserAgentRunCheckApi[];
  action_queue: SourceBrowserAgentRunActionApi[];
  current_action?: SourceBrowserAgentRunActionApi | null;
  automated_action_count: number;
  human_action_count: number;
};

export type SourceFinalizeMode =
  | "provider_callback"
  | "source_specific"
  | "native_finalizer_required"
  | "generic_customer_refs";

export type SourceRehearsalStatus = {
  sourceId: string;
  installed: boolean;
  installation: SourceInstallation | null;
  installations: SourceInstallation[];
  triggerCount: number;
  consumedTriggerCount: number;
  runStatusCounts: Record<string, number>;
  shardStateCounts: Record<string, { count: number; observationsSeen: number }>;
  observationCount: number;
  syncStartedAt: string | null;
  observations: SourceObservation[];
  unresolvedFailureCount: number;
  latestFailure: string | null;
  bearerToken?: string | null;
  sessionExpiresAt?: string | null;
  autoConnectRun: SourceAutoConnectRun | null;
  accessSummary: SourceAccessSummary;
  accessResources: SourceAccessResource[];
  accessNextActions: string[];
  nextAction: string;
};

export type SourceInstallation = {
  installationId: string;
  enabled: boolean;
  hasSecret: boolean;
  installedAt: string;
  details: Record<string, unknown>;
};

export type SourceAccessSummary = {
  total: number;
  ready: number;
  missingAccess: number;
  needsAdmin: number;
  notSelected: number;
  unknown: number;
  selected: number;
  observed: number;
};

export type SourceAccessResource = {
  sourceId: string;
  installationId: string;
  installationName: string | null;
  resourceKind: string;
  resourceId: string;
  displayName: string;
  parentId: string | null;
  parentName: string | null;
  visibility: "public" | "private" | "unknown";
  permissionStatus:
    "ready" | "missing_access" | "needs_admin" | "not_selected" | "unknown";
  selected: boolean;
  canBackfill: boolean;
  canReceiveLive: boolean;
  lastProbeAt: string | null;
  lastObservationAt: string | null;
  observationCount: number;
  diagnostics: Record<string, unknown>;
};

export type SourceRehearsalPrepareResponse = {
  enabled: boolean;
  sourceId: string;
  tenantId: string;
  actorId: string;
  gatewayApiBase: string;
  providerIngressUrl: string;
  oauthRedirectUrl: string | null;
  eventsRequestUrl: string | null;
  installUrl: string | null;
  discordAccessMode: SourceConnectAccessMode | null;
  discordPermissions: string | null;
  providerConsoleUrl: string | null;
  authorizationMode: string;
  missingConfiguration: string[];
  requiredInputs: string[];
  optionalInputs: string[];
  finalizeMode: SourceFinalizeMode;
  automationProfile: SourceAutomationProfile;
  browserAgent: SourceBrowserAgentRecipe | null;
  browserAgentRun: SourceBrowserAgentRun | null;
  bearerToken: string;
  sessionExpiresAt: string;
  stateExpiresInSeconds: number | null;
  status: SourceRehearsalStatus;
};

export type SourceAutoConnectState = {
  state: "connected" | "running" | "admin_gate" | "blocked" | "error";
  label: string;
  message: string;
  humanStepCount: number;
  humanSteps: SourceAutomationProfile["humanSteps"];
  automatedActions: string[];
  browserAgent: SourceBrowserAgentRecipe | null;
  browserAgentRun: SourceBrowserAgentRun | null;
  automationRun: SourceAutoConnectRun | null;
  installUrl: string | null;
};

export type SourceAutoConnectResponse = SourceRehearsalPrepareResponse & {
  autoConnect: SourceAutoConnectState;
};

export type SourceConnectAccessMode = "standard" | "full_server_sync";

export type SourceAutomationProfile = {
  automationLevel: string;
  method: string;
  minimumHumanInputs: string[];
  optionalHints: string[];
  automatedActions: string[];
  humanSteps: Array<{
    id: string;
    label: string;
    reason: string;
    canAgentComplete: boolean;
  }>;
  agentDiscoveryTarget: string;
  postConnectActions: string[];
  humanStepCount: number;
};

export type SourceBrowserAgentRecipe = {
  sourceId: string;
  providerConsoleUrl: string;
  settingsTargets: string[];
  agentCollects: string[];
  agentGenerates: string[];
  humanGates: string[];
  completionChecks: string[];
};

export type SourceBrowserAgentRunAction = {
  id: string;
  owner: "fyralis_agent" | "provider_admin";
  status: string;
  label: string;
  reason?: string;
};

export type SourceBrowserAgentRunGate = {
  id: string;
  label: string;
  reason: string;
  status: string;
  canAgentComplete: boolean;
};

export type SourceBrowserAgentRun = {
  schemaVersion: string;
  sourceId: string;
  state: "connected" | "running" | "waiting_for_admin" | "blocked";
  launchMode: string;
  canStart: boolean;
  handoffUrl: string | null;
  handoffKind: string;
  providerConsoleUrl: string | null;
  oauthRedirectUrl: string | null;
  eventsRequestUrl: string | null;
  settingsTargets: string[];
  agentCollects: string[];
  agentGenerates: string[];
  humanGates: SourceBrowserAgentRunGate[];
  completionChecks: Array<{ name: string; status: string }>;
  actionQueue: SourceBrowserAgentRunAction[];
  currentAction: SourceBrowserAgentRunAction | null;
  automatedActionCount: number;
  humanActionCount: number;
};

type SourceAutoConnectRunApi = {
  schema_version: string;
  source: string;
  status: string;
  launch_mode: string;
  can_start: boolean;
  handoff_url?: string | null;
  current_action_id?: string | null;
  automated_action_count: number;
  human_action_count: number;
  native_connect_kind?: string | null;
  native_payload_template_path_hint?: string | null;
  provider_setup_output_dir_hint?: string | null;
  receipt_path_hint?: string | null;
  background_status?: string | null;
  background_queued_at?: string | null;
  background_started_at?: string | null;
  background_finished_at?: string | null;
  background_runner_mode?: string | null;
  run_artifact_path_hint?: string | null;
  command_preview: string;
  command_args: string[];
};

export type SourceAutoConnectRun = {
  schemaVersion: string;
  sourceId: string;
  status: string;
  launchMode: string;
  canStart: boolean;
  handoffUrl: string | null;
  currentActionId: string | null;
  automatedActionCount: number;
  humanActionCount: number;
  nativeConnectKind: string | null;
  nativePayloadTemplatePathHint: string | null;
  providerSetupOutputDirHint: string | null;
  receiptPathHint: string | null;
  backgroundStatus: string | null;
  backgroundQueuedAt: string | null;
  backgroundStartedAt: string | null;
  backgroundFinishedAt: string | null;
  backgroundRunnerMode: string | null;
  runArtifactPathHint: string | null;
  commandPreview: string;
  commandArgs: string[];
};

export async function fetchGatewaySourceObservations({
  apiBase,
  bearerToken,
  sourceId,
  limit = 50,
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
      "",
  );
  if (!resolvedApiBase) {
    throw new Error("Gateway API base is required.");
  }

  const query = new URLSearchParams({
    limit: String(limit),
    source: gatewaySourceId(sourceId),
  });
  const response = await fetch(`${resolvedApiBase}/observations?${query}`, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${token}`,
    },
  });
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }

  const payload = (await response.json()) as GatewayObservationResponse;
  return payload.items
    .filter((item) => observationBelongsToSource(item, sourceId))
    .map((item) => gatewayObservationToSourceObservation(item, sourceId));
}

export async function autoConnectSourceRehearsal({
  sourceId,
  apiBase,
  deploymentContext,
  accessMode,
}: {
  sourceId: string;
  apiBase?: string;
  deploymentContext?: {
    awsRegion?: string;
    awsAssumingPrincipalArn?: string;
  };
  accessMode?: SourceConnectAccessMode;
}): Promise<SourceAutoConnectResponse> {
  const resolvedApiBase = resolveGatewayApiBase(apiBase);
  const requestBody: Record<string, unknown> = {};
  if (deploymentContext?.awsRegion || deploymentContext?.awsAssumingPrincipalArn) {
    requestBody.deployment_context = {
      aws_region: deploymentContext.awsRegion,
      aws_assuming_principal_arn: deploymentContext.awsAssumingPrincipalArn,
    };
  }
  if (accessMode) {
    requestBody.access_mode = accessMode;
  }
  const hasRequestBody = Object.keys(requestBody).length > 0;
  const response = await fetch(
    `${resolvedApiBase}/platform/onboarding/sources/${encodeURIComponent(
      gatewaySourceId(sourceId),
    )}/rehearsal/auto-connect`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        ...(hasRequestBody ? { "Content-Type": "application/json" } : {}),
      },
      ...(hasRequestBody ? { body: JSON.stringify(requestBody) } : {}),
    },
  );
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }
  return mapSourceAutoConnect(
    (await response.json()) as SourceAutoConnectApiResponse,
    sourceId,
  );
}

export async function fetchSourceRehearsalStatus({
  sourceId,
  apiBase,
}: {
  sourceId: string;
  apiBase?: string;
}): Promise<SourceRehearsalStatus> {
  const resolvedApiBase = resolveGatewayApiBase(apiBase);
  const response = await fetch(
    `${resolvedApiBase}/platform/onboarding/sources/${encodeURIComponent(
      gatewaySourceId(sourceId),
    )}/rehearsal/status`,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
    },
  );
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }
  return mapSourceRehearsalStatus(
    (await response.json()) as SourceRehearsalApiStatus,
    sourceId,
  );
}

export async function finalizeAwsSourceRehearsal({
  apiBase,
  roleArn,
  region,
}: {
  apiBase?: string;
  roleArn: string;
  region?: string;
}): Promise<SourceRehearsalStatus> {
  const resolvedApiBase = resolveGatewayApiBase(apiBase);
  const response = await fetch(
    `${resolvedApiBase}/platform/onboarding/sources/aws/rehearsal/finalize`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        role_arn: roleArn,
        region: region || "us-east-1",
        credential_kind: "assume_role",
        backfill_window_days: 90,
      }),
    },
  );
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }
  const payload = (await response.json()) as {
    status: SourceRehearsalApiStatus;
  };
  return mapSourceRehearsalStatus(payload.status, "aws");
}

export async function retryAwsFirstSyncRehearsal({
  apiBase,
}: {
  apiBase?: string;
}): Promise<SourceRehearsalStatus> {
  const resolvedApiBase = resolveGatewayApiBase(apiBase);
  const response = await fetch(
    `${resolvedApiBase}/platform/onboarding/sources/aws/rehearsal/retry`,
    {
      method: "POST",
      headers: {
        Accept: "application/json",
      },
    },
  );
  if (!response.ok) {
    throw new ApiError(response.status, await response.text());
  }
  const payload = (await response.json()) as {
    status: SourceRehearsalApiStatus;
  };
  return mapSourceRehearsalStatus(payload.status, "aws");
}

async function postJson<T>(
  path: string,
  body: unknown,
  fallback: () => T,
): Promise<T> {
  const apiBase = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_API_BASE ?? "",
  );
  const allowLocalFallback = apiBase === "";
  try {
    const response = await fetch(`${apiBase}${path}`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
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
  customer: Customer,
): Promise<OnboardingIntent> {
  return postJson<OnboardingIntent>(
    `/platform/onboarding/intents/${intentId}/design-partner-intake`,
    {
      company_name: customer.company,
      setup_owner_email: customer.setupOwnerEmail,
      target_cloud: targetCloudToApi(customer.targetCloud),
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
        deploymentId: `dep_${randomHex(8)}`,
      }),
  );
}

function observationBelongsToSource(
  observation: GatewayObservation,
  sourceId: string,
): boolean {
  const sourceChannel = observation.source_channel?.toLowerCase() ?? "";
  const normalized = sourceId.toLowerCase().replaceAll("-", "_");
  return (
    sourceChannel === normalized ||
    sourceChannel.startsWith(`${normalized}:`) ||
    sourceChannel.startsWith(`${sourceId.toLowerCase()}:`) ||
    sourceChannel.startsWith(`gateway:${normalized}:`) ||
    sourceChannel.startsWith(`gateway:${sourceId.toLowerCase()}:`)
  );
}

function resolveGatewayApiBase(apiBase?: string): string {
  const resolvedApiBase = trimTrailingSlash(
    apiBase ??
      process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL ??
      process.env.NEXT_PUBLIC_FYRALIS_API_BASE ??
      browserOrigin() ??
      "",
  );
  if (!resolvedApiBase) {
    throw new Error("Gateway API base is required.");
  }
  return resolvedApiBase;
}

function mapSourceRehearsalPrepare(
  payload: SourceRehearsalPrepareApiResponse,
  fallbackSourceId: string,
): SourceRehearsalPrepareResponse {
  const sourceId = fallbackSourceId;
  return {
    enabled: payload.enabled,
    sourceId,
    tenantId: payload.tenant_id,
    actorId: payload.actor_id,
    gatewayApiBase: payload.gateway_api_base,
    providerIngressUrl: payload.provider_ingress_url,
    oauthRedirectUrl: payload.oauth_redirect_url ?? null,
    eventsRequestUrl: payload.events_request_url ?? null,
    installUrl: payload.install_url ?? null,
    discordAccessMode: mapSourceConnectAccessMode(payload.discord_access_mode),
    discordPermissions: payload.discord_permissions ?? null,
    providerConsoleUrl: payload.provider_console_url ?? null,
    authorizationMode: payload.authorization_mode,
    missingConfiguration: payload.missing_configuration ?? [],
    requiredInputs: payload.required_inputs ?? [],
    optionalInputs: payload.optional_inputs ?? [],
    finalizeMode: payload.finalize_mode ?? "generic_customer_refs",
    automationProfile: mapSourceAutomationProfile(
      payload.automation_profile,
      sourceId,
    ),
    browserAgent: mapSourceBrowserAgentRecipe(payload.browser_agent),
    browserAgentRun: mapSourceBrowserAgentRun(payload.browser_agent_run),
    bearerToken: payload.bearer_token,
    sessionExpiresAt: payload.session_expires_at,
    stateExpiresInSeconds: payload.state_expires_in_seconds ?? null,
    status: mapSourceRehearsalStatus(payload.status, sourceId),
  };
}

function mapSourceConnectAccessMode(
  value: string | null | undefined,
): SourceConnectAccessMode | null {
  if (value === "full_server_sync") {
    return "full_server_sync";
  }
  if (value === "standard") {
    return "standard";
  }
  return null;
}

function mapSourceAutoConnect(
  payload: SourceAutoConnectApiResponse,
  fallbackSourceId: string,
): SourceAutoConnectResponse {
  const prepared = mapSourceRehearsalPrepare(payload, fallbackSourceId);
  const autoConnect = payload.auto_connect;
  return {
    ...prepared,
    autoConnect: {
      state: autoConnect.state,
      label: autoConnect.label,
      message: autoConnect.message,
      humanStepCount: autoConnect.human_step_count ?? 0,
      humanSteps: (autoConnect.human_steps ?? []).map((step) => ({
        id: step.id,
        label: step.label,
        reason: step.reason,
        canAgentComplete: step.can_agent_complete,
      })),
      automatedActions: autoConnect.automated_actions ?? [],
      browserAgent: mapSourceBrowserAgentRecipe(
        autoConnect.browser_agent ?? payload.browser_agent,
      ),
      browserAgentRun: mapSourceBrowserAgentRun(
        autoConnect.browser_agent_run ?? payload.browser_agent_run,
      ),
      automationRun: mapSourceAutoConnectRun(autoConnect.automation_run),
      installUrl: autoConnect.install_url ?? null,
    },
  };
}

function mapSourceAutoConnectRun(
  payload: SourceAutoConnectRunApi | undefined,
): SourceAutoConnectRun | null {
  if (!payload) {
    return null;
  }
  return {
    schemaVersion: payload.schema_version,
    sourceId: payload.source,
    status: payload.status,
    launchMode: payload.launch_mode,
    canStart: payload.can_start,
    handoffUrl: payload.handoff_url ?? null,
    currentActionId: payload.current_action_id ?? null,
    automatedActionCount: payload.automated_action_count ?? 0,
    humanActionCount: payload.human_action_count ?? 0,
    nativeConnectKind: payload.native_connect_kind ?? null,
    nativePayloadTemplatePathHint:
      payload.native_payload_template_path_hint ?? null,
    providerSetupOutputDirHint: payload.provider_setup_output_dir_hint ?? null,
    receiptPathHint: payload.receipt_path_hint ?? null,
    backgroundStatus: payload.background_status ?? null,
    backgroundQueuedAt: payload.background_queued_at ?? null,
    backgroundStartedAt: payload.background_started_at ?? null,
    backgroundFinishedAt: payload.background_finished_at ?? null,
    backgroundRunnerMode: payload.background_runner_mode ?? null,
    runArtifactPathHint: payload.run_artifact_path_hint ?? null,
    commandPreview: payload.command_preview,
    commandArgs: payload.command_args ?? [],
  };
}

function mapSourceBrowserAgentRun(
  payload: SourceBrowserAgentRunApi | undefined,
): SourceBrowserAgentRun | null {
  if (!payload) {
    return null;
  }
  const mapAction = (
    action: SourceBrowserAgentRunActionApi,
  ): SourceBrowserAgentRunAction => ({
    id: action.id,
    owner: action.owner,
    status: action.status,
    label: action.label,
    reason: action.reason,
  });
  return {
    schemaVersion: payload.schema_version,
    sourceId: payload.source,
    state: payload.state,
    launchMode: payload.launch_mode,
    canStart: payload.can_start,
    handoffUrl: payload.handoff_url ?? null,
    handoffKind: payload.handoff_kind,
    providerConsoleUrl: payload.provider_console_url ?? null,
    oauthRedirectUrl: payload.oauth_redirect_url ?? null,
    eventsRequestUrl: payload.events_request_url ?? null,
    settingsTargets: payload.settings_targets ?? [],
    agentCollects: payload.agent_collects ?? [],
    agentGenerates: payload.agent_generates ?? [],
    humanGates: (payload.human_gates ?? []).map((gate) => ({
      id: gate.id,
      label: gate.label,
      reason: gate.reason,
      status: gate.status,
      canAgentComplete: gate.can_agent_complete,
    })),
    completionChecks: payload.completion_checks ?? [],
    actionQueue: (payload.action_queue ?? []).map(mapAction),
    currentAction: payload.current_action
      ? mapAction(payload.current_action)
      : null,
    automatedActionCount: payload.automated_action_count ?? 0,
    humanActionCount: payload.human_action_count ?? 0,
  };
}

function mapSourceBrowserAgentRecipe(
  payload: SourceBrowserAgentRecipeApi | undefined,
): SourceBrowserAgentRecipe | null {
  if (!payload) {
    return null;
  }
  return {
    sourceId: payload.source,
    providerConsoleUrl: payload.provider_console_url,
    settingsTargets: payload.settings_targets ?? [],
    agentCollects: payload.agent_collects ?? [],
    agentGenerates: payload.agent_generates ?? [],
    humanGates: payload.human_gates ?? [],
    completionChecks: payload.completion_checks ?? [],
  };
}

function mapSourceAutomationProfile(
  payload: SourceAutomationProfileApi | undefined,
  sourceId: string,
): SourceAutomationProfile {
  if (!payload) {
    return {
      automationLevel: "automated_after_customer_authorization",
      method: "unknown",
      minimumHumanInputs: [],
      optionalHints: [],
      automatedActions: ["prepare source handoff", "register source metadata"],
      humanSteps: [
        {
          id: "approve_source_connection",
          label: `Approve ${sourceId} connection.`,
          reason: "The source owner must approve the boundary and scope.",
          canAgentComplete: false,
        },
      ],
      agentDiscoveryTarget: "approved source scope",
      postConnectActions: ["poll for observations"],
      humanStepCount: 1,
    };
  }
  return {
    automationLevel: payload.automation_level,
    method: payload.method,
    minimumHumanInputs: payload.minimum_human_inputs ?? [],
    optionalHints: payload.optional_hints ?? [],
    automatedActions: payload.automated_actions ?? [],
    humanSteps: (payload.human_steps ?? []).map((step) => ({
      id: step.id,
      label: step.label,
      reason: step.reason,
      canAgentComplete: step.can_agent_complete,
    })),
    agentDiscoveryTarget: payload.agent_discovery_target,
    postConnectActions: payload.post_connect_actions ?? [],
    humanStepCount:
      payload.human_step_count ?? payload.human_steps?.length ?? 0,
  };
}

function mapSourceRehearsalStatus(
  payload: SourceRehearsalApiStatus,
  fallbackSourceId: string,
): SourceRehearsalStatus {
  const sourceId = fallbackSourceId;
  const installations = (payload.installations ?? [])
    .map(mapSourceInstallation)
    .filter(Boolean) as SourceInstallation[];
  const primaryInstallation = payload.installation
    ? mapSourceInstallation(payload.installation)
    : (installations[0] ?? null);
  return {
    sourceId,
    installed: payload.installed,
    installation: primaryInstallation,
    installations:
      installations.length || !primaryInstallation
        ? installations
        : [primaryInstallation],
    triggerCount: payload.trigger_count,
    consumedTriggerCount: payload.consumed_trigger_count,
    runStatusCounts: payload.run_status_counts,
    shardStateCounts: Object.fromEntries(
      Object.entries(payload.shard_state_counts).map(([state, value]) => [
        state,
        {
          count: value.count,
          observationsSeen: value.observations_seen,
        },
      ]),
    ),
    observationCount: payload.observation_count,
    syncStartedAt: payload.sync_started_at ?? null,
    observations: payload.observations
      .filter((item) => observationBelongsToSource(item, sourceId))
      .map((item) => gatewayObservationToSourceObservation(item, sourceId)),
    unresolvedFailureCount: payload.unresolved_failure_count,
    latestFailure: payload.latest_failure ?? null,
    bearerToken: payload.bearer_token,
    sessionExpiresAt: payload.session_expires_at,
    autoConnectRun: mapSourceAutoConnectRun(
      payload.auto_connect_run ?? undefined,
    ),
    accessSummary: mapSourceAccessSummary(payload.access_summary),
    accessResources: (payload.access_resources ?? []).map(
      mapSourceAccessResource,
    ),
    accessNextActions: payload.access_next_actions ?? [],
    nextAction: payload.next_action,
  };
}

function mapSourceInstallation(payload: {
  installation_id: string;
  enabled: boolean;
  has_secret: boolean;
  installed_at: string;
  details?: Record<string, unknown>;
}): SourceInstallation {
  return {
    installationId: payload.installation_id,
    enabled: payload.enabled,
    hasSecret: payload.has_secret,
    installedAt: payload.installed_at,
    details: payload.details ?? {},
  };
}

function mapSourceAccessSummary(
  summary: SourceAccessApiSummary | undefined,
): SourceAccessSummary {
  return {
    total: summary?.total ?? 0,
    ready: summary?.ready ?? 0,
    missingAccess: summary?.missing_access ?? 0,
    needsAdmin: summary?.needs_admin ?? 0,
    notSelected: summary?.not_selected ?? 0,
    unknown: summary?.unknown ?? 0,
    selected: summary?.selected ?? 0,
    observed: summary?.observed ?? 0,
  };
}

function mapSourceAccessResource(
  resource: SourceAccessApiResource,
): SourceAccessResource {
  return {
    sourceId: uiSourceId(resource.source),
    installationId: resource.installation_id,
    installationName: resource.installation_name ?? null,
    resourceKind: resource.resource_kind,
    resourceId: resource.resource_id,
    displayName: resource.display_name,
    parentId: resource.parent_id ?? null,
    parentName: resource.parent_name ?? null,
    visibility: resource.visibility ?? "unknown",
    permissionStatus: resource.permission_status,
    selected: resource.selected,
    canBackfill: resource.can_backfill,
    canReceiveLive: resource.can_receive_live,
    lastProbeAt: resource.last_probe_at ?? null,
    lastObservationAt: resource.last_observation_at ?? null,
    observationCount: resource.observation_count,
    diagnostics: resource.diagnostics ?? {},
  };
}

function gatewayObservationToSourceObservation(
  observation: GatewayObservation,
  sourceId: string,
): SourceObservation {
  const title = summarizeTitle(
    observation.content_text,
    observation.source_channel,
  );
  return {
    id: observation.id,
    sourceId,
    title,
    kind: mapGatewayObservationKind(
      observation.kind,
      observation.source_channel,
    ),
    occurredAt: observation.occurred_at,
    summary:
      observation.content_text?.trim() ||
      `Gateway observation from ${observation.source_channel}.`,
    evidencePath: `gateway:/observations/${observation.id}`,
    status: "landed",
    origin: "gateway",
    syncTrack: "mixed",
    sourceChannel: observation.source_channel,
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
  sourceChannel: string,
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
    lowerChannel.startsWith("telegram:") ||
    lowerChannel.startsWith("signal:") ||
    lowerChannel.startsWith("whatsapp:")
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
    readonly body: string,
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
  targetCloud: Customer["targetCloud"],
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
  } = {},
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
    stored_scope: "sanitized_onboarding_metadata_only",
  };
}

function randomHex(bytes: number): string {
  const buffer = new Uint8Array(bytes);
  globalThis.crypto?.getRandomValues(buffer);
  if (buffer.every((value) => value === 0)) {
    return Array.from({ length: bytes * 2 }, () =>
      Math.floor(Math.random() * 16).toString(16),
    ).join("");
  }
  return Array.from(buffer, (value) =>
    value.toString(16).padStart(2, "0"),
  ).join("");
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

function gatewaySourceId(sourceId: string): string {
  return sourceId.trim().toLowerCase().replaceAll("-", "_");
}

function uiSourceId(sourceId: string): string {
  return sourceId.trim().toLowerCase().replaceAll("_", "-");
}

function withRuntimeWorkspaceUrls(
  snapshot: OnboardingSnapshot,
): OnboardingSnapshot {
  const apiBase = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_API_BASE ?? "",
  );
  const localConsoleUrl = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_LOCAL_CONSOLE_URL ??
      browserOrigin() ??
      snapshot.workspace.localConsoleUrl,
  );
  const providerIngressUrl = trimTrailingSlash(
    process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL ??
      (apiBase || snapshot.workspace.providerIngressUrl),
  );
  return {
    ...snapshot,
    workspace: {
      ...snapshot.workspace,
      localConsoleUrl,
      providerIngressUrl,
    },
  };
}

function browserOrigin(): string | null {
  if (typeof window === "undefined") {
    return null;
  }
  return window.location.origin;
}
