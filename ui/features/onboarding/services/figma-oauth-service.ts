/**
 * Browser client for the native Figma OAuth onboarding flow.
 *
 * The only credential this module accepts is an optional, short-lived gateway
 * bearer token supplied by the authenticated customer-cloud session. It is
 * deliberately never written to browser storage. Figma credentials are owned
 * by the gateway and are never present in this client.
 */

export type FigmaConnectionState =
  | "deployment_setup_required"
  | "not_connected"
  | "ready_for_provider_approval"
  | "authorizing"
  | "finalizing"
  | "syncing"
  | "connected"
  | "degraded"
  | "reauthorization_required"
  | "error"
  | "disconnected"
  | "multiple_installations"
  | "unknown";

export type FigmaOAuthStartRequest = {
  fileUrls: string[];
  /**
   * A same-origin, token-free return path. The gateway adds only safe OAuth
   * outcome fields (for example `figma=connected`) after the callback.
   */
  returnPath?: string;
};

export type FigmaOAuthStartResponse = {
  ok: boolean;
  state: FigmaConnectionState;
  authorizationUrl: string;
  installUrl: string | null;
  stateExpiresInSeconds: number | null;
  requestedFileCount: number;
};

export type FigmaConnectionFile = {
  fileKey: string;
  fileName: string | null;
  projectName: string | null;
  state: string | null;
  latestVersion: string | null;
  lastSyncedAt: string | null;
  observationCount: number;
};

export type FigmaConnectionObservation = {
  id: string;
  kind: string;
  occurredAt: string | null;
  contentText: string | null;
  artifactId: string | null;
};

export type FigmaConnectionStatus = {
  ok: boolean;
  state: FigmaConnectionState;
  /**
   * The only setup-owner value the browser needs to understand. It is a role,
   * never an identity, credential, callback URL, or configuration detail.
   */
  setupOwner: "deployment_admin" | null;
  installationId: string | null;
  installationIds: string[];
  installationSelectionRequired: boolean;
  fileCount: number;
  selectedFileCount: number;
  syncedFileCount: number;
  failedFileCount: number;
  observationCount: number;
  latestObservationAt: string | null;
  latestObservation: FigmaConnectionObservation | null;
  files: FigmaConnectionFile[];
  lastError: string | null;
  nextAction: string | null;
};

export type FigmaRetryResponse = FigmaConnectionStatus & {
  retryQueued: boolean;
};

export type FigmaDisconnectResponse = {
  ok: boolean;
  state: FigmaConnectionState;
};

export type FigmaOAuthClientOptions = {
  /** Customer-cloud gateway origin. Defaults to the configured browser origin. */
  apiBase?: string;
  /**
   * Optional short-lived authenticated gateway token. Do not derive this from
   * localStorage; production should normally rely on an authenticated cookie.
   */
  gatewayToken?: string | null;
  signal?: AbortSignal;
};

export class FigmaOAuthApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: string,
  ) {
    super(errorMessageFromBody(body) ?? `Figma connection request failed (${status}).`);
    this.name = "FigmaOAuthApiError";
  }
}

/** Starts Figma authorization. Navigate the top-level browser to authorizationUrl. */
export async function startFigmaOAuth(
  request: FigmaOAuthStartRequest,
  options: FigmaOAuthClientOptions = {},
): Promise<FigmaOAuthStartResponse> {
  const fileUrls = normalizeFileUrls(request.fileUrls);
  if (!fileUrls.length) {
    throw new Error("Add at least one Figma file URL.");
  }

  const payload = await requestJson<Record<string, unknown>>(
    "/integrations/figma/oauth/start",
    {
      method: "POST",
      body: {
        file_urls: fileUrls,
        ...(request.returnPath ? { return_path: request.returnPath } : {}),
      },
    },
    options,
  );
  const authorizationUrl = stringValue(
    payload.authorization_url ?? payload.authorizationUrl,
  );
  if (!authorizationUrl) {
    throw new Error("Figma did not return an authorization URL.");
  }

  return {
    ok: booleanValue(payload.ok, true),
    state: connectionState(payload.state),
    authorizationUrl,
    installUrl: nullableString(payload.install_url ?? payload.installUrl),
    stateExpiresInSeconds: nullableNumber(
      payload.state_expires_in_seconds ?? payload.stateExpiresInSeconds,
    ),
    requestedFileCount: numberValue(
      payload.requested_file_count ?? payload.requestedFileCount,
      fileUrls.length,
    ),
  };
}

/** Returns only sanitized installation, sync, and observation proof. */
export async function fetchFigmaConnectionStatus(
  options: FigmaOAuthClientOptions = {},
  installationId?: string,
): Promise<FigmaConnectionStatus> {
  const payload = await requestJson<Record<string, unknown>>(
    figmaInstallationPath(
      "/integrations/figma/connect/status",
      installationId,
    ),
    { method: "GET" },
    options,
  );
  return mapFigmaConnectionStatus(payload);
}

/** Queues a retry/reconciliation without asking the browser for credentials. */
export async function retryFigmaConnection(
  installationId: string,
  options: FigmaOAuthClientOptions = {},
): Promise<FigmaRetryResponse> {
  const payload = await requestJson<Record<string, unknown>>(
    figmaInstallationPath(
      "/integrations/figma/connect/retry",
      installationId,
    ),
    { method: "POST" },
    options,
  );
  return {
    ...mapFigmaConnectionStatus(payload),
    retryQueued: booleanValue(payload.retry_queued ?? payload.retryQueued, true),
  };
}

/** Removes the tenant's Figma connection; no Figma access token crosses the UI. */
export async function disconnectFigmaConnection(
  installationId: string,
  options: FigmaOAuthClientOptions = {},
): Promise<FigmaDisconnectResponse> {
  const payload = await requestJson<Record<string, unknown>>(
    figmaInstallationPath("/integrations/figma/connect", installationId),
    { method: "DELETE" },
    options,
  );
  return {
    ok: booleanValue(payload.ok, true),
    state: connectionState(payload.state),
  };
}

/**
 * Normalizes pasted URL input while keeping a Figma URL rather than extracting
 * a key. The gateway remains the authority for validating permissions/scope.
 */
export function normalizeFileUrls(values: string[]): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const rawValue of values) {
    for (const candidate of rawValue.split(/[\n,]/)) {
      const value = candidate.trim();
      if (!value) {
        continue;
      }
      const canonical = canonicalFigmaFileUrl(value);
      if (!canonical) {
        throw new Error(`“${value}” is not a valid Figma file URL.`);
      }
      if (!seen.has(canonical)) {
        seen.add(canonical);
        normalized.push(canonical);
      }
    }
  }
  return normalized;
}

/** Accepts the Figma document URL types supported by the native OAuth gateway. */
export function canonicalFigmaFileUrl(value: string): string | null {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  const hostname = url.hostname.toLowerCase();
  if (
    url.protocol !== "https:" ||
    (hostname !== "figma.com" && !hostname.endsWith(".figma.com"))
  ) {
    return null;
  }
  const parts = url.pathname.split("/").filter(Boolean);
  const kindIndex = parts.findIndex((part) =>
    ["file", "design", "proto", "board", "slides"].includes(
      part.toLowerCase(),
    ),
  );
  const fileKey = kindIndex >= 0 ? parts[kindIndex + 1] : undefined;
  if (!fileKey || !/^[A-Za-z0-9_-]{6,256}$/.test(fileKey)) {
    return null;
  }
  // Fragments can contain a node id but do not identify a distinct Figma file.
  return `https://www.figma.com/${parts[kindIndex].toLowerCase()}/${fileKey}`;
}

export function defaultFigmaOAuthReturnPath(): string | undefined {
  if (typeof window === "undefined") {
    return undefined;
  }
  const url = new URL(window.location.href);
  url.searchParams.delete("figma");
  url.searchParams.delete("figma_installation_id");
  url.searchParams.delete("figma_error");
  url.searchParams.delete("figma_skipped_files");
  url.searchParams.delete("code");
  url.searchParams.delete("error");
  url.searchParams.delete("error_description");
  url.searchParams.delete("state");
  url.searchParams.delete("access_token");
  url.searchParams.delete("id_token");
  url.searchParams.delete("token");
  return `${url.pathname}${url.search}${url.hash}`;
}

function mapFigmaConnectionStatus(
  payload: Record<string, unknown>,
): FigmaConnectionStatus {
  const sync = recordValue(payload.sync);
  const observation = recordValue(
    payload.latest_observation ?? payload.latestObservation,
  );
  const files = arrayValue(payload.files).map(mapFigmaConnectionFile);
  const installationIds = arrayValue(payload.installations)
    .map((value) =>
      nullableString(
        recordValue(value).installation_id ??
          recordValue(value).installationId,
      ),
    )
    .filter((value): value is string => Boolean(value));
  const fileCount = numberValue(
    payload.file_count ?? payload.fileCount,
    files.length,
  );
  const observationCount = numberValue(
    payload.observation_count ?? payload.observationCount ?? sync.observation_count ?? sync.observationCount,
    0,
  );
  const lastError = nullableString(payload.last_error ?? payload.lastError);
  const reportedState = connectionState(payload.state);
  // Some worker versions retain the durable `pending` marker after the first
  // shard completes. A landed observation is stronger customer-facing proof
  // than that stale queue marker, while a reported error still wins.
  const state =
    reportedState === "syncing" && observationCount > 0 && !lastError
      ? "connected"
      : reportedState;
  return {
    ok: booleanValue(payload.ok, true),
    state,
    setupOwner: setupOwner(payload.setup_owner ?? payload.setupOwner),
    installationId: nullableString(
      payload.installation_id ?? payload.installationId,
    ),
    installationIds,
    installationSelectionRequired: booleanValue(
      payload.installation_selection_required ??
        payload.installationSelectionRequired,
      installationIds.length > 1,
    ),
    fileCount,
    selectedFileCount: numberValue(
      payload.selected_file_count ?? payload.selectedFileCount,
      fileCount,
    ),
    syncedFileCount: numberValue(
      payload.synced_file_count ?? payload.syncedFileCount ?? sync.synced_file_count ?? sync.syncedFileCount,
      0,
    ),
    failedFileCount: numberValue(
      payload.failed_file_count ?? payload.failedFileCount ?? sync.failed_file_count ?? sync.failedFileCount,
      0,
    ),
    observationCount,
    latestObservationAt: nullableString(
      payload.latest_observation_at ?? payload.latestObservationAt ?? observation.occurred_at ?? observation.occurredAt,
    ),
    latestObservation: observation.id ? mapFigmaConnectionObservation(observation) : null,
    files,
    lastError,
    nextAction: nullableString(payload.next_action ?? payload.nextAction),
  };
}

function figmaInstallationPath(path: string, installationId?: string): string {
  const normalized = installationId?.trim();
  if (!normalized) {
    return path;
  }
  return `${path}?installation_id=${encodeURIComponent(normalized)}`;
}

function mapFigmaConnectionFile(value: unknown): FigmaConnectionFile {
  const file = recordValue(value);
  return {
    fileKey: stringValue(file.file_key ?? file.fileKey) ?? "unknown",
    fileName: nullableString(file.file_name ?? file.fileName ?? file.name),
    projectName: nullableString(file.project_name ?? file.projectName),
    state: nullableString(file.state ?? file.sync_state ?? file.syncState),
    latestVersion: nullableString(file.latest_version ?? file.latestVersion ?? file.version),
    lastSyncedAt: nullableString(file.last_synced_at ?? file.lastSyncedAt),
    observationCount: numberValue(
      file.observation_count ?? file.observationCount,
      0,
    ),
  };
}

function mapFigmaConnectionObservation(
  observation: Record<string, unknown>,
): FigmaConnectionObservation {
  return {
    id: stringValue(observation.id) ?? "unknown",
    kind: stringValue(observation.kind) ?? "figma:file_snapshot",
    occurredAt: nullableString(observation.occurred_at ?? observation.occurredAt),
    contentText: nullableString(
      observation.content_text ?? observation.contentText ?? observation.summary,
    ),
    artifactId: nullableString(observation.artifact_id ?? observation.artifactId),
  };
}

async function requestJson<T extends Record<string, unknown>>(
  path: string,
  request: { method: "GET" | "POST" | "DELETE"; body?: Record<string, unknown> },
  options: FigmaOAuthClientOptions,
): Promise<T> {
  const response = await fetch(`${resolveApiBase(options.apiBase)}${path}`, {
    method: request.method,
    headers: {
      Accept: "application/json",
      ...(request.body ? { "Content-Type": "application/json" } : {}),
      ...authorizationHeader(options.gatewayToken),
    },
    ...(request.body ? { body: JSON.stringify(request.body) } : {}),
    credentials: "include",
    signal: options.signal,
  });
  const body = await response.text();
  if (!response.ok) {
    throw new FigmaOAuthApiError(response.status, body);
  }
  if (!body) {
    return {} as T;
  }
  try {
    return JSON.parse(body) as T;
  } catch {
    throw new Error("Figma connection returned an invalid response.");
  }
}

function resolveApiBase(apiBase?: string): string {
  const configured =
    apiBase ??
    process.env.NEXT_PUBLIC_FYRALIS_PROVIDER_INGRESS_URL ??
    process.env.NEXT_PUBLIC_FYRALIS_API_BASE ??
    (typeof window !== "undefined" ? window.location.origin : "");
  const resolved = configured.trim().replace(/\/+$/, "");
  if (!resolved) {
    throw new Error("Fyralis gateway API base is required.");
  }
  return resolved;
}

function authorizationHeader(token: string | null | undefined): HeadersInit {
  const value = token?.trim();
  return value ? { Authorization: `Bearer ${value}` } : {};
}

function connectionState(value: unknown): FigmaConnectionState {
  const normalized = stringValue(value)?.toLowerCase().replaceAll("-", "_");
  // `pending` is the database/storage name used while the source-onboarding
  // workflow is queued. The UI deliberately presents that as a sync state.
  if (normalized === "pending") {
    return "syncing";
  }
  switch (normalized) {
    case "deployment_setup_required":
    case "not_connected":
    case "ready_for_provider_approval":
    case "authorizing":
    case "finalizing":
    case "syncing":
    case "connected":
    case "degraded":
    case "reauthorization_required":
    case "error":
    case "disconnected":
    case "multiple_installations":
      return normalized;
    default:
      return "unknown";
  }
}

function setupOwner(value: unknown): "deployment_admin" | null {
  return stringValue(value) === "deployment_admin"
    ? "deployment_admin"
    : null;
}

function recordValue(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function arrayValue(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function nullableString(value: unknown): string | null {
  return stringValue(value);
}

function numberValue(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return fallback;
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function booleanValue(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function errorMessageFromBody(body: string): string | null {
  try {
    const parsed = JSON.parse(body) as { detail?: unknown; message?: unknown; error?: unknown };
    return stringValue(parsed.message) ?? stringValue(parsed.error) ?? stringValue(parsed.detail);
  } catch {
    return body.trim() || null;
  }
}
