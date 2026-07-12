"use client";

import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from "react";

import {
  defaultFigmaOAuthReturnPath,
  disconnectFigmaConnection,
  fetchFigmaConnectionStatus,
  retryFigmaConnection,
  startFigmaOAuth,
  type FigmaConnectionStatus,
  type FigmaOAuthClientOptions,
} from "../services/figma-oauth-service";

export type FigmaOAuthCallbackOutcome = "connected" | "error" | null;

export type UseFigmaOAuthConnectionOptions = FigmaOAuthClientOptions & {
  /** Optional prefilled, non-secret Figma file URLs. */
  initialFileUrlInput?: string;
  /** A relative return path for the Figma OAuth callback. */
  returnPath?: string;
  /** Defaults to 3 seconds while the gateway is finalizing or syncing. */
  pollIntervalMs?: number;
  /** Test/integration seam. The default is a top-level browser redirect. */
  onAuthorize?: (authorizationUrl: string) => void | Promise<void>;
  onStatusChange?: (status: FigmaConnectionStatus) => void;
};

export type FigmaOAuthConnectionController = {
  fileUrlInput: string;
  setFileUrlInput: Dispatch<SetStateAction<string>>;
  status: FigmaConnectionStatus | null;
  callbackOutcome: FigmaOAuthCallbackOutcome;
  loading: boolean;
  starting: boolean;
  retrying: boolean;
  disconnecting: boolean;
  error: string | null;
  connect: () => Promise<void>;
  refresh: () => Promise<void>;
  retry: () => Promise<void>;
  disconnect: () => Promise<void>;
  clearError: () => void;
};

/**
 * Keeps OAuth UI state in React memory only. In particular, this hook never
 * writes gateway credentials, OAuth state, file URLs, or Figma tokens to local
 * or session storage.
 */
export function useFigmaOAuthConnection(
  options: UseFigmaOAuthConnectionOptions = {},
): FigmaOAuthConnectionController {
  const {
    apiBase,
    gatewayToken,
    initialFileUrlInput = "",
    returnPath,
    pollIntervalMs = 3_000,
    onAuthorize,
    onStatusChange,
  } = options;
  const [fileUrlInput, setFileUrlInput] = useState(initialFileUrlInput);
  const [status, setStatus] = useState<FigmaConnectionStatus | null>(null);
  const [callbackOutcome, setCallbackOutcome] =
    useState<FigmaOAuthCallbackOutcome>(null);
  const [loading, setLoading] = useState(true);
  const [starting, setStarting] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const statusChangeRef = useRef(onStatusChange);

  statusChangeRef.current = onStatusChange;

  const clientOptions = useCallback(
    (signal?: AbortSignal): FigmaOAuthClientOptions => ({
      apiBase,
      gatewayToken: gatewayToken ?? developmentGatewayToken(),
      signal,
    }),
    [apiBase, gatewayToken],
  );

  const applyStatus = useCallback((next: FigmaConnectionStatus) => {
    setStatus(next);
    statusChangeRef.current?.(next);
  }, []);

  const refresh = useCallback(async () => {
    const next = await fetchFigmaConnectionStatus(clientOptions());
    applyStatus(next);
  }, [applyStatus, clientOptions]);

  useEffect(() => {
    const controller = new AbortController();
    let alive = true;
    setCallbackOutcome(readAndClearCallbackOutcome());
    setLoading(true);
    void fetchFigmaConnectionStatus(clientOptions(controller.signal))
      .then((next) => {
        if (alive) {
          applyStatus(next);
        }
      })
      .catch((caught: unknown) => {
        // A new tenant normally has no Figma installation yet. Keep the form
        // usable if the gateway represents that as a 404 rather than a status.
        if (alive && !isNotConnectedResponse(caught)) {
          setError(errorMessage(caught));
        }
      })
      .finally(() => {
        if (alive) {
          setLoading(false);
        }
      });
    return () => {
      alive = false;
      controller.abort();
    };
  }, [applyStatus, clientOptions]);

  useEffect(() => {
    if (!status || !shouldPoll(status)) {
      return;
    }
    const timer = window.setInterval(() => {
      void refresh().catch((caught: unknown) => setError(errorMessage(caught)));
    }, pollIntervalMs);
    return () => window.clearInterval(timer);
  }, [pollIntervalMs, refresh, status]);

  const connect = useCallback(async () => {
    setError(null);
    setStarting(true);
    try {
      const result = await startFigmaOAuth(
        {
          fileUrls: [fileUrlInput],
          returnPath: safeReturnPath(returnPath) ?? defaultFigmaOAuthReturnPath(),
        },
        clientOptions(),
      );
      applyStatus({
        ok: result.ok,
        state: result.state,
        setupOwner: null,
        installationId: null,
        fileCount: result.requestedFileCount,
        selectedFileCount: result.requestedFileCount,
        syncedFileCount: 0,
        failedFileCount: 0,
        observationCount: 0,
        latestObservationAt: null,
        latestObservation: null,
        files: [],
        lastError: null,
        nextAction: "Approve the requested Figma scopes, then return to Fyralis.",
      });
      if (onAuthorize) {
        await onAuthorize(result.authorizationUrl);
      } else if (typeof window !== "undefined") {
        // OAuth must use a top-level navigation so Figma can return to the
        // registered callback; opening a popup/webview breaks many SSO flows.
        window.location.assign(result.authorizationUrl);
      }
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setStarting(false);
    }
  }, [applyStatus, clientOptions, fileUrlInput, onAuthorize, returnPath]);

  const retry = useCallback(async () => {
    setError(null);
    setRetrying(true);
    try {
      applyStatus(await retryFigmaConnection(clientOptions()));
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setRetrying(false);
    }
  }, [applyStatus, clientOptions]);

  const disconnect = useCallback(async () => {
    setError(null);
    setDisconnecting(true);
    try {
      const result = await disconnectFigmaConnection(clientOptions());
      applyStatus({
        ok: result.ok,
        state: result.state,
        setupOwner: null,
        installationId: null,
        fileCount: 0,
        selectedFileCount: 0,
        syncedFileCount: 0,
        failedFileCount: 0,
        observationCount: 0,
        latestObservationAt: null,
        latestObservation: null,
        files: [],
        lastError: null,
        nextAction: null,
      });
      setFileUrlInput("");
    } catch (caught) {
      setError(errorMessage(caught));
    } finally {
      setDisconnecting(false);
    }
  }, [applyStatus, clientOptions]);

  return {
    fileUrlInput,
    setFileUrlInput,
    status,
    callbackOutcome,
    loading,
    starting,
    retrying,
    disconnecting,
    error,
    connect,
    refresh: async () => {
      setError(null);
      try {
        await refresh();
      } catch (caught) {
        setError(errorMessage(caught));
      }
    },
    retry,
    disconnect,
    clearError: () => setError(null),
  };
}

function shouldPoll(status: FigmaConnectionStatus): boolean {
  const workflowActive = [
    "ready_for_provider_approval",
    "authorizing",
    "finalizing",
    "syncing",
  ].includes(status.state);
  // Finalization deliberately marks the OAuth grant connected before the
  // asynchronous snapshot shard lands. Keep polling that narrow window so the
  // user sees the first observation without manually refreshing.
  const awaitingFirstObservation =
    status.state === "connected" &&
    Boolean(status.installationId) &&
    status.observationCount === 0 &&
    !status.lastError;
  return workflowActive || awaitingFirstObservation;
}

function readAndClearCallbackOutcome(): FigmaOAuthCallbackOutcome {
  if (typeof window === "undefined") {
    return null;
  }
  const url = new URL(window.location.href);
  const outcome = url.searchParams.get("figma");
  if (outcome !== "connected" && outcome !== "error") {
    return null;
  }
  // Do not leave callback codes or provider error text in the address bar,
  // browser history, copied links, or screenshots after we have rendered it.
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
  window.history.replaceState(null, "", `${url.pathname}${url.search}${url.hash}`);
  return outcome;
}

function developmentGatewayToken(): string | undefined {
  // This public environment variable is intentionally only a local developer
  // bridge. A production host must pass a trusted in-memory session token or
  // mediate the request through its own server-side BFF; never use public env.
  if (process.env.NODE_ENV === "production") {
    return undefined;
  }
  return process.env.NEXT_PUBLIC_FYRALIS_GATEWAY_TOKEN?.trim() || undefined;
}

function safeReturnPath(value: string | undefined): string | undefined {
  if (!value || !value.startsWith("/") || value.startsWith("//")) {
    return undefined;
  }
  return value;
}

function isNotConnectedResponse(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "status" in error &&
    (error as { status?: unknown }).status === 404
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Figma connection failed.";
}
