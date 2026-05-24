// HTTP client for /v1/model/{node_id}/* endpoints. Mirrors the
// map-client pattern: thin fetch wrapper, auth header, AbortSignal
// threaded through.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  AdjacencyResponse,
  TraceChain,
  TraceDirection,
} from "./model-trace-types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

async function request<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal
): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...getAuthHeader(),
      ...((init?.headers as Record<string, string> | undefined) ?? {}),
    },
    signal,
  });
  if (!res.ok) {
    if (res.status === 401) handleAuthFailure();
    throw new ApiError(`${res.status} ${res.statusText}`, res.status);
  }
  return (await res.json()) as T;
}

export type TraceOptions = {
  maxDepth?: number;
};

export function traceBack(
  nodeId: string,
  opts: TraceOptions = {},
  signal?: AbortSignal
): Promise<TraceChain> {
  return trace(nodeId, "back", opts, signal);
}

export function traceForward(
  nodeId: string,
  opts: TraceOptions = {},
  signal?: AbortSignal
): Promise<TraceChain> {
  return trace(nodeId, "forward", opts, signal);
}

export function trace(
  nodeId: string,
  direction: TraceDirection,
  opts: TraceOptions = {},
  signal?: AbortSignal
): Promise<TraceChain> {
  const params = new URLSearchParams();
  params.set("direction", direction);
  if (opts.maxDepth !== undefined) {
    params.set("max_depth", String(opts.maxDepth));
  }
  return request<TraceChain>(
    `/v1/model/${encodeURIComponent(nodeId)}/trace?${params.toString()}`,
    undefined,
    signal
  );
}

export function getSupports(
  nodeId: string,
  signal?: AbortSignal
): Promise<AdjacencyResponse> {
  return request<AdjacencyResponse>(
    `/v1/model/${encodeURIComponent(nodeId)}/supports`,
    undefined,
    signal
  );
}

export function getDependsOn(
  nodeId: string,
  signal?: AbortSignal
): Promise<AdjacencyResponse> {
  return request<AdjacencyResponse>(
    `/v1/model/${encodeURIComponent(nodeId)}/depends_on`,
    undefined,
    signal
  );
}
