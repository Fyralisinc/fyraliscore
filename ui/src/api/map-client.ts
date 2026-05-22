// HTTP client for the Map view. Mirrors structure-client.ts pattern:
// thin fetch wrapper, auth header, AbortSignal threaded through.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";
import type {
  MapSnapshotResponse,
  ModelStoryResponse,
  TopologyEventsResponse,
} from "./map-types";

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

export type GetSnapshotOptions = {
  neighborhoodId?: string;
  edgeKinds?: string[]; // subset of the 4 kinds
  includeArchived?: boolean;
  since?: string; // ISO8601
  // Expand a single band to up to 30 nodes (overview keeps others
  // small). One of 'goal' | 'commitment' | 'decision' | 'risk' |
  // 'customer'. Omit for the overview view.
  lens?: string;
};

export function getMapSnapshot(
  opts: GetSnapshotOptions = {},
  signal?: AbortSignal
): Promise<MapSnapshotResponse> {
  const params = new URLSearchParams();
  if (opts.neighborhoodId) params.set("neighborhood_id", opts.neighborhoodId);
  if (opts.edgeKinds && opts.edgeKinds.length > 0) {
    params.set("edge_kinds", opts.edgeKinds.join(","));
  }
  if (opts.includeArchived) params.set("include_archived", "true");
  if (opts.since) params.set("since", opts.since);
  if (opts.lens) params.set("lens", opts.lens);
  const qs = params.toString();
  return request<MapSnapshotResponse>(
    `/map/snapshot${qs ? `?${qs}` : ""}`,
    undefined,
    signal
  );
}

export function getTopologyEvents(
  opts: { since?: string; limit?: number } = {},
  signal?: AbortSignal
): Promise<TopologyEventsResponse> {
  const params = new URLSearchParams();
  if (opts.since) params.set("since", opts.since);
  if (opts.limit !== undefined) params.set("limit", String(opts.limit));
  const qs = params.toString();
  return request<TopologyEventsResponse>(
    `/map/topology_events${qs ? `?${qs}` : ""}`,
    undefined,
    signal
  );
}

export function getModelStory(
  modelId: string,
  signal?: AbortSignal
): Promise<ModelStoryResponse> {
  return request<ModelStoryResponse>(
    `/map/models/${encodeURIComponent(modelId)}`,
    undefined,
    signal
  );
}
