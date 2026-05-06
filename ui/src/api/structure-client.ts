// HTTP client for the Structure page surface. Currently exposes a
// single call: an "overlay" fetch used after a recommendation has just
// created a new Commitment, so the freshly-created entity can be
// rendered into the relational view alongside the page's in-memory
// sample graph.

import { ApiError } from "./client";
import { getAuthHeader, handleAuthFailure } from "./auth";

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

export type StructureOverlayPatternEvidence = {
  when: string;
  text: string;
};

export type StructureOverlayPattern = {
  id: string;
  statement: string;
  strength: number;
  evidence: StructureOverlayPatternEvidence[];
};

export type StructureOverlayCommitment = {
  id: string;
  label: string;
  owner: string | null;
  owner_display: string | null;
  due_date: string | null;
  status: "on-track" | "slipping" | "at-risk" | "blocked";
  priority: "low" | "standard" | "high";
  customer: string | null;
  customer_label: string | null;
  edges: {
    contributes_to: string[];
    constrained_by: string[];
    consumes: string[];
    contributors: string[];
  };
  // Per-commit slice of every consumed resource. Carries the deployed
  // quantity in the resource's native unit so chips can render
  // "Engineering pod · 0.4 FTE" without a separate fetch.
  consumed_resources?: StructureOverlayResource[];
  substrate_insight?: string | null;
  activity?: { date: string; desc: string }[];
  learnings?: StructureOverlayPattern[];
};

export type StructureOverlayGoal = {
  id: string;
  label: string;
  altitude: "strategic" | "operational";
  parent_goal_id?: string | null;
};

export type StructureOverlayPerson = {
  id: string;
  label: string;
  role: string;
};

export type StructureOverlayCustomer = {
  id: string;
  label: string;
};

export type StructureOverlayDecision = {
  id: string;
  label: string;
  state: "in-force" | "drifting" | "revisited";
};

export type StructureOverlayResource = {
  id: string;
  label: string;
  kind: "human" | "financial" | "technical" | "time";
  unit?: string | null;
  // Per-commitment deployed quantity. Present on the commitment-overlay
  // payload (each commit's slice); absent on the recent-list payload
  // (resources are deduped at top level there).
  deployed_quantity?: number | null;
};

export type StructureOverlayResponse = {
  commitment: StructureOverlayCommitment;
  goals: StructureOverlayGoal[];
  people: StructureOverlayPerson[];
  customers: StructureOverlayCustomer[];
  decisions?: StructureOverlayDecision[];
  resources?: StructureOverlayResource[];
};

export type StructureRecentResponse = {
  commitments: StructureOverlayCommitment[];
  goals: StructureOverlayGoal[];
  people: StructureOverlayPerson[];
  customers: StructureOverlayCustomer[];
  decisions?: StructureOverlayDecision[];
  resources?: StructureOverlayResource[];
};

export type ResourceHealth =
  | "available"
  | "under-utilized"
  | "deployed"
  | "constrained"
  | "over-allocated";

export type ResourceTopConsumer = {
  commitment_id: string;
  label: string;
  state: string | null;
  owner_id: string | null;
  deployed_quantity: number;
};

export type StructureResourceAggregate = {
  id: string;
  kind: "human" | "financial" | "technical" | "time";
  identity: string;
  label: string;
  description: string;
  capacity: number;
  unit: string;
  deployed: number;
  available: number;
  utilization_pct: number;
  deployments_count: number;
  health: ResourceHealth;
  category: string | null;
  top_consumers: ResourceTopConsumer[];
};

export type StructureResourcesAggregateResponse = {
  resources: StructureResourceAggregate[];
};

export type StructureResourceConsumer = {
  id: string;
  label: string;
  state: string | null;
  owner_id: string | null;
  due_date: string | null;
  deployed_quantity: number;
};

export type StructureResourceOverlayResponse = {
  resource: {
    id: string;
    kind: "human" | "financial" | "technical" | "time";
    identity: string;
    label: string;
    description: string;
    capacity: number;
    unit: string;
    deployed: number;
    utilization_pct: number;
    category: string | null;
  };
  consumers: StructureResourceConsumer[];
  owners: StructureOverlayPerson[];
};

export function getStructureOverlay(
  commitmentId: string,
  signal?: AbortSignal
): Promise<StructureOverlayResponse> {
  return request<StructureOverlayResponse>(
    `/v1/structure/overlay/${commitmentId}`,
    undefined,
    signal
  );
}

export function getStructureRecent(
  sinceMinutes = 10,
  signal?: AbortSignal
): Promise<StructureRecentResponse> {
  return request<StructureRecentResponse>(
    `/v1/structure/recent?since_minutes=${sinceMinutes}`,
    undefined,
    signal
  );
}

export function getStructureResourcesAggregate(
  signal?: AbortSignal
): Promise<StructureResourcesAggregateResponse> {
  return request<StructureResourcesAggregateResponse>(
    `/v1/structure/resources/aggregate`,
    undefined,
    signal
  );
}

export function getStructureResourceOverlay(
  resourceId: string,
  signal?: AbortSignal
): Promise<StructureResourceOverlayResponse> {
  return request<StructureResourceOverlayResponse>(
    `/v1/structure/resources/${resourceId}/overlay`,
    undefined,
    signal
  );
}
