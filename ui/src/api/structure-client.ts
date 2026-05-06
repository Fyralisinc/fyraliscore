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
  substrate_insight?: string | null;
  activity?: { date: string; desc: string }[];
};

export type StructureOverlayGoal = {
  id: string;
  label: string;
  altitude: "strategic" | "operational";
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

export type StructureOverlayResponse = {
  commitment: StructureOverlayCommitment;
  goals: StructureOverlayGoal[];
  people: StructureOverlayPerson[];
  customers: StructureOverlayCustomer[];
};

export type StructureRecentResponse = {
  commitments: StructureOverlayCommitment[];
  goals: StructureOverlayGoal[];
  people: StructureOverlayPerson[];
  customers: StructureOverlayCustomer[];
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
