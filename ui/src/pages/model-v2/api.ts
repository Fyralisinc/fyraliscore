// HTTP client for the Model page v2 endpoints. Mirrors map-client.ts
// patterns: thin fetch wrapper, AbortSignal threaded through, ApiError
// surfaced for 4xx/5xx.
//
// Falls back to the spec-aligned fixture when the API returns sparse
// data — the Model page is supposed to feel substantial even before
// production tenants have populated every category.

import { ApiError } from "@/api/client";
import { getAuthHeader, handleAuthFailure } from "@/api/auth";

import type {
  CategoryFocus,
  CategoryId,
  ItemDetail,
  ModelOverview,
  RelationshipFocus,
  RelationshipMode,
  Trace,
} from "./types";

const BASE = import.meta.env.VITE_API_BASE ?? "/api";

async function request<T>(
  path: string,
  init?: RequestInit,
  signal?: AbortSignal,
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

export function fetchOverview(
  mode: RelationshipMode,
  signal?: AbortSignal,
): Promise<ModelOverview> {
  return request<ModelOverview>(
    `/model/overview?mode=${encodeURIComponent(mode)}`,
    undefined,
    signal,
  );
}

export function fetchCategoryFocus(
  categoryId: CategoryId,
  mode: RelationshipMode,
  signal?: AbortSignal,
): Promise<CategoryFocus> {
  return request<CategoryFocus>(
    `/model/categories/${encodeURIComponent(categoryId)}/focus?mode=${encodeURIComponent(mode)}`,
    undefined,
    signal,
  );
}

export function fetchRelationshipFocus(
  bundleId: string,
  signal?: AbortSignal,
): Promise<RelationshipFocus> {
  return request<RelationshipFocus>(
    `/model/relationships/${encodeURIComponent(bundleId)}`,
    undefined,
    signal,
  );
}

export function fetchItemDetail(
  itemId: string,
  signal?: AbortSignal,
): Promise<ItemDetail> {
  return request<ItemDetail>(
    `/model/items/${encodeURIComponent(itemId)}`,
    undefined,
    signal,
  );
}

export function fetchItemTrace(
  itemId: string,
  direction: "cause" | "consequence",
  depth: number,
  signal?: AbortSignal,
): Promise<Trace> {
  return request<Trace>(
    `/model/items/${encodeURIComponent(itemId)}/trace?direction=${direction}&depth=${depth}`,
    undefined,
    signal,
  );
}
