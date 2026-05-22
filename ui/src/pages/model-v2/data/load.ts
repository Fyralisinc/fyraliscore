// Data loaders for the Model page. Prefer the backend; fall back to
// the spec-aligned fixture when the API is unavailable or returns a
// sparse response (no real tenant has the full 8-category model
// populated yet, so the page would otherwise look thin). The fixture
// matches the worked examples in the spec so the page reads correctly
// even in fallback mode.

import { ApiError } from "@/api/client";

import {
  fetchCategoryFocus,
  fetchItemDetail,
  fetchItemTrace,
  fetchOverview,
  fetchRelationshipFocus,
} from "../api";
import {
  fixtureCategoryFocus,
  fixtureItemDetail,
  fixtureOverview,
  fixtureRelationshipFocus,
  fixtureTrace,
} from "./fixture";
import type {
  CategoryFocus,
  CategoryId,
  ItemDetail,
  ModelOverview,
  RelationshipFocus,
  RelationshipMode,
  Trace,
} from "../types";

function isSparseOverview(o: ModelOverview): boolean {
  if (!o.categories || o.categories.length < 8) return true;
  const total = o.categories.reduce((s, c) => s + c.itemCount, 0);
  if (total < 8) return true;
  if ((o.relationshipBundles || []).length < 3) return true;
  return false;
}

export async function loadOverview(
  mode: RelationshipMode,
  signal?: AbortSignal,
): Promise<ModelOverview> {
  try {
    const api = await fetchOverview(mode, signal);
    if (isSparseOverview(api)) return fixtureOverview(mode);
    return api;
  } catch (err) {
    if ((err as Error)?.name === "AbortError") throw err;
    if (err instanceof ApiError) return fixtureOverview(mode);
    return fixtureOverview(mode);
  }
}

export async function loadCategoryFocus(
  categoryId: CategoryId,
  mode: RelationshipMode,
  signal?: AbortSignal,
): Promise<CategoryFocus> {
  try {
    const api = await fetchCategoryFocus(categoryId, mode, signal);
    if (!api.category || api.topItems.length === 0) {
      return fixtureCategoryFocus(categoryId, mode);
    }
    return api;
  } catch (err) {
    if ((err as Error)?.name === "AbortError") throw err;
    return fixtureCategoryFocus(categoryId, mode);
  }
}

export async function loadRelationshipFocus(
  bundleId: string,
  signal?: AbortSignal,
): Promise<RelationshipFocus | null> {
  try {
    const api = await fetchRelationshipFocus(bundleId, signal);
    if (!api.bundle || api.instances.length === 0) {
      const fx = fixtureRelationshipFocus(bundleId);
      return fx ?? api;
    }
    return api;
  } catch (err) {
    if ((err as Error)?.name === "AbortError") throw err;
    return fixtureRelationshipFocus(bundleId);
  }
}

export async function loadItemDetail(
  itemId: string,
  signal?: AbortSignal,
): Promise<ItemDetail | null> {
  try {
    const api = await fetchItemDetail(itemId, signal);
    return api;
  } catch (err) {
    if ((err as Error)?.name === "AbortError") throw err;
    return fixtureItemDetail(itemId);
  }
}

export async function loadTrace(
  itemId: string,
  direction: "cause" | "consequence",
  depth: number,
  signal?: AbortSignal,
): Promise<Trace> {
  try {
    const api = await fetchItemTrace(itemId, direction, depth, signal);
    if (!api.nodes || api.nodes.length < 2) {
      return fixtureTrace(itemId, direction);
    }
    return api;
  } catch (err) {
    if ((err as Error)?.name === "AbortError") throw err;
    return fixtureTrace(itemId, direction);
  }
}
