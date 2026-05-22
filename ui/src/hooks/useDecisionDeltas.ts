import { useCallback, useEffect, useRef, useState } from "react";
import {
  acceptDelta as apiAccept,
  addContext as apiAddContext,
  contestDelta as apiContest,
  delegateDelta as apiDelegate,
  getDelta as apiGetDelta,
  listDeltas as apiList,
} from "@/api/decision-deltas-client";
import type {
  AddContextBody,
  ContestBody,
  DecisionDelta,
  DelegateBody,
  DeltaSeverity,
  DeltaView,
  ListDeltasParams,
} from "@/api/decision-deltas-types";

export interface DecisionDeltasState {
  deltas: DecisionDelta[];
  loading: boolean;
  error: string | null;
  refetch: () => Promise<void>;
  loadDetail: (id: string) => Promise<DecisionDelta | null>;
  accept: (id: string) => Promise<void>;
  delegate: (id: string, body: DelegateBody) => Promise<void>;
  contest: (id: string, body: ContestBody) => Promise<void>;
  addContext: (id: string, body: AddContextBody) => Promise<void>;
}

// Build a UI view from the raw wire delta if the server didn't already
// attach one. Keeps downstream components consuming a single shape.
function deriveView(d: DecisionDelta): DeltaView {
  if (d.view) return d.view;
  const severity: DeltaSeverity =
    d.label === "authority_required"
      ? d.confidence != null && d.confidence >= 0.75
        ? "critical"
        : "high"
      : d.confidence != null && d.confidence >= 0.7
        ? "medium"
        : "low";
  const title = d.main_assertion.split(":")[0]?.trim() || d.main_assertion;
  const body = d.main_assertion;
  const chips: string[] = [];
  if (d.category) {
    chips.push(
      d.category
        .split("_")
        .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
        .join(" ")
    );
  }
  const entityRefs = Array.isArray(d.impact?.entity_refs)
    ? (d.impact?.entity_refs as string[])
    : [];
  const stale = d.impact?.stale_days ?? null;
  const staleLabel = typeof stale === "number" ? `${stale} days` : null;
  return {
    severity,
    title,
    body,
    chips,
    entity_refs: entityRefs,
    stale_days: typeof stale === "number" ? stale : null,
    stale_label: staleLabel,
    owner: null,
    authority_required: d.label === "authority_required",
  };
}

export function enrich(d: DecisionDelta): DecisionDelta {
  return { ...d, view: deriveView(d) };
}

export function useDecisionDeltas(
  params: ListDeltasParams = { status: "proposed", limit: 50 }
): DecisionDeltasState {
  const [deltas, setDeltas] = useState<DecisionDelta[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const mountedRef = useRef(true);
  // Stable serialization of the params object so the effect doesn't
  // refetch on every parent render.
  const paramsKey = JSON.stringify(params);

  const fetchOnce = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiList(JSON.parse(paramsKey));
      if (!mountedRef.current) return;
      setDeltas(res.items.map(enrich));
    } catch (e) {
      if (!mountedRef.current) return;
      setError(e instanceof Error ? e.message : "Failed to load");
    } finally {
      if (mountedRef.current) setLoading(false);
    }
  }, [paramsKey]);

  useEffect(() => {
    mountedRef.current = true;
    void fetchOnce();
    function onFocus() {
      void fetchOnce();
    }
    window.addEventListener("focus", onFocus);
    return () => {
      mountedRef.current = false;
      window.removeEventListener("focus", onFocus);
    };
  }, [fetchOnce]);

  const loadDetail = useCallback(async (id: string) => {
    try {
      const detail = await apiGetDelta(id);
      const enriched = enrich(detail);
      setDeltas((prev) =>
        prev.map((d) => (d.id === id ? { ...d, ...enriched } : d))
      );
      return enriched;
    } catch (e) {
      return null;
    }
  }, []);

  const removeFromList = useCallback((id: string) => {
    setDeltas((prev) => prev.filter((d) => d.id !== id));
  }, []);

  const accept = useCallback(async (id: string) => {
    try {
      await apiAccept(id);
      removeFromList(id);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Accept failed");
    }
  }, [removeFromList]);

  const delegate = useCallback(
    async (id: string, body: DelegateBody) => {
      try {
        await apiDelegate(id, body);
        removeFromList(id);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Delegate failed");
      }
    },
    [removeFromList]
  );

  const contest = useCallback(
    async (id: string, body: ContestBody) => {
      try {
        await apiContest(id, body);
        removeFromList(id);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Contest failed");
      }
    },
    [removeFromList]
  );

  const addCtx = useCallback(
    async (id: string, body: AddContextBody) => {
      try {
        await apiAddContext(id, body);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Add context failed");
      }
    },
    []
  );

  return {
    deltas,
    loading,
    error,
    refetch: fetchOnce,
    loadDetail,
    accept,
    delegate,
    contest,
    addContext: addCtx,
  };
}
