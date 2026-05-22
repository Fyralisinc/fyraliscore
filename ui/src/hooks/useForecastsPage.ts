// React hook that loads the Forecasts page initial payload + on-demand
// forecast details. Components consume `state` for render and the
// returned callbacks to react to user actions. The hook is intentionally
// flat — no caching layer; the backend response already includes the
// default selection's detail so the first paint is single-fetch.

import { useCallback, useEffect, useRef, useState } from "react";
import {
  getAccuracy,
  getForecastDetail,
  getForecastsPage,
  getPatterns,
} from "@/api/forecasts-client";
import type {
  AccuracyResponse,
  ForecastDetail,
  ForecastsPagePayload,
  PatternCard,
} from "@/api/forecasts-types";

export type LoadPhase = "loading" | "ready" | "empty" | "error";

export interface UseForecastsPage {
  phase: LoadPhase;
  payload: ForecastsPagePayload | null;
  error: string | null;
  selectedId: string | null;
  detailById: Record<string, ForecastDetail>;
  detailPending: boolean;
  patterns: PatternCard[];
  accuracy: AccuracyResponse | null;
  selectForecast: (id: string | null) => void;
  refresh: () => void;
}

export function useForecastsPage(horizonDays = 90): UseForecastsPage {
  const [phase, setPhase] = useState<LoadPhase>("loading");
  const [payload, setPayload] = useState<ForecastsPagePayload | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [detailById, setDetailById] = useState<Record<string, ForecastDetail>>({});
  const [detailPending, setDetailPending] = useState(false);
  const [accuracy, setAccuracy] = useState<AccuracyResponse | null>(null);
  const [patterns, setPatterns] = useState<PatternCard[]>([]);
  const reloadTokenRef = useRef(0);

  const loadAll = useCallback(() => {
    setPhase("loading");
    setError(null);
    const token = ++reloadTokenRef.current;
    const ac = new AbortController();
    getForecastsPage(horizonDays, ac.signal)
      .then((p) => {
        if (token !== reloadTokenRef.current) return;
        setPayload(p);
        setSelectedId(p.selected_forecast_id);
        setDetailById(p.forecast_details_by_id ?? {});
        setPatterns(p.patterns ?? []);
        setPhase(p.header.active_forecast_count === 0 ? "empty" : "ready");
      })
      .catch((e: unknown) => {
        if (token !== reloadTokenRef.current) return;
        setError(e instanceof Error ? e.message : String(e));
        setPhase("error");
      });

    // Sidecar loads — accuracy + full patterns list. These don't block
    // first paint but are needed for Patterns / Accuracy modes.
    getAccuracy(180, ac.signal).then(
      (a) => token === reloadTokenRef.current && setAccuracy(a),
      () => {/* swallow — non-fatal */},
    );
    getPatterns(ac.signal).then(
      (p) => {
        if (token !== reloadTokenRef.current) return;
        if (p.patterns?.length) setPatterns(p.patterns);
      },
      () => {/* swallow */},
    );

    return () => ac.abort();
  }, [horizonDays]);

  useEffect(() => loadAll(), [loadAll]);

  const selectForecast = useCallback(
    (id: string | null) => {
      setSelectedId(id);
      if (!id || detailById[id]) return;
      setDetailPending(true);
      getForecastDetail(id)
        .then((d) => setDetailById((prev) => ({ ...prev, [id]: d })))
        .catch(() => {/* ignored — selection stays without detail */})
        .finally(() => setDetailPending(false));
    },
    [detailById],
  );

  return {
    phase,
    payload,
    error,
    selectedId,
    detailById,
    detailPending,
    patterns,
    accuracy,
    selectForecast,
    refresh: loadAll,
  };
}
