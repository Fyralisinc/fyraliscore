import { useEffect, useRef, useState } from "react";
import {
  ApiError,
  getHistory,
  type HistoryPeriod,
  type HistoryResponse,
} from "@/api/history-client";

export type HistoryState = {
  history: HistoryResponse | null;
  loading: boolean;
  error: string | null;
  offline: boolean;
  period: HistoryPeriod;
  setPeriod: (p: HistoryPeriod) => void;
  refresh: () => void;
};

// Fetches /v1/history on mount and whenever the period changes. Keeps
// the last-good payload around so a transient fetch failure doesn't
// blank the page. Polls every 8s as a low-rate safety net for newly
// landed substrate events; the History page doesn't need the snappier
// 4s cadence the Today page uses.
export function useHistory(initialPeriod: HistoryPeriod = "90d"): HistoryState {
  const [history, setHistory] = useState<HistoryResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [offline, setOffline] = useState(false);
  const [period, setPeriod] = useState<HistoryPeriod>(initialPeriod);
  const [refreshTick, setRefreshTick] = useState(0);
  const lastGoodRef = useRef<HistoryResponse | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    setLoading(true);
    (async () => {
      try {
        const data = await getHistory(period, ctrl.signal);
        if (!alive) return;
        setHistory(data);
        lastGoodRef.current = data;
        setLoading(false);
        setOffline(false);
        setError(null);
      } catch (err) {
        if (!alive) return;
        if (err instanceof Error && err.name === "AbortError") return;
        setLoading(false);
        if (err instanceof ApiError) {
          setError(err.message);
        } else if (err instanceof Error) {
          setError(err.message);
        }
        setOffline(true);
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, [period, refreshTick]);

  // Polling safety net so freshly landed state-change events surface
  // without the user reloading. Skipped while the tab is hidden.
  useEffect(() => {
    let alive = true;
    const id = window.setInterval(async () => {
      if (document.hidden) return;
      try {
        const data = await getHistory(period);
        if (!alive) return;
        setHistory(data);
        lastGoodRef.current = data;
        setOffline(false);
      } catch {
        // Swallow — initial fetch path surfaces real errors.
      }
    }, 8000);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, [period]);

  return {
    history,
    loading,
    error,
    offline,
    period,
    setPeriod,
    refresh: () => setRefreshTick((t) => t + 1),
  };
}
