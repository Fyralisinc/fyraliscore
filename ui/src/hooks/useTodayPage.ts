// Data hook for the Today page v2. Loads /api/today on mount, refreshes
// on tab focus, and exposes mutation helpers that re-fetch after each
// successful action. The hook is intentionally fetch-based (no SWR /
// TanStack Query) to match the project's existing idiom.

import { useCallback, useEffect, useRef, useState } from "react";

import {
  ApiError,
  applyDelta,
  delegateDelta,
  getTodayPage,
  submitCorrection,
} from "@/api/today-page-client";
import type {
  ApplyResult,
  CorrectionBody,
  CorrectionResult,
  DelegateBody,
  DelegateResult,
  TodayPageData,
} from "@/api/today-page-types";

export type TodayPageState = {
  data: TodayPageData | null;
  loading: boolean;
  error: ApiError | null;
  refetch: () => Promise<void>;
  applyChange: (id: string) => Promise<ApplyResult | null>;
  delegate: (id: string, body: DelegateBody) => Promise<DelegateResult | null>;
  correct: (id: string, body: CorrectionBody) => Promise<CorrectionResult | null>;
};

export function useTodayPage(): TodayPageState {
  const [data, setData] = useState<TodayPageData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<ApiError | null>(null);
  const aliveRef = useRef(true);

  const load = useCallback(async (signal?: AbortSignal) => {
    setLoading(true);
    setError(null);
    try {
      const page = await getTodayPage(signal);
      if (!aliveRef.current) return;
      setData(page);
    } catch (e) {
      if ((e as DOMException)?.name === "AbortError") return;
      if (!aliveRef.current) return;
      setError(e as ApiError);
    } finally {
      if (aliveRef.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    aliveRef.current = true;
    const ctrl = new AbortController();
    void load(ctrl.signal);
    function onFocus() {
      // Refresh stale data when the user returns to the tab. This is
      // an executive surface — drift is worse than redundant fetches.
      void load();
    }
    window.addEventListener("focus", onFocus);
    return () => {
      aliveRef.current = false;
      ctrl.abort();
      window.removeEventListener("focus", onFocus);
    };
  }, [load]);

  const refetch = useCallback(async () => {
    await load();
  }, [load]);

  const applyChange = useCallback(
    async (id: string): Promise<ApplyResult | null> => {
      try {
        const result = await applyDelta(id);
        await load();
        return result;
      } catch (e) {
        const err = e as ApiError & { body?: unknown };
        if (err.status === 409 && err.body) {
          return err.body as ApplyResult;
        }
        throw e;
      }
    },
    [load],
  );

  const delegate = useCallback(
    async (id: string, body: DelegateBody): Promise<DelegateResult | null> => {
      try {
        const result = await delegateDelta(id, body);
        await load();
        return result;
      } catch (e) {
        const err = e as ApiError & { body?: unknown };
        if (err.status === 409 && err.body) {
          return err.body as DelegateResult;
        }
        throw e;
      }
    },
    [load],
  );

  const correct = useCallback(
    async (id: string, body: CorrectionBody): Promise<CorrectionResult | null> => {
      try {
        const result = await submitCorrection(id, body);
        await load();
        return result;
      } catch (e) {
        const err = e as ApiError & { body?: unknown };
        if (err.status === 409 && err.body) {
          return err.body as CorrectionResult;
        }
        throw e;
      }
    },
    [load],
  );

  return { data, loading, error, refetch, applyChange, delegate, correct };
}
