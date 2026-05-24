// Hooks that fetch the spec-aligned views and write them into the
// global Fyralis store. Components select from the store via
// `useFyralisStore(s => s.X)` so re-renders are scoped.

import { useEffect, useState } from "react";

import {
  listLedgerEvents,
  listOperatingThreads,
  listRecentModelChanges,
  listSpecDeltas,
  listSpecForecasts,
} from "@/api/spec-client";
import type { ListThreadsResponse } from "@/api/operating-thread-types";
import type { ListSpecDeltasResponse } from "@/api/spec-delta-types";
import type { ListLedgerEventsParams } from "@/api/ledger-event-types";
import { useFyralisStore } from "@/lib/store";

type Phase = "idle" | "loading" | "ready" | "error";

export function useOperatingThreads(lens?: string) {
  const setThreads = useFyralisStore((s) => s.setThreads);
  const [meta, setMeta] = useState<{
    phase: Phase;
    error: string | null;
    response: ListThreadsResponse | null;
  }>({ phase: "loading", error: null, response: null });

  useEffect(() => {
    let cancelled = false;
    const ac = new AbortController();
    setMeta((m) => ({ ...m, phase: "loading", error: null }));
    listOperatingThreads({ lens: lens as never }, ac.signal)
      .then((resp) => {
        if (cancelled) return;
        const flat = resp.groups.flatMap((g) => g.threads);
        setThreads(flat);
        setMeta({ phase: "ready", error: null, response: resp });
      })
      .catch((err) => {
        if (cancelled || (err as Error)?.name === "AbortError") return;
        setMeta({ phase: "error", error: (err as Error).message, response: null });
      });
    return () => {
      cancelled = true;
      ac.abort();
    };
  }, [lens, setThreads]);

  return meta;
}

export function useSpecDeltas() {
  const setDeltas = useFyralisStore((s) => s.setDeltas);
  const [meta, setMeta] = useState<{
    phase: Phase;
    error: string | null;
    response: ListSpecDeltasResponse | null;
  }>({ phase: "loading", error: null, response: null });

  useEffect(() => {
    let cancelled = false;
    const ac = new AbortController();
    setMeta((m) => ({ ...m, phase: "loading", error: null }));
    listSpecDeltas(ac.signal)
      .then((resp) => {
        if (cancelled) return;
        setDeltas(resp.deltas);
        setMeta({ phase: "ready", error: null, response: resp });
      })
      .catch((err) => {
        if (cancelled || (err as Error)?.name === "AbortError") return;
        setMeta({ phase: "error", error: (err as Error).message, response: null });
      });
    return () => {
      cancelled = true;
      ac.abort();
    };
  }, [setDeltas]);

  return meta;
}

export function useSpecForecasts() {
  const setForecasts = useFyralisStore((s) => s.setForecasts);
  const [phase, setPhase] = useState<Phase>("loading");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const ac = new AbortController();
    setPhase("loading");
    setError(null);
    listSpecForecasts(ac.signal)
      .then((items) => {
        if (cancelled) return;
        setForecasts(items);
        setPhase("ready");
      })
      .catch((err) => {
        if (cancelled || (err as Error)?.name === "AbortError") return;
        setError((err as Error).message);
        setPhase("error");
      });
    return () => {
      cancelled = true;
      ac.abort();
    };
  }, [setForecasts]);

  return { phase, error };
}

export function useLedgerEvents(params?: ListLedgerEventsParams) {
  const setLedgerEvents = useFyralisStore((s) => s.setLedgerEvents);
  const [phase, setPhase] = useState<Phase>("loading");
  const [error, setError] = useState<string | null>(null);
  const [rangeLabel, setRangeLabel] = useState<string>("");

  const paramsKey = JSON.stringify(params ?? {});

  useEffect(() => {
    let cancelled = false;
    const ac = new AbortController();
    setPhase("loading");
    setError(null);
    listLedgerEvents(params, ac.signal)
      .then((resp) => {
        if (cancelled) return;
        setLedgerEvents(resp.events);
        setRangeLabel(resp.rangeLabel);
        setPhase("ready");
      })
      .catch((err) => {
        if (cancelled || (err as Error)?.name === "AbortError") return;
        setError((err as Error).message);
        setPhase("error");
      });
    return () => {
      cancelled = true;
      ac.abort();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [paramsKey, setLedgerEvents]);

  return { phase, error, rangeLabel };
}

export function useRecentModelChanges() {
  const setRecentChanges = useFyralisStore((s) => s.setRecentChanges);

  useEffect(() => {
    let cancelled = false;
    const ac = new AbortController();
    listRecentModelChanges(ac.signal)
      .then((items) => {
        if (cancelled) return;
        setRecentChanges(items);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
      ac.abort();
    };
  }, [setRecentChanges]);
}
