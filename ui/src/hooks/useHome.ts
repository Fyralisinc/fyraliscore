import { useEffect, useRef, useState } from "react";
import type { HomeResponse } from "@/api/types";
import { ApiError, getHome } from "@/api/client";
import { createStreamClient } from "@/api/stream";

export type HomeState = {
  home: HomeResponse | null;
  loading: boolean;
  error: string | null;
  offline: boolean;
};

// Fetches /view/ceo/home once on mount and keeps it fresh via
// /view/ceo/stream. If the backend is unreachable the hook flags
// `offline: true` but keeps the last good payload visible — stale is
// better than nothing (per §5.4 of the build plan).
export function useHome(): HomeState {
  const [home, setHome] = useState<HomeResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [offline, setOffline] = useState(false);
  const lastGoodRef = useRef<HomeResponse | null>(null);

  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    (async () => {
      try {
        const data = await getHome(ctrl.signal);
        if (!alive) return;
        setHome(data);
        lastGoodRef.current = data;
        setLoading(false);
        setOffline(false);
      } catch (err) {
        if (!alive) return;
        setLoading(false);
        if (err instanceof ApiError) {
          setError(err.message);
        } else {
          setError(err instanceof Error ? err.message : "unknown error");
        }
        setOffline(true);
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, []);

  useEffect(() => {
    const stream = createStreamClient();
    const unsubData = stream.subscribe((msg) => {
      setHome((prev) => {
        const base = prev ?? lastGoodRef.current;
        if (!base) return prev;
        switch (msg.type) {
          case "greeting_updated":
            return { ...base, greeting: msg.greeting };
          case "cards_updated":
            return { ...base, cards: msg.cards };
          case "query_grid_updated":
            return { ...base, query_grid: msg.query_grid };
          case "status_updated":
            return { ...base, status: msg.status };
          default:
            return base;
        }
      });
    });
    const unsubConn = stream.onConnectionChange((state) => {
      setOffline(state !== "open" && lastGoodRef.current === null);
    });
    stream.start();
    return () => {
      unsubData();
      unsubConn();
      stream.stop();
    };
  }, []);

  return { home, loading, error, offline };
}
