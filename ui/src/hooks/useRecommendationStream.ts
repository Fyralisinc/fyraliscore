import { useEffect, useRef, useState } from "react";
import {
  RecommendationStream,
  type RecStreamEvent,
} from "@/api/recommendation-stream";

export type RecEvent = Extract<
  RecStreamEvent,
  { event: "created" | "updated" | "archived" }
>;

export type UseRecommendationStreamOpts = {
  enabled: boolean;
  token: string | null;
};

export type UseRecommendationStreamResult = {
  events: RecEvent[];
  connected: boolean;
};

// Wraps RecommendationStream as a React hook. Tolerates a missing token
// (no-op) so the page renders cleanly before Session 5 wires auth.
export function useRecommendationStream({
  enabled,
  token,
}: UseRecommendationStreamOpts): UseRecommendationStreamResult {
  const [events, setEvents] = useState<RecEvent[]>([]);
  const [connected, setConnected] = useState(false);
  const streamRef = useRef<RecommendationStream | null>(null);

  useEffect(() => {
    if (!enabled || !token) {
      setConnected(false);
      setEvents([]);
      return;
    }
    const stream = new RecommendationStream();
    streamRef.current = stream;
    stream.connect(
      token,
      (ev) => {
        if (ev.event === "ready") {
          setConnected(true);
          return;
        }
        setEvents((prev) => [...prev, ev]);
      },
      () => {
        setConnected(false);
      }
    );
    return () => {
      stream.close();
      streamRef.current = null;
      setConnected(false);
      setEvents([]);
    };
  }, [enabled, token]);

  return { events, connected };
}
