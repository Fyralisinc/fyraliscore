import { useCallback, useState } from "react";
import { postAsk, postTurnAction } from "@/api/client";
import type { AskResponse } from "@/api/types";

// Thin hook around POST /view/ceo/ask + /turn-action. Tracks inflight
// state so the UI can guard against double-submits. Errors surface as
// a string so the caller can render a subtle banner.

export type UseAsk = {
  turns: AskResponse[];
  sending: boolean;
  error: string | null;
  ask: (query: string, contextCardId?: string) => Promise<AskResponse | null>;
  dismiss: (turnId: string) => void;
  save: (turnId: string) => Promise<boolean>;
  markDone: (turnId: string) => Promise<boolean>;
};

export function useAsk(): UseAsk {
  const [turns, setTurns] = useState<AskResponse[]>([]);
  const [sending, setSending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const ask = useCallback<UseAsk["ask"]>(async (query, contextCardId) => {
    setSending(true);
    setError(null);
    try {
      const resp = await postAsk({
        query,
        context_card_id: contextCardId,
      });
      setTurns((prev) => [resp, ...prev]);
      return resp;
    } catch (err) {
      setError(err instanceof Error ? err.message : "ask failed");
      return null;
    } finally {
      setSending(false);
    }
  }, []);

  const dismiss = useCallback((turnId: string) => {
    setTurns((prev) => prev.filter((t) => t.turn_id !== turnId));
  }, []);

  const save = useCallback<UseAsk["save"]>(async (turnId) => {
    try {
      const r = await postTurnAction({ turn_id: turnId, action: "save" });
      return r.ok;
    } catch {
      return false;
    }
  }, []);

  const markDone = useCallback<UseAsk["markDone"]>(async (turnId) => {
    try {
      const r = await postTurnAction({ turn_id: turnId, action: "done" });
      return r.ok;
    } catch {
      // Do not block the UI on this — `done` is a local-first gesture.
      return true;
    }
  }, []);

  return { turns, sending, error, ask, dismiss, save, markDone };
}
