import { useCallback, useState } from "react";
import { postAsk, postTurnAction } from "@/api/client";
import type { AskResponse } from "@/api/types";

// Thin hook around POST /view/ceo/ask + /turn-action. Tracks inflight
// state so the UI can guard against double-submits. Errors surface as
// a string so the caller can render a subtle banner.

// Client-side decoration so the App can route per-card answers back to
// the originating card (the response renders inline beside the ask
// input rather than far below the AskZone).
export type AskTurn = AskResponse & { context_card_id?: string };

// Snapshot of the in-flight ask. Surfaced so the UI can render a
// skeleton/loading placeholder under the right zone (page-level or
// card-level) while the LLM call is outstanding.
export type PendingAsk = {
  query: string;
  context_card_id?: string;
};

export type UseAsk = {
  turns: AskTurn[];
  sending: boolean;
  pending: PendingAsk | null;
  error: string | null;
  ask: (query: string, contextCardId?: string) => Promise<AskResponse | null>;
  dismiss: (turnId: string) => void;
  save: (turnId: string) => Promise<boolean>;
  markDone: (turnId: string) => Promise<boolean>;
};

export function useAsk(): UseAsk {
  const [turns, setTurns] = useState<AskTurn[]>([]);
  const [sending, setSending] = useState(false);
  const [pending, setPending] = useState<PendingAsk | null>(null);
  const [error, setError] = useState<string | null>(null);

  const ask = useCallback<UseAsk["ask"]>(async (query, contextCardId) => {
    setSending(true);
    setPending({ query, context_card_id: contextCardId });
    setError(null);
    try {
      const resp = await postAsk({
        query,
        context_card_id: contextCardId,
      });
      const turn: AskTurn = { ...resp, context_card_id: contextCardId };
      setTurns((prev) => [turn, ...prev]);
      return resp;
    } catch (err) {
      setError(err instanceof Error ? err.message : "ask failed");
      return null;
    } finally {
      setPending(null);
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

  return { turns, sending, pending, error, ask, dismiss, save, markDone };
}
