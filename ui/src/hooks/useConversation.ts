import { useCallback, useEffect, useRef, useState } from "react";
import {
  clearCardConversation,
  getCardConversation,
  postCardProbe,
} from "@/api/client";
import type {
  CardConversation,
  CardExchange,
  ProbeRequest,
} from "@/api/today-types";

// Driftwood revision: per-card conversation hook. Loads the persisted
// conversation on mount, surfaces a sender for new probes, and tracks
// "pending" so the UI can render a thinking indicator on the latest
// exchange while the substrate is generating.

export type PendingProbe = {
  // Display the probe header optimistically, in the right shape, so
  // there's no layout flash when the response settles in.
  probe_kind: "phrase" | "chip" | "ask";
  probe_id?: string;
  probe_action: string;
  probe_text: string;
  // Stable id assigned client-side so the optimistic placeholder can be
  // matched and replaced when the server response arrives.
  pending_id: string;
};

export type UseConversation = {
  conversation: CardConversation | null;
  loading: boolean;
  pending: PendingProbe | null;
  error: string | null;
  /** Send a probe and append the resulting exchange. */
  probe: (req: ProbeRequest, optimistic: Omit<PendingProbe, "pending_id">) => Promise<CardExchange | null>;
  /** Clear the conversation server-side and reset local state. */
  clear: () => Promise<boolean>;
};

export function useConversation(
  cardId: string,
  conversationId?: string,
  enabled: boolean = true
): UseConversation {
  const [conversation, setConversation] = useState<CardConversation | null>(null);
  const [loading, setLoading] = useState(false);
  const [pending, setPending] = useState<PendingProbe | null>(null);
  const [error, setError] = useState<string | null>(null);
  const loadedFor = useRef<string | null>(null);

  useEffect(() => {
    if (!enabled || !cardId) return;
    if (loadedFor.current === cardId) return;
    loadedFor.current = cardId;
    setLoading(true);
    let cancelled = false;
    getCardConversation(cardId)
      .then((c) => {
        if (cancelled) return;
        setConversation(c);
        setError(null);
      })
      .catch((e) => {
        if (cancelled) return;
        // 404 = no conversation yet; treat as empty rather than error
        // so the fresh-state UI renders cleanly.
        if (e?.status === 404) {
          setConversation({
            conversation_id: conversationId ?? "",
            card_id: cardId,
            exchanges: [],
            probed_phrase_ids: [],
            used_chip_ids: [],
            archived: false,
          });
          setError(null);
        } else {
          setError(e instanceof Error ? e.message : "load failed");
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [cardId, conversationId, enabled]);

  const probe = useCallback<UseConversation["probe"]>(
    async (req, optimistic) => {
      const pid = `pending-${Math.random().toString(36).slice(2, 10)}`;
      setPending({ ...optimistic, pending_id: pid });
      setError(null);
      try {
        const resp = await postCardProbe(cardId, req);
        setConversation((prev) => {
          const base: CardConversation = prev ?? {
            conversation_id: resp.exchange.conversation_id,
            card_id: cardId,
            exchanges: [],
            probed_phrase_ids: [],
            used_chip_ids: [],
            archived: false,
          };
          const probedIds = new Set(base.probed_phrase_ids);
          const usedChips = new Set(base.used_chip_ids);
          if (req.kind === "phrase" && req.probe_id) probedIds.add(req.probe_id);
          if (req.kind === "chip" && req.probe_id) usedChips.add(req.probe_id);
          return {
            ...base,
            exchanges: [...base.exchanges, resp.exchange],
            probed_phrase_ids: [...probedIds],
            used_chip_ids: [...usedChips],
            last_probed_at: resp.exchange.created_at,
          };
        });
        return resp.exchange;
      } catch (e) {
        setError(e instanceof Error ? e.message : "probe failed");
        return null;
      } finally {
        setPending(null);
      }
    },
    [cardId]
  );

  const clear = useCallback(async () => {
    try {
      await clearCardConversation(cardId);
      setConversation((prev) =>
        prev
          ? { ...prev, exchanges: [], probed_phrase_ids: [], used_chip_ids: [] }
          : prev
      );
      return true;
    } catch (e) {
      setError(e instanceof Error ? e.message : "clear failed");
      return false;
    }
  }, [cardId]);

  return { conversation, loading, pending, error, probe, clear };
}
