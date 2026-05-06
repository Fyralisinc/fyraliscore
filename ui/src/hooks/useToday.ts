import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError, getToday, postTriage, postRename } from "@/api/today-client";
import { createStreamClient } from "@/api/stream";
import type {
  TodayResponse,
  TriageAction,
  TriageRequest,
} from "@/api/today-types";

export type TriageToast = {
  id: number;
  kind: TriageAction;
  headline: string;          // 1-line confirmation, eg "Reaffirmed: …"
  detail?: string;           // optional secondary line
  at: number;                // timestamp the toast was created
  // Optional CTA — rendered as a link inside the toast. Used after an
  // accept that creates a new entity so the user can jump straight to
  // the Structure page focused on the freshly-created commitment.
  action?: { label: string; href: string };
};

export type TodayState = {
  today: TodayResponse | null;
  loading: boolean;
  error: string | null;
  offline: boolean;
  dismissingIds: Set<string>;
  cleared: number;
  toast: TriageToast | null;
  dismissToast: () => void;
  triage: (
    cardId: string,
    action: TriageAction,
    extra?: { selected_path_id?: string; ask?: string; routed_to?: string; reason?: string }
  ) => Promise<void>;
  rename: (newName: string) => Promise<void>;
  dismissJustUpdated: () => void;
};

// Fetches /v1/today once on mount, subscribes to /stream/view/ceo/stream
// for live updates. Triage actions sweep the card with the dismissal
// animation (per spec §6.4) before the optimistic update lands. Falls
// back to last-good payload on network failure (per spec §10.6).
export function useToday(): TodayState {
  const [today, setToday] = useState<TodayResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [offline, setOffline] = useState(false);
  const [dismissingIds, setDismissingIds] = useState<Set<string>>(
    () => new Set()
  );
  const [cleared, setCleared] = useState(0);
  const [toast, setToast] = useState<TriageToast | null>(null);
  const lastGoodRef = useRef<TodayResponse | null>(null);
  const toastIdRef = useRef(0);
  // The "Just learned" banner is disabled for now — the backend keeps
  // re-emitting it across polls and the noise outweighs the value.
  // Stripping at the hook layer kills both the App.tsx and Structure.tsx
  // renders without touching the JSX.
  function suppressDismissed(data: TodayResponse): TodayResponse {
    if (data.just_updated) {
      return { ...data, just_updated: undefined };
    }
    return data;
  }

  // Initial fetch.
  useEffect(() => {
    const ctrl = new AbortController();
    let alive = true;
    (async () => {
      try {
        const raw = await getToday(ctrl.signal);
        if (!alive) return;
        const data = suppressDismissed(raw);
        setToday(data);
        lastGoodRef.current = data;
        setLoading(false);
        setOffline(false);
        setError(null);
      } catch (err) {
        if (!alive) return;
        setLoading(false);
        if (err instanceof ApiError) {
          setError(err.message);
        } else if (err instanceof Error && err.name !== "AbortError") {
          setError(err.message);
        }
        setOffline(true);
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, []);

  // Stream subscription for live patches (cards/vitals/signal-strip).
  useEffect(() => {
    const stream = createStreamClient();
    const unsubData = stream.subscribe((msg: any) => {
      setToday((prev) => {
        const base = prev ?? lastGoodRef.current;
        if (!base) return prev;
        switch (msg.type) {
          case "today_updated": {
            const next = suppressDismissed(msg.today);
            lastGoodRef.current = next;
            return next;
          }
          case "vitals_updated":
            return { ...base, vitals: msg.vitals };
          case "signal_strip_updated":
            return { ...base, signal_strip: msg.signal_strip };
          case "card_triaged":
            return {
              ...base,
              cards: base.cards.filter((c) => c.id !== msg.card_id),
            };
          default:
            return prev;
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

  // Polling safety net for new recommendation arrival.
  // The /stream/view/ceo/stream WebSocket only pushes today_updated
  // when the greeting scheduler refreshes its cache; new recommendation
  // Models publish to a different bus (services/demo/sse.py) the
  // frontend isn't subscribed to, so a fresh card from a live signal
  // injection won't appear without a refetch. A 4s poll keeps the demo
  // feel snappy without the cost of a second stream client.
  useEffect(() => {
    let alive = true;
    const id = window.setInterval(async () => {
      if (document.hidden) return;
      try {
        const raw = await getToday();
        if (!alive) return;
        const data = suppressDismissed(raw);
        setToday(data);
        lastGoodRef.current = data;
        setOffline(false);
      } catch {
        // Swallow — the WS path will surface real connection errors.
      }
    }, 4000);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, []);

  const triage = useCallback<TodayState["triage"]>(
    async (cardId, action, extra) => {
      // Snapshot the card BEFORE the optimistic sweep so the toast
      // can quote the actual headline + path label even after the
      // card is gone from state.
      const card = (today ?? lastGoodRef.current)?.cards.find(
        (c) => c.id === cardId
      );
      const cardHeadline = _stripHtml(card?.headline_html) || "the card";
      const pathLabel = (() => {
        if (!extra?.selected_path_id || !card?.detail?.paths) return null;
        const p = card.detail.paths.find((p) => p.id === extra.selected_path_id);
        return p?.label ?? null;
      })();
      const targetTitle: string | null = (() => {
        const stat = card?.stats?.find((s) => s.label === "Target");
        return stat?.value ?? null;
      })();

      // Optimistic sweep — see spec §6.4 (320ms fade + 500ms collapse).
      setDismissingIds((prev) => {
        const next = new Set(prev);
        next.add(cardId);
        return next;
      });
      // Server call in parallel; if it fails we still remove the card
      // locally — the user's intent is the source of truth here, and
      // we'll resync next stream tick.
      const body: TriageRequest = {
        action,
        reason: extra?.reason,
        routed_to: extra?.routed_to,
        notes: extra?.ask,
        selected_path_id: extra?.selected_path_id,
      };
      let serverError: string | null = null;
      let toastAction: { label: string; href: string } | undefined;
      try {
        const res = await postTriage(cardId, body);
        // When an accept produces a freshly-created commitment, offer a
        // jump-to-Structure CTA on the confirmation toast.
        if (
          action === "act" &&
          res?.target_act_change_kind === "create_commitment" &&
          res?.target_act_change_id
        ) {
          toastAction = {
            label: "See in graph →",
            href: `/structure?focus=${res.target_act_change_id}`,
          };
        }
      } catch (err) {
        if (err instanceof ApiError && err.status === 404) {
          // No-op — already gone.
        } else if (err instanceof Error) {
          serverError = err.message;
        }
      }

      // Surface a toast so the user sees what happened. The wording
      // mirrors the verb the user clicked (Reaffirm/Wait/Reject) when
      // a path was selected, falling back to the generic action label.
      const verb = pathLabel ?? _ACTION_VERB[action] ?? "Action taken";
      const detailParts: string[] = [];
      if (targetTitle) detailParts.push(`on ${targetTitle}`);
      if (serverError) detailParts.push(`(server: ${serverError})`);
      const toastId = ++toastIdRef.current;
      setToast({
        id: toastId,
        kind: action,
        headline: `${verb} — ${cardHeadline.slice(0, 70)}${cardHeadline.length > 70 ? "…" : ""}`,
        detail: detailParts.length ? detailParts.join(" ") : undefined,
        at: Date.now(),
        action: toastAction,
      });
      // Auto-dismiss after 4.5s — only if no newer toast has replaced it.
      window.setTimeout(() => {
        setToast((cur) => (cur && cur.id === toastId ? null : cur));
      }, 4500);

      // After the dismissal animation, drop the card from state.
      window.setTimeout(() => {
        setToday((prev) => {
          if (!prev) return prev;
          return { ...prev, cards: prev.cards.filter((c) => c.id !== cardId) };
        });
        setDismissingIds((prev) => {
          const next = new Set(prev);
          next.delete(cardId);
          return next;
        });
        setCleared((c) => c + 1);
      }, 600);
    },
    [today]
  );

  const dismissToast = useCallback(() => setToast(null), []);

  const rename = useCallback<TodayState["rename"]>(async (newName) => {
    setToday((prev) =>
      prev
        ? { ...prev, brand: { ...prev.brand, name: newName, mark: newName.charAt(0).toUpperCase() } }
        : prev
    );
    try {
      await postRename(newName);
    } catch {
      // ignore — local-first
    }
  }, []);

  const dismissJustUpdated = useCallback(() => {
    setToday((prev) => (prev ? { ...prev, just_updated: undefined } : prev));
  }, []);

  return {
    today,
    loading,
    error,
    offline,
    dismissingIds,
    cleared,
    toast,
    dismissToast,
    triage,
    rename,
    dismissJustUpdated,
  };
}


// Verb labels for the toast banner — used when no `selected_path_id`
// hands us a more specific path label (e.g. keyboard shortcut firing
// triage directly).
const _ACTION_VERB: Record<TriageAction, string> = {
  act:     "Acted",
  hold:    "Held",
  route:   "Routed",
  snooze:  "Snoozed",
  dismiss: "Dismissed",
};


function _stripHtml(html: string | undefined): string {
  if (!html) return "";
  // Cheap tag stripper for toast text — never round-tripped to HTML.
  return html.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
}
