import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Sidebar } from "@/components/Sidebar";
import { PageHeader } from "@/components/PageHeader";
import { JustUpdated } from "@/components/JustUpdated";
import { FilterBar, DEFAULT_FILTERS, type TodayFilters } from "@/components/FilterBar";
import { RecCard } from "@/components/RecCard";
import { EmptyState } from "@/components/EmptyState";
import { RoutedCoda } from "@/components/RoutedCoda";
import { AskZone } from "@/components/AskZone";
import { ShortcutsOverlay } from "@/components/ShortcutsOverlay";
import { Conversation } from "@/components/Conversation";
import { ThinkingTurn } from "@/components/ThinkingTurn";
import { SignalSimulator } from "@/components/SignalSimulator";
import { TriageToast } from "@/components/TriageToast";
import { ArtifactDrawer } from "@/components/ArtifactDrawer";
import type { ArtifactKind } from "@/api/today-types";
import { useToday } from "@/hooks/useToday";
import { useAsk } from "@/hooks/useAsk";
import { useRecommendationStream } from "@/hooks/useRecommendationStream";
import type { RecCard as RecCardType, TriageAction } from "@/api/today-types";
import { MindStore, makeId } from "@/components/mind/store";
import { HoldPicker } from "@/components/mind/HoldPicker";
import type { Loop, Note, Reminder } from "@/components/mind/types";

// Fyralis — Today page.
// Two-column cockpit (sidebar + main column). The main column carries
// the signal strip across its top and the feed below. Keyboard model
// per spec §5.2: J/K, A/H/R/S/D, Enter, /, 1/2/3, ?, Esc — all without
// modifiers, except when an input is focused (only Esc to blur).
export default function App() {
  const navigate = useNavigate();
  const {
    today,
    loading,
    offline,
    dismissingIds,
    cleared,
    triage,
    dismissJustUpdated,
    toast,
    dismissToast,
  } = useToday();
  const { turns, ask, dismiss, save, markDone, sending, pending } = useAsk();

  const [filters, setFilters] = useState<TodayFilters>(() => DEFAULT_FILTERS);
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const [expandedIds, setExpandedIds] = useState<Set<string>>(() => new Set());
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [justArrived, setJustArrived] = useState<Set<string>>(() => new Set());
  const [holdPickerCard, setHoldPickerCard] = useState<RecCardType | null>(null);
  const [artifactTarget, setArtifactTarget] = useState<
    { kind: ArtifactKind; id: string } | null
  >(null);
  // Focus mode: hide sidebar + signal strip so the cards are the only
  const askRef = useRef<HTMLInputElement | null>(null);
  const cardRefs = useRef<Record<string, HTMLElement | null>>({});

  // Demo session probe — Session 5 stores these on /demo session start.
  // Reading them once on mount is sufficient for this surface; the page
  // reloads when the demo flow starts.
  const demoToken = useMemo(() => {
    try {
      return localStorage.getItem("demoAuthToken");
    } catch {
      return null;
    }
  }, []);
  const demoSessionId = useMemo(() => {
    try {
      return localStorage.getItem("demoSessionId");
    } catch {
      return null;
    }
  }, []);

  // SSE recommendation stream — only enabled when a demo token is present.
  // The hook is a no-op otherwise so the regular Today flow stays unchanged.
  const { events: streamEvents, connected: streamConnected } =
    useRecommendationStream({ enabled: !!demoToken, token: demoToken });

  // When a created/archived event arrives, refetch the recommendation list
  // and flag the affected card as "just arrived" for the highlight flash.
  // The Today endpoint owns the on-screen card layout, so we hint it to
  // refresh by triggering a soft reload of the page-level fetch through the
  // useToday hook's stream channel — but since we don't own that, we
  // additionally flash the card using the recommendation_id directly.
  const lastEventIdxRef = useRef(0);
  useEffect(() => {
    if (streamEvents.length === lastEventIdxRef.current) return;
    const fresh = streamEvents.slice(lastEventIdxRef.current);
    lastEventIdxRef.current = streamEvents.length;
    for (const ev of fresh) {
      if (ev.event === "created" || ev.event === "updated") {
        setJustArrived((prev) => {
          const next = new Set(prev);
          next.add(ev.recommendation_id);
          return next;
        });
        const id = ev.recommendation_id;
        window.setTimeout(() => {
          setJustArrived((prev) => {
            const next = new Set(prev);
            next.delete(id);
            return next;
          });
        }, 1800);
      }
    }
  }, [streamEvents]);

  // Multi-dimensional filter. Empty sets = no constraint on that axis.
  const visibleCards = useMemo(() => {
    if (!today) return [];
    return today.cards.filter((c) => {
      if (filters.category !== "all" && c.category !== filters.category) return false;
      if (filters.severities.size > 0 && !filters.severities.has(c.severity)) return false;
      if (filters.targetKinds.size > 0) {
        const tk = c.detail?.diff?.target_kind;
        if (!tk || !filters.targetKinds.has(tk)) return false;
      }
      if (filters.owners.size > 0) {
        const owner = c.detail?.diff?.owner_name;
        if (!owner || !filters.owners.has(owner)) return false;
      }
      if (filters.newOnly && c.tag?.kind !== "new") return false;
      return true;
    });
  }, [today, filters]);

  // Derive multi-select option lists from the current feed.
  const ownerOptions = useMemo(() => {
    const set = new Set<string>();
    for (const c of today?.cards ?? []) {
      const o = c.detail?.diff?.owner_name;
      if (o) set.add(o);
    }
    return [...set].sort();
  }, [today?.cards]);
  const targetKindOptions = useMemo(() => {
    const set = new Set<string>();
    for (const c of today?.cards ?? []) {
      const tk = c.detail?.diff?.target_kind;
      if (tk) set.add(tk);
    }
    return [...set].sort();
  }, [today?.cards]);

  // Initial focus: first card after 100ms (per spec §5.3).
  useEffect(() => {
    if (!today || focusedId !== null) return;
    if (visibleCards.length === 0) return;
    const t = window.setTimeout(() => {
      setFocusedId(visibleCards[0].id);
    }, 100);
    return () => window.clearTimeout(t);
  }, [today, focusedId, visibleCards]);

  // Auto-scroll the focused card into view.
  useEffect(() => {
    if (!focusedId) return;
    const el = cardRefs.current[focusedId];
    if (!el) return;
    el.scrollIntoView({ block: "nearest", behavior: "smooth" });
  }, [focusedId]);

  // Reset focus when filters change.
  useEffect(() => {
    if (visibleCards.length === 0) {
      setFocusedId(null);
      return;
    }
    if (focusedId && visibleCards.find((c) => c.id === focusedId)) return;
    setFocusedId(visibleCards[0]?.id ?? null);
  }, [filters, visibleCards, focusedId]);

  const focusNext = useCallback(
    (delta: number) => {
      if (visibleCards.length === 0) return;
      const idx = visibleCards.findIndex((c) => c.id === focusedId);
      const target =
        idx < 0
          ? visibleCards[0]
          : visibleCards[Math.min(visibleCards.length - 1, Math.max(0, idx + delta))];
      setFocusedId(target.id);
    },
    [focusedId, visibleCards]
  );

  const toggleExpansion = useCallback((id: string) => {
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }, []);

  const onTriage = useCallback(
    (id: string, action: TriageAction, extra?: { selected_path_id?: string; ask?: string }) => {
      // Hold sends the card to My Mind as a Loop with substrate context
      // preserved (spec §12.1). Capture before triage removes the card.
      if (action === "hold") {
        const card = visibleCards.find((c) => c.id === id);
        if (card) {
          MindStore.addLoop(makeLoopFromTodayCard(card));
        }
      }
      // After the dismissal animation, focus the next card (per spec §5.3).
      const idx = visibleCards.findIndex((c) => c.id === id);
      const next = visibleCards[idx + 1] ?? visibleCards[idx - 1] ?? null;
      void triage(id, action, extra);
      window.setTimeout(() => {
        setFocusedId(next?.id ?? null);
        setExpandedIds((prev) => {
          const n = new Set(prev);
          n.delete(id);
          return n;
        });
      }, 600);
    },
    [triage, visibleCards]
  );

  // Shift+H opens the picker (spec §12.3). Confirming the picker writes the
  // chosen item type to My Mind and dispatches the existing triage flow so
  // the Today card sweeps away the same as a plain hold.
  const handleHoldPickerConfirm = useCallback(
    (
      choice: "loop" | "note" | "reminder",
      extras?: { remind_at?: string; condition?: string }
    ) => {
      if (!holdPickerCard) return;
      const card = holdPickerCard;
      const headline = stripHtml(card.headline_html);
      const nowIso = new Date().toISOString();
      if (choice === "loop") {
        MindStore.addLoop(makeLoopFromTodayCard(card));
      } else if (choice === "note") {
        const note: Note = {
          id: makeId("note"),
          category: "note",
          headline,
          created: nowIso,
          state: "captured",
        };
        MindStore.addNote(note);
      } else {
        const rem: Reminder = {
          id: makeId("rem"),
          category: "reminder",
          trigger_type: extras?.condition ? "condition" : "time",
          headline,
          created: nowIso,
          state: "pending",
          remind_at: extras?.remind_at,
          condition: extras?.condition,
          signals: extras?.condition ? [] : undefined,
        };
        MindStore.addReminder(rem);
      }
      setHoldPickerCard(null);
      // After picking, still run the hold triage so the Today card sweeps.
      onTriageRef.current?.(card.id, "hold");
    },
    [holdPickerCard]
  );

  // Need a stable ref to onTriage so handleHoldPickerConfirm can call it
  // without circular deps.
  const onTriageRef = useRef(onTriage);
  useEffect(() => {
    onTriageRef.current = onTriage;
  }, [onTriage]);

  // Delegated artifact-link clicks. Server emits `<a class="artifact-link"
  // data-artifact-type=… data-artifact-id=…>`; we catch those and open
  // the drawer. Stop propagation so card-level toggles + probe handlers
  // don't fire on the same click.
  useEffect(() => {
    const KNOWN: ReadonlySet<ArtifactKind> = new Set([
      "actor", "commitment", "goal", "decision",
      "resource", "observation", "model",
    ]);
    function onDocClick(e: MouseEvent) {
      const link = (e.target as HTMLElement | null)?.closest<HTMLElement>(
        ".artifact-link"
      );
      if (!link) return;
      const kind = link.dataset.artifactType ?? "";
      const id = link.dataset.artifactId ?? "";
      if (!id || !KNOWN.has(kind as ArtifactKind)) return;
      e.preventDefault();
      e.stopPropagation();
      setArtifactTarget({ kind: kind as ArtifactKind, id });
    }
    document.addEventListener("click", onDocClick, true);
    return () => document.removeEventListener("click", onDocClick, true);
  }, []);

  // Keyboard model per spec §5.2. Single-key shortcuts only fire when
  // no input/textarea is focused; Esc always works to blur.
  useEffect(() => {
    function isInput(el: Element | null): boolean {
      if (!el) return false;
      const tag = (el as HTMLElement).tagName;
      return tag === "INPUT" || tag === "TEXTAREA" || (el as HTMLElement).isContentEditable;
    }
    function onKey(e: KeyboardEvent) {
      const active = document.activeElement;
      if (e.key === "Escape") {
        if (shortcutsOpen) {
          setShortcutsOpen(false);
          e.preventDefault();
          return;
        }
        if (isInput(active)) {
          (active as HTMLElement).blur();
          e.preventDefault();
          return;
        }
        // Esc collapses the focused expanded card — the keyboard inverse
        // to Enter (which toggles).
        if (focusedId && expandedIds.has(focusedId)) {
          e.preventDefault();
          setExpandedIds((prev) => {
            const next = new Set(prev);
            next.delete(focusedId);
            return next;
          });
          return;
        }
        return;
      }
      if (isInput(active)) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;

      const k = e.key.toLowerCase();

      // Shift+H — picker for sending to My Mind (spec §12.3).
      if (e.shiftKey && k === "h") {
        if (!focusedId) return;
        const card = visibleCards.find((c) => c.id === focusedId);
        if (!card) return;
        e.preventDefault();
        setHoldPickerCard(card);
        return;
      }
      switch (k) {
        case "j":
          e.preventDefault();
          focusNext(1);
          break;
        case "k":
          e.preventDefault();
          focusNext(-1);
          break;
        case "enter":
          if (focusedId) {
            e.preventDefault();
            toggleExpansion(focusedId);
          }
          break;
        case "a":
        case "h":
        case "r":
        case "s":
        case "d": {
          if (!focusedId) return;
          const map: Record<string, TriageAction> = {
            a: "act", h: "hold", r: "route", s: "snooze", d: "dismiss",
          };
          const card = visibleCards.find((c) => c.id === focusedId);
          if (!card) return;
          const action = map[k];
          if (!card.actions.includes(action)) return;
          e.preventDefault();
          onTriage(focusedId, action);
          break;
        }
        case "/":
          e.preventDefault();
          askRef.current?.focus();
          break;
        case "?":
          e.preventDefault();
          setShortcutsOpen(true);
          break;
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [expandedIds, focusNext, focusedId, navigate, onTriage, shortcutsOpen, toggleExpansion, visibleCards]);

  return (
    <>
      {offline ? (
        <div className="offline-banner">
          backend unreachable · showing last good state
        </div>
      ) : null}
      <div className="cockpit">
        <Sidebar
          brand={{ name: "Fyralis", mark: "F", pulse_day: today?.brand?.pulse_day ?? 0 }}
          nav={[
            {
              id: "primary",
              label: "Surfaces",
              items: [
                { id: "today", label: "Today", active: true },
                { id: "structure", label: "Structure" },
                { id: "history", label: "History" },
              ],
            },
          ]}
          onBrandClick={() => {
            // Reset Today to its default view: clear all filters,
            // collapse expanded cards, focus the first card.
            setFilters(DEFAULT_FILTERS);
            setExpandedIds(new Set());
            setFocusedId(null);
          }}
          onNavigate={(_section, item) => {
            if (item === "structure") navigate("/structure");
            else if (item === "history") navigate("/history");
            else if (item === "today") navigate("/");
          }}
        />
        <main>
          <div className="feed">
            {loading && !today ? (
              <div className="loading-shell">Warming up…</div>
            ) : null}

            {today ? (
              <>
                <PageHeader
                  header={today.page}
                  live={demoToken ? streamConnected : null}
                />

                {today.calibration_alert ? (
                  <div className="cal-alert">{today.calibration_alert.text}</div>
                ) : null}

                {today.just_updated ? (
                  <JustUpdated
                    text_html={today.just_updated.text_html}
                    onDismiss={dismissJustUpdated}
                  />
                ) : null}

                <FilterBar
                  filters={filters}
                  onChange={setFilters}
                  ownerOptions={ownerOptions}
                  targetKindOptions={targetKindOptions}
                  visibleCount={visibleCards.length}
                  totalCount={today.cards.length}
                  cleared={cleared}
                />

                <div className="feed-list">
                  {visibleCards.length === 0 && today.cards.length === 0 ? (
                    <EmptyState
                      headline={today.empty_state?.headline}
                      body={today.empty_state?.body}
                    />
                  ) : visibleCards.length === 0 ? (
                    <div className="feed-empty-filter">
                      No items match the current filter.
                      <br />
                      <button
                        type="button"
                        className="btn-text"
                        onClick={() => setFilters(DEFAULT_FILTERS)}
                      >
                        Clear filters
                      </button>
                    </div>
                  ) : (
                    visibleCards.slice(0, 12).map((card) => (
                      <RecCard
                        key={card.id}
                        ref={(n) => {
                          cardRefs.current[card.id] = n;
                        }}
                        card={card}
                        focused={focusedId === card.id}
                        expanded={expandedIds.has(card.id)}
                        dismissing={dismissingIds.has(card.id)}
                        justArrived={justArrived.has(card.id)}
                        onFocus={() => setFocusedId(card.id)}
                        onToggle={() => toggleExpansion(card.id)}
                        onTriage={(action, extra) =>
                          onTriage(card.id, action, extra)
                        }
                      />
                    ))
                  )}
                </div>

                {today.routed_coda && today.routed_coda.total > 0 ? (
                  <RoutedCoda coda={today.routed_coda} />
                ) : null}

                <AskZone
                  ref={askRef}
                  suggestions={today.ask_suggestions}
                  onAsk={(q) => ask(q)}
                  sending={sending}
                />

                {pending && !pending.context_card_id ? (
                  <div className="turns">
                    <ThinkingTurn query={pending.query} />
                  </div>
                ) : null}

                {turns.filter((t) => !t.context_card_id).length > 0 ? (
                  <div className="turns">
                    {turns
                      .filter((t) => !t.context_card_id)
                      .map((t) => (
                        <Conversation
                          key={t.turn_id}
                          turn={t}
                          onFollowUp={() => askRef.current?.focus()}
                          onSave={async () => save(t.turn_id)}
                          onDone={async () => {
                            await markDone(t.turn_id);
                            dismiss(t.turn_id);
                          }}
                        />
                      ))}
                  </div>
                ) : null}
              </>
            ) : null}
          </div>
        </main>
      </div>

      {shortcutsOpen ? (
        <ShortcutsOverlay onClose={() => setShortcutsOpen(false)} />
      ) : null}

      {holdPickerCard ? (
        <HoldPicker
          headline={stripHtml(holdPickerCard.headline_html)}
          onCancel={() => setHoldPickerCard(null)}
          onConfirm={handleHoldPickerConfirm}
        />
      ) : null}

      {demoToken && demoSessionId ? (
        <SignalSimulator token={demoToken} sessionId={demoSessionId} />
      ) : null}

      <TriageToast toast={toast} onDismiss={dismissToast} />

      <ArtifactDrawer
        target={artifactTarget}
        onClose={() => setArtifactTarget(null)}
      />
    </>
  );
}

// Helpers for held-from-Today integration (spec §12).
function stripHtml(html: string | undefined): string {
  if (!html) return "";
  return html
    .replace(/<[^>]+>/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function makeLoopFromTodayCard(card: RecCardType): Loop {
  const now = new Date().toISOString();
  const headline = stripHtml(card.headline_html);
  const evidenceParts: string[] = [];
  if (card.detail?.confidence) {
    const conf = card.detail.confidence
      .map((c) => `${c.label}: ${stripHtml(c.value_html)}`)
      .join(" · ");
    if (conf) evidenceParts.push(conf);
  }
  if (card.detail?.evidence) {
    const ev = card.detail.evidence
      .slice(0, 3)
      .map((e) => `${e.src} ${stripHtml(e.quote_html)}`)
      .join(" / ");
    if (ev) evidenceParts.push(ev);
  }
  return {
    id: makeId("loop"),
    category: "loop",
    kind: card.category === "strategic" ? "concern" : "action",
    headline,
    created: now,
    updated: now,
    state: "open",
    from_today: true,
    today_card_id: card.id,
    person: null,
    substrate_evidence: evidenceParts.length > 0 ? evidenceParts.join(" · ") : undefined,
    user_notes: [],
  };
}
