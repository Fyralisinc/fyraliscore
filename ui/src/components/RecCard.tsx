import {
  forwardRef,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type {
  RecCard as RecCardModel,
  TriageAction,
  CardExchange,
  ProbeChip,
  DiffPanel,
  SignalRow,
  ReasoningGroup,
  Calibration,
  Falsifier,
} from "@/api/today-types";
import { useConversation } from "@/hooks/useConversation";
import { postWatch, deleteWatch } from "@/api/today-client";

type Props = {
  card: RecCardModel;
  focused: boolean;
  expanded: boolean;
  dismissing: boolean;
  justArrived?: boolean;
  onFocus: () => void;
  onToggle: () => void;
  onTriage: (action: TriageAction, opts?: { selected_path_id?: string; ask?: string }) => void;
};

// UX-2 layout: collapsed card has four bands of decreasing emphasis —
// headline → proposed_change → supporting/epistemic → footer. The
// 3-cell stats grid is gone (its information is folded into
// epistemic_line and approve_label). Severity is a left rule, not a
// header band. Actions collapse to Approve / Discuss / Not now ▾.
const ACTION_LABEL: Record<TriageAction, string> = {
  act: "Approve",
  hold: "Hold",
  route: "Route",
  snooze: "Snooze",
  dismiss: "Dismiss",
};
const ACTION_KEY: Record<TriageAction, string> = {
  act: "A",
  hold: "H",
  route: "R",
  snooze: "S",
  dismiss: "D",
};
const NOT_NOW_ACTIONS: TriageAction[] = ["hold", "snooze", "route", "dismiss"];

export const RecCard = forwardRef<HTMLElement, Props>(function RecCard(
  { card, focused, expanded, dismissing, justArrived, onFocus, onToggle, onTriage },
  ref
) {
  const conversationId = card.detail?.conversation_id;
  const probeChips = useMemo<ProbeChip[]>(
    () => card.detail?.probe_chips ?? [],
    [card.detail?.probe_chips]
  );

  const { conversation, pending, probe } = useConversation(
    card.id,
    conversationId,
    expanded
  );

  const expandLabel = card.expand_cta ?? "Ask why";
  const primary = card.actions[0];
  const approveLabel =
    card.approve_label ?? (primary ? ACTION_LABEL[primary] : "Approve");
  const otherActions = card.actions.slice(1);
  const notNowActions = NOT_NOW_ACTIONS.filter((a) => otherActions.includes(a));

  const [askText, setAskText] = useState("");
  const askRef = useRef<HTMLInputElement>(null);
  const detailInnerRef = useRef<HTMLDivElement>(null);
  const lastExchangeRef = useRef<HTMLElement>(null);
  const [scrollFlags, setScrollFlags] = useState({ above: false, below: false });

  const [menuOpen, setMenuOpen] = useState(false);
  const menuTriggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);

  // Mark probed phrases with the .probed class. We do this in a layout
  // effect so the DOM is up-to-date *before* the browser paints — no
  // flash of un-marked dotted underlines on re-render.
  useLayoutEffect(() => {
    const root = detailInnerRef.current;
    if (!root) return;
    const probedIds = new Set(conversation?.probed_phrase_ids ?? []);
    root.querySelectorAll<HTMLElement>("[data-probe-id]").forEach((el) => {
      const id = el.dataset.probeId;
      if (id && probedIds.has(id)) el.classList.add("probed");
      else el.classList.remove("probed");
    });
  }, [conversation?.probed_phrase_ids, conversation?.exchanges, expanded, card.headline_html]);

  // Wire phrase clicks via event delegation.
  const handleProbeClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      const target = (e.target as HTMLElement).closest<HTMLElement>(
        "[data-probe-id]"
      );
      if (!target) return;
      e.stopPropagation();
      const probeId = target.dataset.probeId!;
      const existing = conversation?.exchanges.find(
        (ex) => ex.probe_kind === "phrase" && ex.probe_id === probeId
      );
      if (existing) {
        const node = detailInnerRef.current?.querySelector<HTMLElement>(
          `[data-exchange-id="${existing.id}"]`
        );
        if (node) {
          node.scrollIntoView({ behavior: "smooth", block: "start" });
          node.classList.add("flash");
          setTimeout(() => node.classList.remove("flash"), 700);
        }
        return;
      }
      target.classList.add("pulse");
      setTimeout(() => target.classList.remove("pulse"), 220);
      const text = target.textContent ?? "";
      void probe(
        { kind: "phrase", probe_id: probeId },
        {
          probe_kind: "phrase",
          probe_id: probeId,
          probe_action: "You clicked",
          probe_text: `"${text}"`,
        }
      );
    },
    [conversation?.exchanges, probe]
  );

  const handleChipClick = useCallback(
    (chip: ProbeChip) => {
      void probe(
        { kind: "chip", probe_id: chip.id },
        {
          probe_kind: "chip",
          probe_id: chip.id,
          probe_action: "You probed",
          probe_text: chip.text,
        }
      );
    },
    [probe]
  );

  const handleAskSubmit = useCallback(() => {
    const q = askText.trim();
    if (!q) return;
    setAskText("");
    void probe(
      { kind: "ask", query: q },
      { probe_kind: "ask", probe_action: "You asked", probe_text: q }
    );
    setTimeout(() => askRef.current?.focus(), 0);
  }, [askText, probe]);

  // Discuss = expand the card and focus the ask field. The single
  // affordance whose cognitive cost is "I want to think before I act."
  const handleDiscuss = useCallback(() => {
    if (!expanded) onToggle();
    onFocus();
    setTimeout(() => askRef.current?.focus(), 220);
  }, [expanded, onToggle, onFocus]);

  // "Not now ▾" menu — dismiss is a value judgement and prompts.
  const handleMenuItem = useCallback(
    (a: TriageAction) => {
      setMenuOpen(false);
      if (a === "dismiss") {
        const reason = window.prompt(
          "Tell me why you disagree (so I can recalibrate)",
          ""
        );
        if (reason && reason.trim()) onTriage(a, { ask: reason });
        return;
      }
      onTriage(a);
    },
    [onTriage]
  );

  // Close menu on outside click / Escape.
  useEffect(() => {
    if (!menuOpen) return;
    function onDocClick(e: MouseEvent) {
      const t = e.target as Node;
      if (menuRef.current?.contains(t)) return;
      if (menuTriggerRef.current?.contains(t)) return;
      setMenuOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setMenuOpen(false);
    }
    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [menuOpen]);

  // Scroll new exchanges (and the pending placeholder) into view.
  useEffect(() => {
    if (!expanded) return;
    const node = lastExchangeRef.current;
    if (!node) return;
    node.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [conversation?.exchanges.length, pending, expanded]);

  // Track scroll-edge gradients per spec §6.6.
  useEffect(() => {
    const el = detailInnerRef.current;
    if (!el) return;
    const update = () => {
      setScrollFlags({
        above: el.scrollTop > 4,
        below: el.scrollTop + el.clientHeight < el.scrollHeight - 4,
      });
    };
    update();
    el.addEventListener("scroll", update, { passive: true });
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => {
      el.removeEventListener("scroll", update);
      ro.disconnect();
    };
  }, [expanded, conversation?.exchanges.length]);

  const visibleChips = useMemo(() => {
    const used = new Set(conversation?.used_chip_ids ?? []);
    return probeChips.filter((c) => !used.has(c.id));
  }, [probeChips, conversation?.used_chip_ids]);

  const renderedExchanges: (CardExchange | { pending: true; id: string })[] = useMemo(() => {
    const list: (CardExchange | { pending: true; id: string })[] = [
      ...(conversation?.exchanges ?? []),
    ];
    if (pending) list.push({ pending: true, id: pending.pending_id });
    return list;
  }, [conversation?.exchanges, pending]);

  const archived = conversation?.archived ?? false;

  return (
    <article
      ref={ref}
      className={
        "card" +
        (focused ? " focused" : "") +
        (expanded ? " expanded" : "") +
        (dismissing ? " dismissing" : "")
      }
      data-sev={card.severity}
      data-kind={card.category}
      data-item={card.id}
      data-just-arrived={justArrived ? "true" : undefined}
      tabIndex={0}
      onClick={(e) => {
        // Toggle on clicks anywhere on the card *surface* (the headline/body
        // chrome) — but never on interactive sub-regions. In the expanded
        // state that means: bands, probe row, ask field, and conversation
        // exchanges keep their own click semantics; the headline area and
        // the explicit collapse handle drive the toggle. Symmetric with the
        // collapsed state where the same gesture expands.
        const t = e.target as HTMLElement;
        if (t.closest(".card-action")) return;
        if (t.closest(".card-actions")) return;
        if (t.closest(".card-action-menu")) return;
        if (t.closest(".card-band")) return;
        if (t.closest(".probe-row")) return;
        if (t.closest(".card-ask-wrap")) return;
        if (t.closest(".conversation")) return;
        if (t.closest(".card-footer")) return;
        if (t.closest("[data-probe-id]")) return;
        onFocus();
        onToggle();
      }}
      onFocus={onFocus}
    >
      {!expanded ? (
        <div className="card-body">
          <CardCore card={card} justArrived={justArrived} />
        </div>
      ) : null}

      <div className="card-detail" aria-hidden={!expanded}>
        <div
          ref={detailInnerRef}
          className={
            "card-detail-inner revision" +
            (scrollFlags.above ? " has-scroll-above" : "") +
            (scrollFlags.below ? " has-scroll-below" : "")
          }
          onClick={handleProbeClick}
        >
          {/* Sticky top-right collapse pill — discoverable, always reachable,
              decoupled from the triage action row. Mirrors the "▸ Ask why"
              CTA from the collapsed footer at the inverse position. */}
          <button
            type="button"
            className="card-collapse-handle"
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
            aria-label="Collapse card"
            title="Collapse (Esc)"
          >
            <span className="chevron-up" aria-hidden="true">▴</span>
            <span>Collapse</span>
          </button>
          <div className="card-body in-detail">
            <CardCore card={card} justArrived={justArrived} />
          </div>

          {/* UX-3 expanded bands: diff → signals → reasoning →
              uncertainty. The probe/ask is now the *fallback* for
              questions the bands didn't already answer. */}
          {card.detail?.diff ? <DiffBand diff={card.detail.diff} /> : null}
          {card.detail?.signals && card.detail.signals.length > 0 ? (
            <SignalsBand signals={card.detail.signals} />
          ) : null}
          {card.detail?.reasoning && card.detail.reasoning.length > 0 ? (
            <ReasoningBand groups={card.detail.reasoning} />
          ) : null}
          {card.detail?.falsifier ? (
            <UncertaintyBand
              recommendationId={card.id}
              confidencePct={parseConfidencePct(card.epistemic_line)}
              falsifier={card.detail.falsifier}
              calibration={card.detail.calibration}
              isWatched={card.detail.is_watched ?? false}
            />
          ) : null}

          {!archived && visibleChips.length > 0 ? (
            <section className="probe-row">
              <div className="probe-row-label">What do you want to understand?</div>
              <div className="probe-chips">
                {visibleChips.map((chip) => (
                  <button
                    key={chip.id}
                    className="probe-chip"
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleChipClick(chip);
                    }}
                  >
                    {chip.text}
                  </button>
                ))}
              </div>
            </section>
          ) : null}

          {!archived ? (
            <section className="card-ask-wrap">
              <div className="card-ask">
                <input
                  ref={askRef}
                  type="text"
                  className="card-ask-input"
                  placeholder="Or ask anything about this…"
                  value={askText}
                  autoComplete="off"
                  spellCheck
                  onChange={(e) => setAskText(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      handleAskSubmit();
                    } else if (e.key === "Escape") {
                      e.currentTarget.blur();
                    }
                  }}
                  onClick={(e) => e.stopPropagation()}
                />
                <button
                  className="card-ask-submit"
                  aria-label="Submit question"
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleAskSubmit();
                  }}
                >
                  ↵
                </button>
              </div>
            </section>
          ) : null}

          {renderedExchanges.length > 0 ? (
            <div className="conversation">
              {conversation?.last_probed_at && conversation.exchanges.length > 0 ? (
                <LastProbedMarker iso={conversation.last_probed_at} />
              ) : null}
              {renderedExchanges.map((ex, idx) => {
                const isLast = idx === renderedExchanges.length - 1;
                if ("pending" in ex) {
                  return (
                    <Exchange
                      key={ex.id}
                      ref={isLast ? lastExchangeRef : undefined}
                      exchange={null}
                      pending={pending}
                      onFollowUp={handleChipClick}
                    />
                  );
                }
                return (
                  <Exchange
                    key={ex.id}
                    ref={isLast ? lastExchangeRef : undefined}
                    exchange={ex}
                    pending={null}
                    onFollowUp={handleChipClick}
                  />
                );
              })}
            </div>
          ) : null}
        </div>

        <footer className="card-footer sticky">
          <ActionRow
            primary={primary}
            approveLabel={approveLabel}
            notNowActions={notNowActions}
            menuOpen={menuOpen}
            menuTriggerRef={menuTriggerRef}
            menuRef={menuRef}
            onApprove={() => primary && onTriage(primary)}
            onDiscuss={handleDiscuss}
            onToggleMenu={() => setMenuOpen((v) => !v)}
            onMenuItem={handleMenuItem}
          />
        </footer>
      </div>

      {!expanded ? (
        <footer className="card-footer">
          <button
            className="expand-cta"
            onClick={(e) => {
              e.stopPropagation();
              onFocus();
              onToggle();
            }}
            type="button"
          >
            <span className="chevron">▸</span>
            <span>{expandLabel}</span>
          </button>
          <ActionRow
            primary={primary}
            approveLabel={approveLabel}
            notNowActions={notNowActions}
            menuOpen={menuOpen}
            menuTriggerRef={menuTriggerRef}
            menuRef={menuRef}
            onApprove={() => primary && onTriage(primary)}
            onDiscuss={handleDiscuss}
            onToggleMenu={() => setMenuOpen((v) => !v)}
            onMenuItem={handleMenuItem}
          />
        </footer>
      ) : null}
    </article>
  );
});

// CardCore — the content stack used in both collapsed and expanded
// views. headline → subtitle (kind_label, demoted) → proposal →
// supporting (when present) → epistemic. The kind taxonomy
// ("Commitment shift" / "Goal direction" / etc) is engineering jargon
// that doesn't deserve to lead the card; it's now a quiet caption
// under the headline. The legacy `stats` grid is rendered ONLY as a
// fallback when the server has not emitted an epistemic_line.
function CardCore({ card, justArrived }: { card: RecCardModel; justArrived?: boolean }) {
  const showNewTag = justArrived && card.tag?.kind === "new";
  return (
    <>
      <h2
        className="card-headline"
        dangerouslySetInnerHTML={{ __html: card.headline_html }}
      />
      {card.kind_label || showNewTag ? (
        <div className="card-subtitle">
          {card.kind_label ? (
            <span className="card-subtitle-kind">{card.kind_label}</span>
          ) : null}
          {showNewTag ? (
            <span className="tag-new">{card.tag!.label}</span>
          ) : null}
        </div>
      ) : null}
      {card.proposed_change_text ? (
        <p className="card-proposal">
          <span className="card-proposal-target">{card.proposed_change_text}</span>
        </p>
      ) : null}
      {card.supporting_html ? (
        <p
          className="card-supporting"
          dangerouslySetInnerHTML={{ __html: card.supporting_html }}
        />
      ) : null}
      {card.epistemic_line ? (
        <p className="card-epistemic">{card.epistemic_line}</p>
      ) : card.stats && card.stats.length > 0 ? (
        <div className="card-stats">
          {card.stats.slice(0, 3).map((s, i) => (
            <div className="stat-cell" key={i}>
              <span className="stat-label">{s.label}</span>
              <span
                className={
                  "stat-value" +
                  (/^[\d$.,%/\s+−↑↓-]+$/.test(s.value) ? "" : " text") +
                  (s.tone && s.tone !== "default" ? ` ${s.tone}` : "")
                }
              >
                {s.value}
              </span>
            </div>
          ))}
        </div>
      ) : null}
    </>
  );
}

// Action palette — Approve / Discuss / Not now ▾.
//
// Approve runs the primary triage action (typically "act") with a
// verb-specialized label ("Close c-5", "Schedule 1:1", "Add to Q2").
// Discuss is a UI-only affordance: expand and focus the ask field —
// it doesn't write a triage. "Not now ▾" gathers hold/snooze/route/
// dismiss into one menu so the page reads as a list of yes/think/later
// instead of five symmetric buttons forcing a 5-way classification per
// card.
type ActionRowProps = {
  primary: TriageAction | undefined;
  approveLabel: string;
  notNowActions: TriageAction[];
  menuOpen: boolean;
  menuTriggerRef: React.Ref<HTMLButtonElement>;
  menuRef: React.Ref<HTMLDivElement>;
  onApprove: () => void;
  onDiscuss: () => void;
  onToggleMenu: () => void;
  onMenuItem: (a: TriageAction) => void;
};

function ActionRow({
  primary,
  approveLabel,
  notNowActions,
  menuOpen,
  menuTriggerRef,
  menuRef,
  onApprove,
  onDiscuss,
  onToggleMenu,
  onMenuItem,
}: ActionRowProps) {
  return (
    <div className="card-actions">
      {primary ? (
        <button
          className="card-action primary"
          onClick={(e) => {
            e.stopPropagation();
            onApprove();
          }}
          type="button"
        >
          <span className="key">{ACTION_KEY[primary]}</span>
          {approveLabel}
        </button>
      ) : null}
      <button
        className="card-action"
        onClick={(e) => {
          e.stopPropagation();
          onDiscuss();
        }}
        type="button"
      >
        <span className="key">/</span>
        Discuss
      </button>
      {notNowActions.length > 0 ? (
        <>
          <button
            ref={menuTriggerRef}
            className="card-action secondary-menu"
            onClick={(e) => {
              e.stopPropagation();
              onToggleMenu();
            }}
            type="button"
            aria-haspopup="menu"
            aria-expanded={menuOpen}
          >
            Not now
            <span className="caret">▾</span>
          </button>
          {menuOpen ? (
            <div
              ref={menuRef}
              className="card-action-menu"
              role="menu"
              onClick={(e) => e.stopPropagation()}
            >
              {notNowActions.map((a) => (
                <button
                  key={a}
                  type="button"
                  role="menuitem"
                  className={a === "dismiss" ? "item-danger" : undefined}
                  onClick={() => onMenuItem(a)}
                >
                  <span>{ACTION_LABEL[a]}</span>
                  <span className="item-key">{ACTION_KEY[a]}</span>
                </button>
              ))}
            </div>
          ) : null}
        </>
      ) : null}
    </div>
  );
}

const Exchange = forwardRef<
  HTMLElement,
  {
    exchange: CardExchange | null;
    pending: { probe_action: string; probe_text: string } | null;
    onFollowUp: (chip: ProbeChip) => void;
  }
>(function Exchange({ exchange, pending, onFollowUp }, ref) {
  const action = exchange?.probe_action ?? pending?.probe_action ?? "";
  const text = exchange?.probe_text ?? pending?.probe_text ?? "";
  return (
    <article
      ref={ref}
      className="exchange"
      data-exchange-id={exchange?.id}
      data-pending={pending ? "true" : undefined}
    >
      <header className="exchange-probe">
        <span className="probe-marker">↳</span>
        <span className="probe-action">{action}</span>
        <span className="probe-text">{text}</span>
        {exchange?.created_at ? (
          <span className="probe-time">{relativeTime(exchange.created_at)}</span>
        ) : null}
      </header>
      {exchange ? (
        <>
          <div
            className="exchange-response"
            dangerouslySetInnerHTML={{ __html: exchange.response_html }}
          />
          {exchange.follow_ups.length > 0 ? (
            <footer className="exchange-followups">
              {exchange.follow_ups.map((f) => (
                <FollowUpChip key={f.id} chip={f} onClick={onFollowUp} />
              ))}
            </footer>
          ) : null}
        </>
      ) : (
        <div className="exchange-response thinking" aria-live="polite">
          <span className="thinking-marker">⟢</span>
          <span className="thinking-text">Driftwood is thinking</span>
          <span className="thinking-dots" />
        </div>
      )}
    </article>
  );
});

function FollowUpChip({
  chip,
  onClick,
}: {
  chip: ProbeChip;
  onClick: (chip: ProbeChip) => void;
}) {
  return (
    <button
      className="followup-chip"
      type="button"
      onClick={(e) => {
        e.stopPropagation();
        onClick(chip);
      }}
    >
      {chip.text}
    </button>
  );
}

function LastProbedMarker({ iso }: { iso: string }) {
  return (
    <div className="last-probed-marker">Last probed {relativeTime(iso)}</div>
  );
}

// ──────────────────────────────────────────────────────────────────
// UX-3 expanded-card bands
// ──────────────────────────────────────────────────────────────────

function parseConfidencePct(epistemic: string | undefined): number | null {
  if (!epistemic) return null;
  const m = epistemic.match(/(\d+)%/);
  return m ? parseInt(m[1], 10) : null;
}

const REASONING_KIND_LABEL: Record<string, string> = {
  state: "STATE",
  pattern: "PATTERN",
  pattern_instance: "PATTERN",
  prediction: "PREDICTION",
  concern: "CONCERN",
  hypothesis: "HYPOTHESIS",
  capability_assessment: "CAPABILITY",
  market_assessment: "MARKET",
  environmental_trend: "TREND",
  relation: "RELATION",
};

// "If you approve" band — answers the user's actual question: what
// happens when I click the dark Approve button? Renders the effect as
// a sentence ("State moves from X to Y") rather than a git-diff column,
// and is honest about no-op transitions ("Already at X; approving
// confirms this is the right call.").
function DiffBand({ diff }: { diff: DiffPanel }) {
  const { target_title, target_kind, current_state, to_state, operation, owner_name, created_at, days_idle, acceptance } = diff;
  const created = created_at ? formatShortDate(created_at) : null;
  const contextParts: string[] = [];
  if (owner_name) contextParts.push(`Owned by ${owner_name}`);
  if (created) contextParts.push(`created ${created}`);
  if (typeof days_idle === "number" && days_idle >= 1) {
    contextParts.push(`idle ${days_idle}d`);
  }
  return (
    <section className="card-band band-diff">
      <div className="band-label">If you approve</div>
      <p className="diff-effect">
        <DiffEffectSentence
          targetTitle={target_title}
          targetKind={target_kind}
          operation={operation}
          currentState={current_state}
          toState={to_state}
        />
      </p>
      {contextParts.length > 0 ? (
        <div className="diff-row diff-meta">{contextParts.join(" · ")}</div>
      ) : null}
      {acceptance ? (
        <div className="diff-row diff-acceptance">{truncate(acceptance, 220)}</div>
      ) : null}
    </section>
  );
}

// One natural-English sentence that combines entity + effect. The
// entity name is rendered inline as the subject so the user reads
// "what happens" without a separate header line that just names the
// entity already shown in the headline above.
function DiffEffectSentence({
  targetTitle,
  targetKind,
  operation,
  currentState,
  toState,
}: {
  targetTitle: string;
  targetKind: string;
  operation: string;
  currentState?: string;
  toState?: string;
}) {
  const kind = targetKind || "item";
  const Subject = (
    <>
      <em className="diff-target-name">"{targetTitle}"</em>{" "}
      <span className="diff-target-kind">{kind}</span>
    </>
  );

  if (operation === "transition") {
    if (currentState && toState && currentState !== toState) {
      return (
        <>
          The {Subject} moves from <span className="diff-from">{currentState}</span>{" "}
          <span className="diff-arrow">→</span>{" "}
          <span className="diff-to">{toState}</span>.
        </>
      );
    }
    if (currentState && toState && currentState === toState) {
      return (
        <>
          The {Subject} stays at <span className="diff-to">{toState}</span>. Your approval confirms this is the right call going forward.
        </>
      );
    }
    if (toState) {
      return (
        <>
          The {Subject} moves to <span className="diff-to">{toState}</span>.
        </>
      );
    }
    return <>The {Subject} transitions to the proposed state.</>;
  }
  if (operation === "archive") {
    return (
      <>
        The {Subject} gets <span className="diff-to">archived</span> — removed from active consideration.
      </>
    );
  }
  if (operation === "create") {
    return (
      <>
        A new {kind} is <span className="diff-to">created</span>: <em className="diff-target-name">"{targetTitle}"</em>.
      </>
    );
  }
  if (operation === "update") {
    return (
      <>
        The {Subject} is <span className="diff-to">updated</span> with the proposed change.
      </>
    );
  }
  return <>The proposed change is applied to {Subject}.</>;
}

function SignalsBand({ signals }: { signals: SignalRow[] }) {
  const [showAll, setShowAll] = useState(false);
  const visible = showAll ? signals : signals.slice(0, 3);
  const hidden = signals.length - visible.length;
  return (
    <section className="card-band band-signals">
      <div className="band-label">
        What I'm seeing
        {signals.length > 0 ? (
          <span className="band-count">
            {visible.length} of {signals.length}
          </span>
        ) : null}
      </div>
      <ul className="signal-list">
        {visible.map((s, i) => {
          const cleanSource = s.source.split(":")[0];
          const showAttr = s.attribution && s.attribution !== s.source && s.attribution !== cleanSource;
          return (
            <li key={s.observation_id ?? i} className="signal-row">
              <q className="signal-quote">{s.quote}</q>
              <div className="signal-meta">
                <span className="signal-source">{cleanSource}</span>
                <span className="signal-meta-sep">·</span>
                <span className="signal-date">{s.date_label}</span>
                {showAttr ? (
                  <>
                    <span className="signal-meta-sep">·</span>
                    <span className="signal-attr">{s.attribution}</span>
                  </>
                ) : null}
              </div>
            </li>
          );
        })}
      </ul>
      {hidden > 0 ? (
        <button
          type="button"
          className="signals-more"
          onClick={(e) => {
            e.stopPropagation();
            setShowAll(true);
          }}
        >
          {hidden} more {hidden === 1 ? "signal" : "signals"} →
        </button>
      ) : null}
    </section>
  );
}

function ReasoningBand({ groups }: { groups: ReasoningGroup[] }) {
  const rows = useMemo(() => {
    const out: { kind: string; label: string; natural: string; confidence: number; id?: string }[] = [];
    for (const g of groups) {
      const label = g.label || REASONING_KIND_LABEL[g.kind] || g.kind.toUpperCase();
      for (const it of g.items) {
        out.push({ kind: g.kind, label, natural: it.natural, confidence: it.confidence, id: it.model_id });
      }
    }
    return out.slice(0, 6);
  }, [groups]);
  if (rows.length === 0) return null;
  return (
    <section className="card-band band-reasoning">
      <div className="band-label">How it adds up</div>
      <ul className="reasoning-list">
        {rows.map((r, i) => (
          <li key={r.id ?? i} className="reasoning-row">
            <p className="reasoning-natural">{r.natural}</p>
            <div className="reasoning-meta">
              <span className="reasoning-kind">{r.label}</span>
              <span className="reasoning-meta-sep">·</span>
              <span className="reasoning-conf">{Math.round(r.confidence * 100)}% confident</span>
            </div>
          </li>
        ))}
      </ul>
    </section>
  );
}

function UncertaintyBand({
  recommendationId,
  confidencePct,
  falsifier,
  calibration,
  isWatched: initialWatched,
}: {
  recommendationId: string;
  confidencePct: number | null;
  falsifier: Falsifier;
  calibration: Calibration | undefined;
  isWatched: boolean;
}) {
  const [watching, setWatching] = useState(initialWatched);
  const [pending, setPending] = useState(false);

  // Reflect server-pushed state if it changes (e.g. SSE refresh).
  useEffect(() => {
    setWatching(initialWatched);
  }, [initialWatched]);

  const toggleWatch = useCallback(async () => {
    if (pending) return;
    setPending(true);
    const next = !watching;
    setWatching(next); // optimistic
    try {
      if (next) {
        if (!falsifier.predicate) throw new Error("no predicate");
        await postWatch(recommendationId, falsifier.predicate);
      } else {
        await deleteWatch(recommendationId);
      }
    } catch {
      setWatching(!next); // revert
    } finally {
      setPending(false);
    }
  }, [pending, watching, falsifier.predicate, recommendationId]);

  const calibLine = useMemo(() => {
    if (!calibration) return null;
    const { kind_label, hit_rate, n_prior, window_days } = calibration;
    if (n_prior < 3 || hit_rate == null) {
      return (
        <span className="calibration-line">
          Not enough prior {kind_label} recs to calibrate yet ({n_prior} in {window_days} days).
        </span>
      );
    }
    const pct = Math.round(hit_rate * 100);
    return (
      <span className="calibration-line">
        On <strong>{n_prior} prior {kind_label} recs</strong> I was right <strong>{pct}%</strong>.
      </span>
    );
  }, [calibration]);

  return (
    <section className="card-band band-uncertainty">
      <div className="band-label">Where I'm uncertain</div>
      <p className="uncertainty-conf">
        {confidencePct !== null ? (
          <span className="uncertainty-pct">{confidencePct}%</span>
        ) : null}
        <span className="uncertainty-text">confident overall.</span>
      </p>
      <p className="uncertainty-falsifier">
        <span className="falsifier-prefix">Would revise if:</span>
        <span className="falsifier-text">{falsifier.text}.</span>
      </p>
      <div className="uncertainty-actions">
        {falsifier.watchable ? (
          <button
            type="button"
            className={"watch-btn" + (watching ? " watching" : "")}
            onClick={(e) => {
              e.stopPropagation();
              void toggleWatch();
            }}
            disabled={pending}
            aria-pressed={watching}
          >
            <span className="watch-icon">{watching ? "●" : "◷"}</span>
            {watching ? "Watching" : "Watch for revision"}
          </button>
        ) : null}
        {calibLine}
      </div>
    </section>
  );
}

function formatShortDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function truncate(s: string, n: number): string {
  if (s.length <= n) return s;
  return s.slice(0, n - 1).trimEnd() + "…";
}

function relativeTime(iso: string): string {
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return "";
  const delta = Date.now() - t;
  if (delta < 60_000) return "just now";
  const min = Math.floor(delta / 60_000);
  if (min < 60) return `${min} min ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr} hr ago`;
  const day = Math.floor(hr / 24);
  if (day === 1) return "yesterday";
  if (day < 7) return `${day} days ago`;
  return new Date(iso).toLocaleDateString();
}
