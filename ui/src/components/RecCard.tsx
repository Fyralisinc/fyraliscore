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
} from "@/api/today-types";
import { useConversation } from "@/hooks/useConversation";

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

const ACTION_LABEL: Record<TriageAction, string> = {
  act: "Act",
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

// Driftwood revision (DRIFTWOOD_TODAY_CARD_REVISION.md):
// - Collapsed card unchanged.
// - Expanded card replaces the five static sections with a probe-driven
//   conversation: clickable <probe> phrases in the body, substrate-emitted
//   chips, in-card Ask field, exchange list, sticky footer.
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

  const expandLabel = card.expand_cta ?? "Open";
  const primary = card.actions[0];
  const secondaries = card.actions.slice(1);

  const [askText, setAskText] = useState("");
  const askRef = useRef<HTMLInputElement>(null);
  const detailInnerRef = useRef<HTMLDivElement>(null);
  const lastExchangeRef = useRef<HTMLElement>(null);
  const [scrollFlags, setScrollFlags] = useState({ above: false, below: false });

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

  // Wire phrase clicks via event delegation so we don't have to attach
  // listeners to each <probe> element (which is rendered via
  // dangerouslySetInnerHTML and thus opaque to React).
  const handleProbeClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      const target = (e.target as HTMLElement).closest<HTMLElement>(
        "[data-probe-id]"
      );
      if (!target) return;
      e.stopPropagation();
      const probeId = target.dataset.probeId!;
      // If this phrase was already probed, scroll to its existing
      // exchange instead of generating a new one.
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
      // Optimistic pulse on the clicked phrase.
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
    // Refocus so the user can keep asking.
    setTimeout(() => askRef.current?.focus(), 0);
  }, [askText, probe]);

  // Scroll new exchanges (and the pending placeholder) into view.
  useEffect(() => {
    if (!expanded) return;
    const node = lastExchangeRef.current;
    if (!node) return;
    node.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [conversation?.exchanges.length, pending, expanded]);

  // Track whether scrollable content extends above/below the visible
  // area so we can render the gradient affordances per spec §6.6.
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

  // Suppress used chips per session, AND chips already probed in
  // earlier sessions (loaded from server).
  const visibleChips = useMemo(() => {
    const used = new Set(conversation?.used_chip_ids ?? []);
    return probeChips.filter((c) => !used.has(c.id));
  }, [probeChips, conversation?.used_chip_ids]);

  // Compose the list of "rendered exchanges" — persisted ones plus the
  // optimistic pending placeholder if any.
  const renderedExchanges: (CardExchange | { pending: true; id: string })[] = useMemo(() => {
    const list: (CardExchange | { pending: true; id: string })[] = [
      ...(conversation?.exchanges ?? []),
    ];
    if (pending) {
      list.push({ pending: true, id: pending.pending_id });
    }
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
        const t = e.target as HTMLElement;
        if (t.closest(".card-action")) return;
        if (t.closest(".card-actions")) return;
        if (t.closest(".card-detail-inner")) return;
        if (t.closest(".card-footer")) return;
        onFocus();
        onToggle();
      }}
      onFocus={onFocus}
    >
      <header className="card-header">
        <div className="card-header-left">
          <span className="card-kind">{card.kind_label}</span>
          {card.meta ? <span className="card-meta">{card.meta}</span> : null}
        </div>
        {card.tag ? (
          <span className={card.tag.kind === "new" ? "tag-new" : "tag-quiet"}>
            {card.tag.label}
          </span>
        ) : null}
      </header>

      {/* Card body — collapsed view. The expanded card re-renders the
          headline/supporting/stats inside the scrollable inner panel
          so they scroll with the conversation. */}
      {!expanded ? (
        <div className="card-body">
          <h2
            className="card-headline"
            dangerouslySetInnerHTML={{ __html: card.headline_html }}
          />
          {card.supporting_html ? (
            <p
              className="card-supporting"
              dangerouslySetInnerHTML={{ __html: card.supporting_html }}
            />
          ) : null}
          {card.stats && card.stats.length > 0 ? (
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
          {/* Body inside the scroll area. */}
          <div className="card-body in-detail">
            <h2
              className="card-headline"
              dangerouslySetInnerHTML={{ __html: card.headline_html }}
            />
            {card.supporting_html ? (
              <p
                className="card-supporting"
                dangerouslySetInnerHTML={{ __html: card.supporting_html }}
              />
            ) : null}
            {card.stats && card.stats.length > 0 ? (
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
          </div>

          {/* Probe chips — substrate-suggested probes. Hidden in
              archived (post-resolution) view and once all chips used. */}
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

          {/* In-card Ask field. */}
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

          {/* Conversation panel. */}
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

        {/* Sticky footer — actions remain reachable as conversation grows. */}
        <footer className="card-footer sticky">
          <button
            className="expand-cta collapse"
            onClick={(e) => {
              e.stopPropagation();
              onToggle();
            }}
            type="button"
          >
            <span className="chevron rotated">▾</span>
            <span>Collapse</span>
          </button>
          <div className="card-actions">
            {primary ? (
              <button
                className="card-action primary"
                onClick={(e) => {
                  e.stopPropagation();
                  onTriage(primary);
                }}
                type="button"
              >
                <span className="key">{ACTION_KEY[primary]}</span>
                {ACTION_LABEL[primary]}
              </button>
            ) : null}
            {secondaries.map((a) => (
              <button
                key={a}
                className="card-action"
                onClick={(e) => {
                  e.stopPropagation();
                  if (a === "dismiss") {
                    const reason = window.prompt(
                      "Tell me why you disagree (so I can recalibrate)",
                      ""
                    );
                    if (reason && reason.trim()) {
                      onTriage(a, { ask: reason });
                    }
                    return;
                  }
                  onTriage(a);
                }}
                type="button"
              >
                <span className="key">{ACTION_KEY[a]}</span>
                {ACTION_LABEL[a]}
              </button>
            ))}
          </div>
        </footer>
      </div>

      {/* Collapsed-state footer (expand CTA + actions) — only when the
          card is collapsed; the expanded view uses the sticky footer
          inside .card-detail. */}
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
          <div className="card-actions">
            {primary ? (
              <button
                className="card-action primary"
                onClick={(e) => {
                  e.stopPropagation();
                  onTriage(primary);
                }}
                type="button"
              >
                <span className="key">{ACTION_KEY[primary]}</span>
                {ACTION_LABEL[primary]}
              </button>
            ) : null}
            {secondaries.map((a) => (
              <button
                key={a}
                className="card-action"
                onClick={(e) => {
                  e.stopPropagation();
                  if (a === "dismiss") {
                    const reason = window.prompt(
                      "Tell me why you disagree (so I can recalibrate)",
                      ""
                    );
                    if (reason && reason.trim()) {
                      onTriage(a, { ask: reason });
                    }
                    return;
                  }
                  onTriage(a);
                }}
                type="button"
              >
                <span className="key">{ACTION_KEY[a]}</span>
                {ACTION_LABEL[a]}
              </button>
            ))}
          </div>
        </footer>
      ) : null}
    </article>
  );
});

// One persisted exchange OR a pending placeholder that shows the
// "Driftwood is thinking" indicator while the response generates.
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
