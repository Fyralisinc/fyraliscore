import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useLocation, useNavigate } from "react-router-dom";

import {
  actOnAskProposedChange,
  createAskSession,
  expandAskEvidence,
  sendAskTurn,
  type AskEvidenceLedgerItem,
  type AskScope,
  type AskSession,
  type AskStructuredPayload,
  type AskTurnResponse,
  type EvidenceExpansionResponse,
} from "@/api/ask-client";

interface AskOverlayContextValue {
  isOpen: boolean;
  openAsk: (options?: OpenAskOptions) => void;
  closeAsk: () => void;
}

interface OpenAskOptions {
  scope?: AskScope;
  prompt?: string;
}

interface AskTurnView {
  id: string;
  query: string;
  response: AskTurnResponse;
  expandedEvidence?: EvidenceExpansionResponse;
}

const AskOverlayContext = createContext<AskOverlayContextValue>({
  isOpen: false,
  openAsk: () => undefined,
  closeAsk: () => undefined,
});

export function useAskOverlay(): AskOverlayContextValue {
  return useContext(AskOverlayContext);
}

export function AskOverlayProvider({ children }: { children: ReactNode }) {
  const location = useLocation();
  const navigate = useNavigate();
  const routeScope = useMemo(() => deriveScope(location), [location]);

  const [isOpen, setIsOpen] = useState(false);
  const [scope, setScope] = useState<AskScope>(routeScope);
  const [session, setSession] = useState<AskSession | null>(null);
  const [draft, setDraft] = useState("");
  const [turns, setTurns] = useState<AskTurnView[]>([]);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!isOpen) setScope(routeScope);
  }, [isOpen, routeScope]);

  const openAsk = useCallback((options?: OpenAskOptions) => {
    setScope(options?.scope ?? routeScope);
    if (options?.prompt) setDraft(options.prompt);
    setIsOpen(true);
    window.setTimeout(() => inputRef.current?.focus(), 0);
  }, [routeScope]);

  const closeAsk = useCallback(() => {
    setIsOpen(false);
    setError(null);
    if (location.search.includes("ask=1")) {
      const params = new URLSearchParams(location.search);
      params.delete("ask");
      navigate(
        `${location.pathname}${params.toString() ? `?${params.toString()}` : ""}`,
        { replace: true },
      );
    }
  }, [location.pathname, location.search, navigate]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    if (params.get("ask") === "1") {
      openAsk();
    }
  }, [location.search, openAsk]);

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      const key = event.key.toLowerCase();
      if ((event.metaKey || event.ctrlKey) && key === "k") {
        event.preventDefault();
        openAsk();
      } else if (event.key === "Escape" && isOpen) {
        event.preventDefault();
        closeAsk();
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [closeAsk, isOpen, openAsk]);

  async function submit(text: string) {
    const query = text.trim();
    if (!query || pending) return;
    setPending(true);
    setError(null);
    try {
      const activeSession = session ?? await createAskSession({
        initial_scope: scope,
        source_route: `${location.pathname}${location.search}`,
      });
      if (!session) setSession(activeSession);
      const response = await sendAskTurn(activeSession.id, {
        query,
        scope,
      });
      setSession(response.session);
      setTurns((existing) => [
        ...existing,
        { id: response.message_id, query, response },
      ]);
      setDraft("");
    } catch {
      setError("Ask Fyralis could not answer right now.");
    } finally {
      setPending(false);
    }
  }

  async function expandEvidence(turnId: string, retrievalRunId: string) {
    try {
      const expandedEvidence = await expandAskEvidence(retrievalRunId);
      setTurns((existing) =>
        existing.map((turn) =>
          turn.id === turnId ? { ...turn, expandedEvidence } : turn,
        ),
      );
    } catch {
      setError("Evidence expansion failed.");
    }
  }

  async function actOnChange(
    turnId: string,
    changeId: string,
    action: "accept" | "reject" | "delegate" | "deep_review",
  ) {
    try {
      const { change } = await actOnAskProposedChange(changeId, { action });
      setTurns((existing) =>
        existing.map((turn) => {
          if (turn.id !== turnId) return turn;
          const nextPayload: AskStructuredPayload = {
            ...turn.response.payload,
            possible_state_change: change,
          };
          return {
            ...turn,
            response: {
              ...turn.response,
              payload: nextPayload,
            },
          };
        }),
      );
    } catch {
      setError("State change action failed.");
    }
  }

  const value = useMemo(
    () => ({ isOpen, openAsk, closeAsk }),
    [closeAsk, isOpen, openAsk],
  );

  return (
    <AskOverlayContext.Provider value={value}>
      {children}
      {isOpen ? (
        <div className="ask-overlay" role="dialog" aria-modal="true" aria-label="Ask Fyralis">
          <button
            type="button"
            className="ask-overlay__backdrop"
            aria-label="Close Ask Fyralis"
            onClick={closeAsk}
          />
          <section className="ask-overlay__panel" data-testid="ask-overlay">
            <header className="ask-overlay__header">
              <div>
                <p className="ask-overlay__eyebrow">Ask Fyralis</p>
                <h2 className="ask-overlay__title">{scope.label}</h2>
              </div>
              <div className="ask-overlay__header-actions">
                <select
                  className="ask-overlay__scope"
                  value={scope.type}
                  aria-label="Ask scope"
                  onChange={(event) => {
                    const next = scopeForType(event.target.value as AskScope["type"], routeScope);
                    setScope(next);
                  }}
                >
                  <option value="current_object">Current object</option>
                  <option value="current_page">Current page</option>
                  <option value="role_view">My role view</option>
                  <option value="whole_company">Whole company</option>
                  <option value="custom">Custom</option>
                </select>
                <button
                  type="button"
                  className="ask-overlay__icon-btn"
                  onClick={closeAsk}
                  aria-label="Close"
                >
                  <CloseIcon />
                </button>
              </div>
            </header>

            <div className="ask-overlay__body">
              {turns.length === 0 ? (
                <div className="ask-overlay__empty" data-testid="ask-overlay-empty">
                  <button type="button" onClick={() => void submit("What is most at risk here?")}>
                    What is most at risk here?
                  </button>
                  <button type="button" onClick={() => void submit("What changed since last week?")}>
                    What changed since last week?
                  </button>
                  <button type="button" onClick={() => void submit("What evidence is weakest?")}>
                    What evidence is weakest?
                  </button>
                </div>
              ) : null}
              {turns.map((turn) => (
                <article className="ask-overlay__turn" key={turn.id}>
                  <p className="ask-overlay__question">{turn.query}</p>
                  <AnswerCard
                    turn={turn}
                    onExpandEvidence={() => void expandEvidence(turn.id, turn.response.retrieval_run_id)}
                    onAct={(changeId, action) => void actOnChange(turn.id, changeId, action)}
                  />
                </article>
              ))}
              {pending ? (
                <div className="ask-overlay__thinking" role="status">
                  Reading Synthesis…
                </div>
              ) : null}
            </div>

            {error ? <p className="ask-overlay__error" role="alert">{error}</p> : null}

            <form
              className="ask-overlay__composer"
              onSubmit={(event) => {
                event.preventDefault();
                void submit(draft);
              }}
            >
              <input
                ref={inputRef}
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                placeholder="Ask about this surface…"
                aria-label="Ask Fyralis"
                disabled={pending}
              />
              <button
                type="submit"
                className="ask-overlay__send"
                aria-label="Send"
                disabled={pending || draft.trim().length === 0}
              >
                <SendIcon />
              </button>
            </form>
          </section>
        </div>
      ) : null}
    </AskOverlayContext.Provider>
  );
}

function AnswerCard({
  turn,
  onExpandEvidence,
  onAct,
}: {
  turn: AskTurnView;
  onExpandEvidence: () => void;
  onAct: (
    changeId: string,
    action: "accept" | "reject" | "delegate" | "deep_review",
  ) => void;
}) {
  const payload = turn.response.payload;
  const expanded = turn.expandedEvidence;
  return (
    <div className="ask-overlay__answer">
      <div className="ask-overlay__answer-head">
        <span className="ask-overlay__mode">{labelMode(turn.response.mode)}</span>
        <span className="ask-overlay__confidence">
          {Math.round(payload.confidence * 100)}% confidence
        </span>
      </div>
      <p className="ask-overlay__answer-text">{payload.answer}</p>

      <AnswerSection title="Why Fyralis believes this" items={payload.why} />
      <AnswerSection title="Counterevidence" items={payload.counterevidence} />
      <AnswerSection title="Impact" items={payload.impact} />
      <AnswerSection title="Recommended action" items={payload.recommended_actions} />
      <AnswerSection title="Unknowns" items={payload.unknowns} />

      {payload.related_nodes.length > 0 ? (
        <section className="ask-overlay__section">
          <h3>Related nodes</h3>
          <div className="ask-overlay__nodes">
            {payload.related_nodes.map((node) => (
              <span key={node.id}>{node.label}</span>
            ))}
          </div>
        </section>
      ) : null}

      <section className="ask-overlay__section">
        <div className="ask-overlay__section-head">
          <h3>Evidence</h3>
          <button type="button" onClick={onExpandEvidence}>
            Expand evidence
          </button>
        </div>
        <EvidenceList items={expanded?.evidence ?? payload.evidence} />
        {expanded?.omitted && expanded.omitted.length > 0 ? (
          <div className="ask-overlay__omitted">
            <p>Omission ledger</p>
            <EvidenceList items={expanded.omitted} />
          </div>
        ) : payload.omitted_evidence_count > 0 ? (
          <p className="ask-overlay__omitted-count">
            {payload.omitted_evidence_count} omitted evidence item(s)
          </p>
        ) : null}
      </section>

      {payload.possible_state_change ? (
        <section className="ask-overlay__state-change">
          <div>
            <h3>Possible state change</h3>
            <p>{String(payload.possible_state_change.proposed_op.op ?? "Validate synthesis gap")}</p>
            <span>{payload.possible_state_change.status.replaceAll("_", " ")}</span>
          </div>
          <div className="ask-overlay__state-actions">
            <button
              type="button"
              onClick={() => onAct(payload.possible_state_change!.id, "accept")}
              disabled={payload.possible_state_change.status !== "proposed"}
            >
              Queue validation
            </button>
            <button
              type="button"
              onClick={() => onAct(payload.possible_state_change!.id, "deep_review")}
              disabled={payload.possible_state_change.status !== "proposed"}
            >
              Deep review
            </button>
            <button
              type="button"
              onClick={() => onAct(payload.possible_state_change!.id, "reject")}
              disabled={payload.possible_state_change.status !== "proposed"}
            >
              Reject
            </button>
          </div>
        </section>
      ) : null}
    </div>
  );
}

function AnswerSection({ title, items }: { title: string; items: string[] }) {
  if (!items.length) return null;
  return (
    <section className="ask-overlay__section">
      <h3>{title}</h3>
      <ul>
        {items.map((item, idx) => (
          <li key={`${title}-${idx}`}>{item}</li>
        ))}
      </ul>
    </section>
  );
}

function EvidenceList({ items }: { items: AskEvidenceLedgerItem[] }) {
  if (!items.length) {
    return <p className="ask-overlay__muted">No accessible evidence returned.</p>;
  }
  return (
    <ul className="ask-overlay__evidence-list">
      {items.map((item) => (
        <li key={item.id}>
          <span>{item.source_kind}</span>
          <p>{item.summary}</p>
          {item.omitted_reason ? <em>{item.omitted_reason.replaceAll("_", " ")}</em> : null}
        </li>
      ))}
    </ul>
  );
}

function deriveScope(location: ReturnType<typeof useLocation>): AskScope {
  const params = new URLSearchParams(location.search);
  if (location.pathname.startsWith("/today") && params.get("review")) {
    return baseScope("current_object", "Current review item", {
      review_id: params.get("review"),
    });
  }
  if (location.pathname.startsWith("/today")) {
    return baseScope("current_page", "Today review");
  }
  if (location.pathname.startsWith("/model")) {
    return baseScope("current_page", "Company model");
  }
  if (location.pathname.startsWith("/forecasts")) {
    return baseScope("current_page", "Forecasts");
  }
  if (location.pathname.startsWith("/ledger")) {
    return baseScope("current_page", "Ledger");
  }
  return baseScope("current_page", "Current page");
}

function scopeForType(type: AskScope["type"], routeScope: AskScope): AskScope {
  if (type === "current_page") return baseScope("current_page", routeScope.label);
  if (type === "current_object") return routeScope.type === "current_object"
    ? routeScope
    : baseScope("current_object", routeScope.label);
  if (type === "role_view") return baseScope("role_view", "My role view");
  if (type === "whole_company") return baseScope("whole_company", "Whole company");
  return baseScope("custom", "Custom scope");
}

function baseScope(
  type: AskScope["type"],
  label: string,
  filters: Record<string, unknown> = {},
): AskScope {
  return {
    type,
    label,
    root_node_ids: [],
    related_entity_ids: [],
    filters,
    access_mode: "full",
  };
}

function labelMode(mode: string): string {
  return mode.replaceAll("_", " ");
}

function CloseIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M4 4l8 8M12 4l-8 8" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" />
    </svg>
  );
}

function SendIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M3 13 13 3M5 3h8v8" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
