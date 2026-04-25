import { useCallback, useEffect, useRef, useState } from "react";
import { TopBar } from "@/components/TopBar";
import { Greeting } from "@/components/Greeting";
import { QueryGrid } from "@/components/QueryGrid";
import { Card } from "@/components/Card";
import { CardExpanded } from "@/components/CardExpanded";
import { CloseLine } from "@/components/CloseLine";
import { GroundInput } from "@/components/GroundInput";
import { ConversationTurn } from "@/components/ConversationTurn";
import { Icon } from "@/components/Icon";
import { useHome } from "@/hooks/useHome";
import { useAsk } from "@/hooks/useAsk";
import type { CardVerb } from "@/api/types";

// Three-region grid: TopBar / stage / GroundInput. The stage renders the
// full home surface + a `turns` stack below the close line. All state
// (active card, turns, input focus) lifts up here so the components stay
// pure renderers of their props.
export default function App() {
  const { home, loading, offline } = useHome();
  const { turns, ask, dismiss, save, markDone } = useAsk();
  const [activeCardId, setActiveCardId] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const stageRef = useRef<HTMLDivElement | null>(null);
  const cardRefs = useRef<Record<string, HTMLElement | null>>({});

  const onAsk = useCallback(
    (q: string, contextCardId?: string) => {
      ask(q, contextCardId);
    },
    [ask]
  );

  const onCardVerb = useCallback(
    (cardId: string, verb: CardVerb) => {
      setActiveCardId(null);
      if (verb.query_template) {
        onAsk(verb.query_template, cardId);
      }
    },
    [onAsk]
  );

  // Keyboard shortcuts per design doc §11.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setActiveCardId(null);
      }
      if (e.key === "/") {
        const active = document.activeElement as HTMLElement | null;
        if (active && active.tagName === "INPUT") return;
        e.preventDefault();
        inputRef.current?.focus();
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  // When a card expands, scroll the card into view smoothly (matches
  // /company-os.html lines 1085-1090).
  useEffect(() => {
    if (!activeCardId || !stageRef.current) return;
    const el = cardRefs.current[activeCardId];
    if (!el) return;
    const top = el.offsetTop - 20;
    stageRef.current.scrollTo({ top, behavior: "smooth" });
  }, [activeCardId]);

  return (
    <div className="app">
      <TopBar status={home?.status} />
      {offline ? (
        <div className="offline-banner">
          backend unreachable · showing last good state
        </div>
      ) : null}
      <main className="stage" ref={stageRef} id="stage">
        <div className="stage-inner">
          {loading && !home ? (
            <p
              className="greeting-body"
              style={{ color: "var(--ink-3)", opacity: 0.6 }}
            >
              Warming up…
            </p>
          ) : null}
          {home ? (
            <>
              <Greeting greeting={home.greeting} />
              <QueryGrid
                queries={home.query_grid.queries}
                onAsk={(q) => onAsk(q)}
              />
              <div className="sec-break">
                Today&rsquo;s signal{" "}
                <span style={{ color: "var(--ink-3)" }}>
                  · {home.cards.length} items
                </span>
              </div>
              <div className="cards">
                {home.cards.map((card) => (
                  <div key={card.id}>
                    <div
                      ref={(n) => {
                        cardRefs.current[card.id] = n;
                      }}
                    >
                      <Card
                        card={card}
                        active={activeCardId === card.id}
                        onToggle={() =>
                          setActiveCardId((prev) =>
                            prev === card.id ? null : card.id
                          )
                        }
                      />
                    </div>
                    <CardExpanded
                      card={card}
                      open={activeCardId === card.id}
                      onVerb={(v) => onCardVerb(card.id, v)}
                      onClose={() => setActiveCardId(null)}
                    />
                  </div>
                ))}
              </div>
              <CloseLine closeLine={home.close_line} />
              <div className="turns">
                {turns.map((t) => (
                  <ConversationTurn
                    key={t.turn_id}
                    turn={t}
                    onFollowUp={() => inputRef.current?.focus()}
                    onSave={async () => {
                      await save(t.turn_id);
                    }}
                    onDone={async () => {
                      await markDone(t.turn_id);
                      dismiss(t.turn_id);
                    }}
                  />
                ))}
              </div>
            </>
          ) : null}
        </div>
      </main>
      <GroundInput ref={inputRef} onSubmit={(q) => onAsk(q)} />
      {/* Pre-warm icon registry so switches to a new chip in the first
          render don't cost a component-load round trip. */}
      <div style={{ display: "none" }} aria-hidden="true">
        <Icon name="why" />
      </div>
    </div>
  );
}
