// Ask Fyralis contextual strip — appears inside the focused review.
//
//   ASK FYRALIS ABOUT THIS CHANGE
//   [Why now?] [What if I wait?] [Who should own?] [What's weakest?] [What if we escalate?]
//   ┌────────────────────────────────────────────────────────────────┐
//   │ Ask a question or request...                              [ ↗ ] │
//   └────────────────────────────────────────────────────────────────┘
//   Fyralis uses your company model and connected sources.   View conversation history

import { useEffect, useRef, useState } from "react";

import type { DecisionDelta } from "@/api/today-page-types";
import {
  askFyralis,
  getSuggestedPrompts,
  type AskAnswer,
} from "@/api/ask-client";

interface Props {
  delta: DecisionDelta;
}

export function AskFyralisStrip({ delta }: Props) {
  const [prompt, setPrompt] = useState("");
  const [answer, setAnswer] = useState<AskAnswer | null>(null);
  const [lastQuestion, setLastQuestion] = useState("");
  const [loading, setLoading] = useState(false);

  const mountedRef = useRef(true);
  useEffect(() => () => {
    mountedRef.current = false;
  }, []);

  const suggestions = getSuggestedPrompts(delta);

  async function submit(text: string) {
    const q = text.trim();
    if (!q || loading) return;
    setLoading(true);
    setLastQuestion(q);
    try {
      const a = await askFyralis(delta, q);
      if (!mountedRef.current) return;
      setAnswer(a);
    } catch {
      if (!mountedRef.current) return;
      setAnswer({
        type: "unsupported_answer",
        title: "Ask is unavailable",
        body: "Couldn't reach Ask Fyralis right now. Try again in a moment.",
      });
    } finally {
      if (mountedRef.current) {
        setLoading(false);
        setPrompt("");
      }
    }
  }

  return (
    <section
      className="tdv2-ask"
      data-testid={`ask-strip-${delta.id}`}
      aria-label="Ask Fyralis about this proposed change"
    >
      <h3 className="tdv2-ask__heading">Ask Fyralis about this change</h3>
      <div className="tdv2-ask__chips">
        {suggestions.map((s) => (
          <button
            key={s.key}
            type="button"
            className="tdv2-ask__chip"
            onClick={() => void submit(s.label)}
            disabled={loading}
            data-testid={`ask-suggestion-${s.key}`}
          >
            {s.label}
          </button>
        ))}
      </div>
      <form
        className="tdv2-ask__form"
        onSubmit={(e) => {
          e.preventDefault();
          void submit(prompt);
        }}
      >
        <input
          type="text"
          className="tdv2-ask__input"
          placeholder="Ask a question or request..."
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          disabled={loading}
          data-testid={`ask-input-${delta.id}`}
          aria-label="Ask Fyralis about this proposed change"
        />
        <button
          type="submit"
          className="tdv2-ask__submit"
          disabled={loading || prompt.trim().length === 0}
          data-testid={`ask-submit-${delta.id}`}
          aria-label="Send question"
        >
          <SendArrow />
        </button>
      </form>
      <div className="tdv2-ask__foot">
        <span className="tdv2-ask__foot-copy">
          Fyralis uses your company model and connected sources to answer.
        </span>
        <a className="tdv2-ask__foot-link" href="#ask-history">
          View conversation history
        </a>
      </div>
      {answer ? (
        <article
          className="tdv2-ask__answer"
          data-testid={`ask-answer-${delta.id}`}
        >
          <p className="tdv2-ask__question">
            <span className="tdv2-ask__question-label">You asked</span>
            <span className="tdv2-ask__question-text">{lastQuestion}</span>
          </p>
          <h4 className="tdv2-ask__answer-title">{answer.title}</h4>
          <p className="tdv2-ask__answer-body">{answer.body}</p>
          {answer.basedOn && answer.basedOn.length > 0 ? (
            <div className="tdv2-ask__meta">
              <p className="tdv2-ask__meta-label">Based on</p>
              <ul className="tdv2-ask__meta-list">
                {answer.basedOn.map((b) => (
                  <li key={b}>{b}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {answer.mayBeMissing && answer.mayBeMissing.length > 0 ? (
            <div className="tdv2-ask__meta">
              <p className="tdv2-ask__meta-label">May be missing</p>
              <ul className="tdv2-ask__meta-list">
                {answer.mayBeMissing.map((b) => (
                  <li key={b}>{b}</li>
                ))}
              </ul>
            </div>
          ) : null}
          {answer.actions && answer.actions.length > 0 ? (
            <div className="tdv2-ask__actions">
              {answer.actions.map((a) => (
                <span key={a.label} className="tdv2-ask__action">
                  {a.label}
                </span>
              ))}
            </div>
          ) : null}
        </article>
      ) : null}
    </section>
  );
}

function SendArrow() {
  // Up-right arrow — the screenshot's send glyph.
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3.5 10.5L10.5 3.5" />
      <path d="M4.5 3.5h6v6" />
    </svg>
  );
}
