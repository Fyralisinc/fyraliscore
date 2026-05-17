// Ask Fyralis contextual strip — spec §7.8 + §13.
// Lives inside the expanded Focused Review card so the user can ask
// grounded follow-up questions without leaving Today. The strip uses
// the selected proposed change as automatic context (§13.3).
//
// Backend is stubbed (api/ask-client.ts). When the real /api/ask
// endpoint lands the call swap is the only change required.

import { useState } from "react";

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

  const suggestions = getSuggestedPrompts(delta);

  async function submit(text: string) {
    const q = text.trim();
    if (!q || loading) return;
    setLoading(true);
    setLastQuestion(q);
    try {
      const a = await askFyralis(delta, q);
      setAnswer(a);
    } catch {
      setAnswer({
        type: "unsupported_answer",
        title: "Ask is unavailable",
        body: "Couldn't reach Ask Fyralis right now. Try again in a moment.",
      });
    } finally {
      setLoading(false);
      setPrompt("");
    }
  }

  return (
    <section
      className="tdv2-ask"
      data-testid={`ask-strip-${delta.id}`}
      aria-label="Ask Fyralis about this proposed change"
    >
      <div className="tdv2-ask__label">Ask Fyralis about this change</div>
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
          placeholder="Ask about this proposed change…"
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          disabled={loading}
          data-testid={`ask-input-${delta.id}`}
          aria-label="Ask Fyralis about this proposed change"
        />
        <button
          type="submit"
          className="tdv2-btn tdv2-btn--secondary"
          disabled={loading || prompt.trim().length === 0}
          data-testid={`ask-submit-${delta.id}`}
        >
          {loading ? "Asking…" : "Ask"}
        </button>
      </form>
      <div className="tdv2-ask__suggestions">
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
