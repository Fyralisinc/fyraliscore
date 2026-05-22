// Ask Fyralis surface (spec §22). Lives inside the inspector, below
// the forecast detail. Prompt chips + free-form input + response card.

import { useCallback, useState } from "react";
import { askForecasts } from "@/api/forecasts-client";
import type {
  ForecastAskResponse,
  ForecastMode,
} from "@/api/forecasts-types";

const PROMPT_CHIPS = [
  "Why did this move?",
  "What if we assign an owner today?",
  "What is the downside if we wait 7 days?",
  "What would falsify this?",
  "Which intervention has the most leverage?",
  "Show similar past outcomes.",
];

export interface AskFyralisProps {
  selectedForecastId: string | null;
  visibleForecastIds?: string[];
  mode?: ForecastMode;
  horizonDays?: number;
  placeholder?: string;
}

export function AskFyralis({
  selectedForecastId,
  visibleForecastIds,
  mode = "horizon",
  horizonDays = 90,
  placeholder,
}: AskFyralisProps) {
  const [prompt, setPrompt] = useState("");
  const [pending, setPending] = useState(false);
  const [response, setResponse] = useState<ForecastAskResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const send = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed) return;
      setPending(true);
      setError(null);
      try {
        const r = await askForecasts({
          mode,
          prompt: trimmed,
          selected_forecast_id: selectedForecastId,
          visible_forecast_ids: visibleForecastIds,
          horizon_days: horizonDays,
        });
        setResponse(r);
        setPrompt("");
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setPending(false);
      }
    },
    [mode, selectedForecastId, visibleForecastIds, horizonDays],
  );

  return (
    <div className="fc-ask">
      <div className="fc-ask__chips" role="group" aria-label="Quick prompts">
        {PROMPT_CHIPS.map((chip) => (
          <button
            key={chip}
            type="button"
            className="fc-ask__chip"
            onClick={() => send(chip)}
            disabled={pending}
          >
            {chip}
          </button>
        ))}
      </div>
      <form
        className="fc-ask__form"
        onSubmit={(e) => {
          e.preventDefault();
          send(prompt);
        }}
      >
        <input
          type="text"
          className="fc-ask__input"
          placeholder={placeholder ?? "Ask a question or request scenario…"}
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          disabled={pending}
          aria-label="Ask Fyralis"
        />
        <button
          type="submit"
          className="fc-ask__submit"
          disabled={pending || !prompt.trim()}
        >
          {pending ? "Asking…" : "Ask"}
        </button>
      </form>
      {error ? (
        <div className="fc-ask__error" role="alert">
          {error}
        </div>
      ) : null}
      {response ? <AskResponseCard response={response} /> : null}
    </div>
  );
}

function AskResponseCard({ response }: { response: ForecastAskResponse }) {
  return (
    <article className={`fc-ask__response fc-ask__response--${response.type}`}>
      <header className="fc-ask__response-head">
        <span className="fc-micro-label">{labelForType(response.type)}</span>
        <h4 className="fc-ask__response-title">{response.title}</h4>
      </header>
      <p className="fc-ask__response-body">{response.body}</p>
      {response.evidence_used && response.evidence_used.length > 0 ? (
        <div className="fc-ask__section">
          <span className="fc-micro-label">Evidence</span>
          <ul className="fc-ask__list">
            {response.evidence_used.map((e, i) => (
              <li key={`${e}-${i}`}>{e}</li>
            ))}
          </ul>
        </div>
      ) : null}
      {response.missing_context && response.missing_context.length > 0 ? (
        <div className="fc-ask__section">
          <span className="fc-micro-label">Missing</span>
          <ul className="fc-ask__list fc-ask__list--missing">
            {response.missing_context.map((m, i) => (
              <li key={`${m}-${i}`}>{m}</li>
            ))}
          </ul>
        </div>
      ) : null}
      {response.actions && response.actions.length > 0 ? (
        <div className="fc-ask__actions">
          {response.actions.map((a) => (
            <button key={a.label} type="button" className="fc-ask__action">
              {a.label}
            </button>
          ))}
        </div>
      ) : null}
    </article>
  );
}

function labelForType(t: ForecastAskResponse["type"]): string {
  return {
    forecast_explanation: "Explanation",
    scenario_analysis: "Scenario",
    falsifier_explanation: "Falsifiers",
    pattern_trace: "Pattern trace",
    intervention_comparison: "Interventions",
    accuracy_reference: "Accuracy",
  }[t];
}
