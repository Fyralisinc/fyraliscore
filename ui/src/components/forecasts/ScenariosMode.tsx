// Scenarios Mode (spec §27). "What if" builder + saved scenario cards
// + comparison strip. Scenarios live in-memory until saved — the spec
// is explicit that scenarios must not mutate the model silently.

import { useCallback, useMemo, useState } from "react";
import { askForecasts } from "@/api/forecasts-client";
import type {
  ForecastAskResponse,
  ForecastDetail,
} from "@/api/forecasts-types";

const SUGGESTED_PROMPTS = [
  "What if we assign a sync escalation owner today?",
  "What if we pause net-new platform commitments?",
  "What if we wait 7 days?",
  "What if we increase account touchpoints?",
];

interface SavedScenario {
  id: string;
  prompt: string;
  response: ForecastAskResponse;
  baseForecastId: string | null;
  createdAt: number;
}

export interface ScenariosModeProps {
  selectedForecast: ForecastDetail | null;
  onSelectForecast: (id: string) => void;
  horizonDays: number;
}

export function ScenariosMode({
  selectedForecast,
  horizonDays,
}: ScenariosModeProps) {
  const [prompt, setPrompt] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [scenarios, setScenarios] = useState<SavedScenario[]>([]);
  const [draft, setDraft] = useState<ForecastAskResponse | null>(null);

  const runScenario = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed) return;
      setPending(true);
      setError(null);
      try {
        const resp = await askForecasts({
          mode: "scenarios",
          prompt: trimmed,
          selected_forecast_id: selectedForecast?.id ?? null,
          horizon_days: horizonDays,
        });
        setDraft(resp);
        setPrompt(trimmed);
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setPending(false);
      }
    },
    [selectedForecast, horizonDays],
  );

  const saveScenario = useCallback(() => {
    if (!draft) return;
    setScenarios((prev) => [
      {
        id: `sc-${Date.now()}`,
        prompt,
        response: draft,
        baseForecastId: selectedForecast?.id ?? null,
        createdAt: Date.now(),
      },
      ...prev,
    ]);
    setDraft(null);
    setPrompt("");
  }, [draft, prompt, selectedForecast]);

  const comparison = useMemo(() => scenarios.slice(0, 3), [scenarios]);

  return (
    <section className="fc-scenarios" aria-label="Scenarios mode">
      <header className="fc-scenarios__head">
        <h2 className="fc-scenarios__title">Scenarios</h2>
        <p className="fc-scenarios__sub">
          Explore what-if outcomes for {selectedForecast?.statement ?? "the selected forecast"}.
        </p>
      </header>

      <section className="fc-scenario-builder">
        <span className="fc-micro-label">Build a scenario</span>
        <form
          className="fc-scenario-builder__form"
          onSubmit={(e) => {
            e.preventDefault();
            runScenario(prompt);
          }}
        >
          <input
            type="text"
            className="fc-scenario-builder__input"
            placeholder="What if we…"
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            disabled={pending}
          />
          <button
            type="submit"
            className="fc-scenario-builder__submit"
            disabled={pending || !prompt.trim()}
          >
            {pending ? "Running…" : "Run scenario"}
          </button>
        </form>
        <div className="fc-scenario-builder__chips">
          {SUGGESTED_PROMPTS.map((s) => (
            <button
              key={s}
              type="button"
              className="fc-scenario-builder__chip"
              onClick={() => runScenario(s)}
              disabled={pending}
            >
              {s}
            </button>
          ))}
        </div>
        {error ? (
          <div className="fc-scenario-builder__error" role="alert">{error}</div>
        ) : null}

        {draft ? (
          <article className="fc-scenario-card fc-scenario-card--draft">
            <header className="fc-scenario-card__head">
              <span className="fc-micro-label">Scenario draft</span>
              <h3>{draft.title}</h3>
            </header>
            <p className="fc-scenario-card__body">{draft.body}</p>
            {draft.missing_context && draft.missing_context.length > 0 ? (
              <div className="fc-scenario-card__missing">
                <span className="fc-micro-label">Missing</span>
                <ul>
                  {draft.missing_context.map((m, i) => (
                    <li key={`${m}-${i}`}>{m}</li>
                  ))}
                </ul>
              </div>
            ) : null}
            <div className="fc-scenario-card__actions">
              <button
                type="button"
                className="fc-scenario-card__action fc-scenario-card__action--primary"
                onClick={saveScenario}
              >
                Save scenario
              </button>
              <button
                type="button"
                className="fc-scenario-card__action"
                onClick={() => setDraft(null)}
              >
                Discard
              </button>
            </div>
          </article>
        ) : null}
      </section>

      {scenarios.length > 0 ? (
        <section className="fc-scenarios__saved">
          <span className="fc-micro-label">Saved scenarios</span>
          <ul className="fc-scenarios__grid">
            {scenarios.map((s) => (
              <li key={s.id}>
                <article className="fc-scenario-card">
                  <header className="fc-scenario-card__head">
                    <span className="fc-micro-label">Scenario</span>
                    <h3>{s.response.title}</h3>
                  </header>
                  <p className="fc-scenario-card__body">{s.response.body}</p>
                  <div className="fc-scenario-card__actions">
                    <button type="button" className="fc-scenario-card__action fc-scenario-card__action--primary">
                      Create Proposed Change
                    </button>
                    <button type="button" className="fc-scenario-card__action">
                      Compare
                    </button>
                  </div>
                </article>
              </li>
            ))}
          </ul>
        </section>
      ) : null}

      {comparison.length >= 2 ? (
        <section className="fc-scenarios__compare">
          <span className="fc-micro-label">Comparison</span>
          <table className="fc-scenarios__compare-table">
            <thead>
              <tr>
                <th>Scenario</th>
                <th>Expected effect</th>
                <th>Missing</th>
              </tr>
            </thead>
            <tbody>
              {comparison.map((s) => (
                <tr key={s.id}>
                  <td>{s.response.title}</td>
                  <td>{s.response.body.split(".")[0]}.</td>
                  <td>{(s.response.missing_context ?? []).join("; ") || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      ) : null}
    </section>
  );
}
