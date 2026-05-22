// Patterns Mode (spec §24). Pattern overview + cluster grid +
// selected pattern inspector showing supporting forecasts.

import { useMemo, useState } from "react";
import type {
  ForecastsPagePayload,
  PatternCard,
} from "@/api/forecasts-types";
import { AskFyralis } from "./AskFyralis";
import { PatternStatusBadge } from "./shared";

export interface PatternsModeProps {
  payload: ForecastsPagePayload | null;
  patterns: PatternCard[];
  onSelectForecast: (id: string) => void;
  horizonDays: number;
}

export function PatternsMode({
  payload,
  patterns,
  onSelectForecast,
  horizonDays,
}: PatternsModeProps) {
  const [selectedPatternId, setSelectedPatternId] = useState<string | null>(
    patterns[0]?.id ?? null,
  );

  const counts = useMemo(() => {
    const out = { strengthening: 0, emerging: 0, weakening: 0, stable: 0 };
    for (const p of patterns) {
      if (p.status in out) (out as Record<string, number>)[p.status] += 1;
    }
    return out;
  }, [patterns]);

  const selected =
    patterns.find((p) => p.id === selectedPatternId) ?? patterns[0] ?? null;

  // Compose related forecasts for the selected pattern by id lookup
  // against the page payload's matrix cells.
  const relatedForecasts = useMemo(() => {
    if (!selected || !payload) return [];
    const allForecasts = payload.horizon.domains
      .flatMap((d) => d.cells)
      .flatMap((c) => c.forecasts);
    return allForecasts.filter((f) => selected.related_forecast_ids.includes(f.id));
  }, [selected, payload]);

  return (
    <section className="fc-patterns" aria-label="Patterns mode">
      <header className="fc-patterns__head">
        <h2 className="fc-patterns__title">Patterns</h2>
        <p className="fc-patterns__counts">
          <span><strong>{counts.strengthening}</strong> strengthening</span>
          <span aria-hidden="true">·</span>
          <span><strong>{counts.emerging}</strong> emerging</span>
          <span aria-hidden="true">·</span>
          <span><strong>{counts.weakening}</strong> weakening</span>
          <span aria-hidden="true">·</span>
          <span><strong>{counts.stable}</strong> stable</span>
        </p>
      </header>

      <div className="fc-patterns__layout">
        <div className="fc-patterns__grid" role="listbox" aria-label="Pattern clusters">
          {patterns.length === 0 ? (
            <div className="fc-empty">No patterns yet.</div>
          ) : (
            patterns.map((p) => (
              <button
                key={p.id}
                type="button"
                role="option"
                aria-selected={selected?.id === p.id}
                className={`fc-patterns__card${selected?.id === p.id ? " fc-patterns__card--selected" : ""} fc-patterns__card--${p.status}`}
                onClick={() => setSelectedPatternId(p.id)}
              >
                <span className="fc-patterns__card-title">{p.title}</span>
                <PatternStatusBadge status={p.status} />
                <span className="fc-patterns__card-supported">
                  Supports {p.supported_forecast_count} forecast
                  {p.supported_forecast_count === 1 ? "" : "s"}
                </span>
                <span className="fc-patterns__card-sources">
                  Sources: {p.sources.slice(0, 3).join(" · ")}
                </span>
              </button>
            ))
          )}
        </div>

        <aside className="fc-pattern-inspector" aria-label="Selected pattern inspector">
          {!selected ? (
            <div className="fc-pattern-inspector__empty">
              Select a pattern to inspect.
            </div>
          ) : (
            <>
              <header className="fc-pattern-inspector__head">
                <span className="fc-micro-label">Pattern</span>
                <h3 className="fc-pattern-inspector__title">{selected.title}</h3>
                <div className="fc-pattern-inspector__meta">
                  <PatternStatusBadge status={selected.status} />
                  <span>
                    Supports {selected.supported_forecast_count} forecast
                    {selected.supported_forecast_count === 1 ? "" : "s"}
                  </span>
                </div>
              </header>

              <section className="fc-pattern-inspector__section">
                <span className="fc-micro-label">Source coverage</span>
                <p>{selected.sources.join(" · ")}</p>
              </section>

              <section className="fc-pattern-inspector__section">
                <span className="fc-micro-label">Supporting forecasts</span>
                {relatedForecasts.length === 0 ? (
                  <p className="fc-pattern-inspector__empty-inline">None visible in current horizon.</p>
                ) : (
                  <ul className="fc-pattern-inspector__related">
                    {relatedForecasts.map((f) => (
                      <li key={f.id}>
                        <button
                          type="button"
                          className="fc-link"
                          onClick={() => onSelectForecast(f.id)}
                        >
                          {f.statement}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </section>

              <section className="fc-pattern-inspector__section">
                <span className="fc-micro-label">What would weaken</span>
                <p>
                  Evidence reverses across {selected.sources.length} of {selected.sources.length} sources for 7+ days.
                </p>
              </section>

              <section className="fc-pattern-inspector__section fc-pattern-inspector__section--ask">
                <span className="fc-micro-label">Ask Fyralis</span>
                <AskFyralis
                  selectedForecastId={selected.related_forecast_ids[0] ?? null}
                  mode="patterns"
                  horizonDays={horizonDays}
                  placeholder="Ask about this pattern…"
                />
              </section>
            </>
          )}
        </aside>
      </div>
    </section>
  );
}
