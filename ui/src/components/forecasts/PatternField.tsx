// Pattern Field (spec §23) — appears under the Horizon Matrix.
// Renders pattern cards horizontally; clicking a card surfaces its
// supported forecasts (handled by parent via callback).

import type { PatternCard } from "@/api/forecasts-types";
import { PatternStatusBadge } from "./shared";

export interface PatternFieldProps {
  patterns: PatternCard[];
  onSelectPattern?: (id: string) => void;
}

export function PatternField({ patterns, onSelectPattern }: PatternFieldProps) {
  return (
    <section className="fc-pattern-field" aria-label="Pattern Field">
      <header className="fc-pattern-field__head">
        <span className="fc-micro-label">Pattern Field</span>
        <h2 className="fc-pattern-field__title">
          Recurring dynamics supporting these forecasts
        </h2>
      </header>
      {patterns.length === 0 ? (
        <div className="fc-pattern-field__empty">
          No patterns detected yet. Fyralis needs more signal coverage.
        </div>
      ) : (
        <div className="fc-pattern-field__grid">
          {patterns.slice(0, 6).map((p) => (
            <PatternCardView
              key={p.id}
              pattern={p}
              onClick={() => onSelectPattern?.(p.id)}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function PatternCardView({
  pattern,
  onClick,
}: {
  pattern: PatternCard;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      className={`fc-pattern-card fc-pattern-card--${pattern.status}`}
      onClick={onClick}
    >
      <span className="fc-pattern-card__title">{pattern.title}</span>
      <PatternStatusBadge status={pattern.status} />
      <span className="fc-pattern-card__supported">
        Supports {pattern.supported_forecast_count} forecast
        {pattern.supported_forecast_count === 1 ? "" : "s"}
      </span>
      {pattern.sources.length > 0 ? (
        <span className="fc-pattern-card__sources">
          Sources: {pattern.sources.join(" · ")}
        </span>
      ) : null}
    </button>
  );
}
