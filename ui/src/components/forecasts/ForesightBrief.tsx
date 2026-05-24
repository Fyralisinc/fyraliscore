// Foresight Brief (spec §10) — synthesis card that sits above the
// mode selector. Three columns plus a "view all" link.

import type { ForesightBriefData } from "@/api/forecasts-types";
import { TrendArrow, formatDate } from "./shared";

export interface ForesightBriefProps {
  brief: ForesightBriefData | null;
  onSelectForecast: (id: string) => void;
  onSelectInterventionForecast: (id: string) => void;
}

export function ForesightBrief({
  brief,
  onSelectForecast,
  onSelectInterventionForecast,
}: ForesightBriefProps) {
  if (!brief) {
    return (
      <section className="fc-brief fc-brief--loading" aria-label="Foresight Brief">
        <div className="fc-brief__skeleton" />
      </section>
    );
  }
  return (
    <section className="fc-brief" aria-label="Foresight Brief">
      <div className="fc-brief__rail" aria-hidden="true" />
      <div className="fc-brief__inner">
        <header className="fc-brief__statement-block">
          <span className="fc-micro-label">Foresight Brief</span>
          <p className="fc-brief__statement">{brief.statement}</p>
        </header>

        <div className="fc-brief__col">
          <span className="fc-micro-label">What changed</span>
          {brief.what_changed.length === 0 ? (
            <p className="fc-brief__empty">No notable movement.</p>
          ) : (
            <ul className="fc-brief__list">
              {brief.what_changed.slice(0, 3).map((item) => (
                <li key={item.id} className="fc-brief__item">
                  {item.direction ? <TrendArrow trend={item.direction} /> : null}
                  <span>{item.label}</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="fc-brief__col">
          <span className="fc-micro-label">Resolves soon</span>
          {brief.resolves_soon.length === 0 ? (
            <p className="fc-brief__empty">No resolutions imminent.</p>
          ) : (
            <ul className="fc-brief__list">
              {brief.resolves_soon.slice(0, 3).map((item) => (
                <li key={item.forecast_id} className="fc-brief__item">
                  <button
                    type="button"
                    className="fc-brief__link"
                    onClick={() => onSelectForecast(item.forecast_id)}
                  >
                    <span>{item.label}</span>
                    <span className="fc-brief__date">
                      {formatDate(item.resolution_date)}
                    </span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div className="fc-brief__col">
          <span className="fc-micro-label">Interventions</span>
          {brief.interventions.length === 0 ? (
            <p className="fc-brief__empty">No levers queued.</p>
          ) : (
            <ul className="fc-brief__list">
              {brief.interventions.slice(0, 3).map((item) => (
                <li key={item.id} className="fc-brief__item">
                  <button
                    type="button"
                    className="fc-brief__link"
                    onClick={() =>
                      item.related_forecast_id &&
                      onSelectInterventionForecast(item.related_forecast_id)
                    }
                  >
                    {item.label}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </section>
  );
}
