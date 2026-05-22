// Forecast card (spec §13). Compact representation of one future claim.
// Selection state lifts up; clicking updates the inspector.

import type { ForecastSummaryCard } from "@/api/forecasts-types";
import { ConfidencePill, Sparkline, TrendArrow, formatDate } from "./shared";

export interface ForecastCardProps {
  forecast: ForecastSummaryCard;
  selected: boolean;
  onSelect: (id: string) => void;
}

export function ForecastCard({ forecast, selected, onSelect }: ForecastCardProps) {
  return (
    <button
      type="button"
      className={`fc-card${selected ? " fc-card--selected" : ""} fc-card--${forecast.severity ?? "medium"}`}
      onClick={() => onSelect(forecast.id)}
      aria-pressed={selected}
      aria-label={`${forecast.statement}, confidence ${Math.round(forecast.confidence * 100)} percent`}
    >
      <div className="fc-card__statement">{forecast.statement}</div>
      <div className="fc-card__meta">
        <ConfidencePill value={forecast.confidence} delta={forecast.confidence_delta} />
        <TrendArrow trend={forecast.trend} />
      </div>
      {forecast.resolution_date ? (
        <div className="fc-card__resolution">
          resolves {formatDate(forecast.resolution_date)}
        </div>
      ) : null}
      {forecast.top_driver ? (
        <div className="fc-card__driver">Driver: {forecast.top_driver}</div>
      ) : null}
      <div className="fc-card__footer">
        <Sparkline points={forecast.sparkline} width={68} height={18} />
        {forecast.impact ? (
          <span className="fc-card__impact">{forecast.impact.label}</span>
        ) : null}
      </div>
    </button>
  );
}
