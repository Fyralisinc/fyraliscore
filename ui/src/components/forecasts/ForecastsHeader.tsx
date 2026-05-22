// Forecasts page header (spec §9). Title + scope subtitle + inline stats
// + Ask input / horizon picker / filters control.

import type { ForecastsHeaderData } from "@/api/forecasts-types";
import { formatPercent } from "./shared";

export interface ForecastsHeaderProps {
  header: ForecastsHeaderData | null;
  horizonDays: number;
  onHorizonChange: (days: number) => void;
  onAskClick: () => void;
}

export function ForecastsHeader({
  header,
  horizonDays,
  onHorizonChange,
  onAskClick,
}: ForecastsHeaderProps) {
  return (
    <header className="fc-header">
      <div className="fc-header__lede">
        <h1 className="fc-header__title">Forecasts</h1>
        <p className="fc-header__subtitle">What Fyralis sees forming.</p>
        <p className="fc-header__stats">
          {header ? (
            <>
              <span>{header.active_forecast_count} active forecasts</span>
              <span className="fc-header__dot" aria-hidden="true">·</span>
              <span>
                {header.resolving_soon_count} resolve in {Math.min(14, header.horizon_days)} days
              </span>
              <span className="fc-header__dot" aria-hidden="true">·</span>
              <span>
                {header.accelerating_pattern_count} pattern
                {header.accelerating_pattern_count === 1 ? "" : "s"} accelerating
              </span>
              <span className="fc-header__dot" aria-hidden="true">·</span>
              <span>{formatPercent(header.calibrated_accuracy)} calibrated accuracy</span>
            </>
          ) : (
            <span className="fc-header__stats-loading">Loading…</span>
          )}
        </p>
      </div>

      <div className="fc-header__controls">
        <button
          type="button"
          className="fc-header__ask"
          onClick={onAskClick}
          aria-label="Ask Fyralis about forecasts"
        >
          <span className="fc-header__ask-glyph" aria-hidden="true">⌘</span>
          <span>Ask about forecasts, patterns, or scenarios…</span>
        </button>
        <label className="fc-header__horizon">
          <span className="fc-header__horizon-label">Horizon</span>
          <select
            className="fc-header__horizon-select"
            value={horizonDays}
            onChange={(e) => onHorizonChange(Number(e.target.value))}
            aria-label="Forecast horizon"
          >
            <option value={30}>30 days</option>
            <option value={60}>60 days</option>
            <option value={90}>90 days</option>
            <option value={180}>180 days</option>
          </select>
        </label>
      </div>
    </header>
  );
}
