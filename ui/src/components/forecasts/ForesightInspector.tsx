// Foresight Inspector (spec §14-22). Right-column detail surface for
// the selected forecast: confidence movement, why, driving patterns,
// leading indicators, falsifiers, intervention levers, related context,
// and Ask Fyralis.

import type {
  ForecastDetail,
  Falsifier,
  InterventionLever,
  LeadingIndicator,
} from "@/api/forecasts-types";
import { AskFyralis } from "./AskFyralis";
import {
  ConfidenceChart,
  ConfidencePill,
  Sparkline,
  TrendArrow,
  formatDate,
  formatPpDelta,
  relativeDays,
} from "./shared";

export interface ForesightInspectorProps {
  detail: ForecastDetail | null;
  pending: boolean;
  visibleForecastIds: string[];
  horizonDays: number;
}

export function ForesightInspector({
  detail,
  pending,
  visibleForecastIds,
  horizonDays,
}: ForesightInspectorProps) {
  if (pending && !detail) {
    return (
      <aside className="fc-inspector fc-inspector--loading" aria-label="Foresight Inspector">
        <div className="fc-inspector__skeleton" />
      </aside>
    );
  }
  if (!detail) {
    return (
      <aside className="fc-inspector fc-inspector--empty" aria-label="Foresight Inspector">
        <div className="fc-inspector__empty-state">
          <span className="fc-micro-label">Inspector</span>
          <h3>Select a forecast to inspect.</h3>
          <p>
            The inspector shows why Fyralis sees this future forming, what
            would change its mind, and what you can do about it.
          </p>
        </div>
      </aside>
    );
  }

  return (
    <aside
      className="fc-inspector"
      aria-label="Foresight Inspector"
      data-severity={detail.severity}
    >
      <header className="fc-inspector__head">
        <span className="fc-micro-label">Selected forecast</span>
        <h2 className="fc-inspector__title">{detail.statement}</h2>
        <div className="fc-inspector__sub">
          <span>{domainLabel(detail.domain)}</span>
          {detail.resolution_date ? (
            <>
              <span aria-hidden="true">·</span>
              <span>resolves {formatDate(detail.resolution_date)} ({relativeDays(detail.resolution_date)})</span>
            </>
          ) : null}
          {detail.impact ? (
            <>
              <span aria-hidden="true">·</span>
              <span>{detail.impact.label}</span>
            </>
          ) : null}
        </div>
      </header>

      <section className="fc-inspector__section">
        <header className="fc-inspector__section-head">
          <span className="fc-micro-label">Confidence movement</span>
          <div className="fc-inspector__confidence">
            <ConfidencePill
              value={detail.confidence}
              delta={detail.confidence_delta}
            />
            {detail.confidence_series.delta != null ? (
              <span className="fc-inspector__confidence-window">
                {formatPpDelta(detail.confidence_series.delta)} in {detail.confidence_series.delta_window_days} days
              </span>
            ) : null}
          </div>
        </header>
        <ConfidenceChart points={detail.confidence_series.points} />
      </section>

      <section className="fc-inspector__section">
        <span className="fc-micro-label">Why this forecast</span>
        <p className="fc-inspector__why">{detail.why_this_forecast}</p>
      </section>

      <section className="fc-inspector__section">
        <span className="fc-micro-label">Driving patterns</span>
        <ul className="fc-inspector__patterns">
          {detail.driving_patterns.map((p) => (
            <li key={p.id} className={`fc-inspector__pattern fc-inspector__pattern--${p.status}`}>
              <span className="fc-inspector__pattern-title">{p.title}</span>
              <span className="fc-inspector__pattern-status">{p.status}</span>
              {p.source_coverage && p.source_coverage.length > 0 ? (
                <span className="fc-inspector__pattern-sources">
                  Sources: {p.source_coverage.join(" · ")}
                </span>
              ) : null}
            </li>
          ))}
        </ul>
      </section>

      <section className="fc-inspector__section">
        <span className="fc-micro-label">Leading indicators</span>
        <ul className="fc-inspector__indicators">
          {detail.leading_indicators.map((ind) => (
            <IndicatorRow key={ind.id} indicator={ind} />
          ))}
        </ul>
      </section>

      <section className="fc-inspector__section">
        <span className="fc-micro-label">Would change if</span>
        <ul className="fc-inspector__falsifiers">
          {detail.would_change_if.map((f) => (
            <FalsifierRow key={f.id} falsifier={f} />
          ))}
        </ul>
      </section>

      <section className="fc-inspector__section">
        <span className="fc-micro-label">Intervention levers</span>
        <ul className="fc-inspector__levers">
          {detail.intervention_levers.map((l, idx) => (
            <LeverRow key={l.id} lever={l} primary={idx === 0} />
          ))}
        </ul>
      </section>

      {detail.related_context && (
        detail.related_context.model_links.length +
          detail.related_context.today_links.length +
          detail.related_context.ledger_links.length >
        0 ? (
          <section className="fc-inspector__section">
            <span className="fc-micro-label">Related context</span>
            <div className="fc-inspector__related">
              {detail.related_context.model_links.length > 0 ? (
                <div className="fc-inspector__related-group">
                  <span className="fc-inspector__related-label">Model</span>
                  <ul>
                    {detail.related_context.model_links.map((l) => (
                      <li key={l.label}>
                        <a className="fc-link" href={l.href}>{l.label}</a>
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}
              {detail.related_context.today_links.length > 0 ? (
                <div className="fc-inspector__related-group">
                  <span className="fc-inspector__related-label">Today</span>
                  <ul>
                    {detail.related_context.today_links.map((l) => (
                      <li key={l.label}>
                        <a className="fc-link" href={`/today?expand=${l.proposed_change_id ?? ""}`}>{l.label}</a>
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}
              {detail.related_context.ledger_links.length > 0 ? (
                <div className="fc-inspector__related-group">
                  <span className="fc-inspector__related-label">Ledger</span>
                  <ul>
                    {detail.related_context.ledger_links.map((l) => (
                      <li key={l.label}>
                        <a className="fc-link" href={`/ledger?event=${l.event_id ?? ""}`}>{l.label}</a>
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </div>
          </section>
        ) : null
      )}

      <section className="fc-inspector__section fc-inspector__section--ask">
        <span className="fc-micro-label">Ask Fyralis</span>
        <AskFyralis
          selectedForecastId={detail.id}
          visibleForecastIds={visibleForecastIds}
          horizonDays={horizonDays}
        />
      </section>
    </aside>
  );
}

function IndicatorRow({ indicator }: { indicator: LeadingIndicator }) {
  return (
    <li className={`fc-indicator fc-indicator--${indicator.severity ?? "neutral"}`}>
      <span className="fc-indicator__label">{indicator.label}</span>
      <span className="fc-indicator__timeframe">{indicator.timeframe}</span>
      <div className="fc-indicator__value">
        <TrendArrow trend={indicator.direction} />
        <span>{indicator.value_label}</span>
      </div>
      <Sparkline points={indicator.sparkline} width={56} height={16} />
    </li>
  );
}

function FalsifierRow({ falsifier }: { falsifier: Falsifier }) {
  const met = falsifier.status === "met";
  return (
    <li className={`fc-falsifier fc-falsifier--${falsifier.status ?? "unmet"}`}>
      <span
        className="fc-falsifier__mark"
        aria-label={met ? "met" : "not yet met"}
      >
        {met ? "✓" : "○"}
      </span>
      <div className="fc-falsifier__body">
        <span className="fc-falsifier__text">{falsifier.text}</span>
        {falsifier.timeframe ? (
          <span className="fc-falsifier__timeframe">{falsifier.timeframe}</span>
        ) : null}
      </div>
    </li>
  );
}

function LeverRow({
  lever,
  primary,
}: {
  lever: InterventionLever;
  primary: boolean;
}) {
  return (
    <li className="fc-lever">
      <button
        type="button"
        className={`fc-lever__btn${primary ? " fc-lever__btn--primary" : ""}`}
      >
        {lever.label}
      </button>
      {lever.expected_effect ? (
        <span className="fc-lever__effect">{lever.expected_effect}</span>
      ) : null}
    </li>
  );
}

function domainLabel(id: string): string {
  return {
    customers_revenue: "Customers & Revenue",
    commitments_delivery: "Commitments & Delivery",
    systems_capacity: "Systems & Capacity",
    people_ownership: "People & Ownership",
    finance_capital: "Finance & Capital",
  }[id] ?? id;
}
