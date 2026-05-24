import type { RelatedContext } from "@/api/ledger-v2-types";

interface Props {
  related?: RelatedContext;
}

// Related Context block. Connects a chain back to Today / Model /
// Forecasts. Each link uses the conventional `lg-link` style.
export function RelatedContextBlock({ related }: Props) {
  if (!related) return null;
  const todayItems = related.todayItems ?? [];
  const modelLinks = related.modelLinks ?? [];
  const forecastLinks = related.forecastLinks ?? [];
  if (
    todayItems.length === 0 &&
    modelLinks.length === 0 &&
    forecastLinks.length === 0
  )
    return null;
  return (
    <section className="lg-block">
      <div className="lg-micro-label">Related context</div>
      <ul className="lg-related">
        {todayItems.map((t) => (
          <li className="lg-related__row" key={t.proposedChangeId}>
            <span className="lg-related__label">Today item</span>
            <a
              className="lg-link"
              href={`/today?review=${encodeURIComponent(t.proposedChangeId)}`}
            >
              {t.label} →
            </a>
          </li>
        ))}
        {modelLinks.map((m) => (
          <li className="lg-related__row" key={m.href}>
            <span className="lg-related__label">Model</span>
            <a className="lg-link" href={m.href}>
              {m.label} →
            </a>
          </li>
        ))}
        {forecastLinks.map((f) => (
          <li className="lg-related__row" key={f.forecastId}>
            <span className="lg-related__label">Forecast</span>
            <a
              className="lg-link"
              href={`/forecasts?forecast=${encodeURIComponent(f.forecastId)}`}
            >
              {f.label} →
            </a>
          </li>
        ))}
      </ul>
    </section>
  );
}
