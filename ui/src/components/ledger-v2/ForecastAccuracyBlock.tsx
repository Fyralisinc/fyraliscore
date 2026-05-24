import type { ForecastAccuracyResult } from "@/api/ledger-v2-types";

interface Props {
  result?: ForecastAccuracyResult;
  onOpenAccuracy?: () => void;
}

// Forecast Accuracy block. Renders only when the chain includes a
// forecast. Provides a link to the Accuracy mode for the broader
// calibration view.
export function ForecastAccuracyBlock({ result, onOpenAccuracy }: Props) {
  if (!result) return null;
  return (
    <section className="lg-block">
      <div className="lg-micro-label">Forecast accuracy</div>
      <ul className="lg-kv">
        <li className="lg-kv__row">
          <span className="lg-kv__label">Initial confidence</span>
          <span className="lg-kv__value">{result.initialConfidencePct}%</span>
        </li>
        <li className="lg-kv__row">
          <span className="lg-kv__label">Outcome</span>
          <span
            className={`lg-kv__value lg-outcome lg-outcome--${result.outcome}`}
          >
            Resolved {result.outcome}
          </span>
        </li>
        <li className="lg-kv__row">
          <span className="lg-kv__label">Resolution date</span>
          <span className="lg-kv__value">{shortDate(result.resolvedAt)}</span>
        </li>
        {typeof result.calibrationImpactPp === "number" ? (
          <li className="lg-kv__row">
            <span className="lg-kv__label">Calibration impact</span>
            <span
              className={`lg-kv__value lg-calib lg-calib--${
                result.calibrationImpactPp >= 0 ? "up" : "down"
              }`}
            >
              {result.calibrationImpactPp >= 0 ? "+" : ""}
              {result.calibrationImpactPp}pp
            </span>
          </li>
        ) : null}
      </ul>
      {result.notes ? <p className="lg-block__note">{result.notes}</p> : null}
      {onOpenAccuracy ? (
        <button type="button" className="lg-link lg-block__more" onClick={onOpenAccuracy}>
          Open accuracy →
        </button>
      ) : null}
    </section>
  );
}

function shortDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}
