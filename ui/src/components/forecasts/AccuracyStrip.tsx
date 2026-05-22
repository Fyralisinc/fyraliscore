// Accuracy & Resolution Strip (spec §25). Bottom-of-page narrow card.
// Shows calibrated accuracy + resolution counts. Pulls live data from
// the AccuracyResponse if available; degrades gracefully on the
// ForecastAccuracySummary that ships in the page payload.

import type {
  AccuracyResponse,
  ForecastAccuracySummary,
} from "@/api/forecasts-types";
import { formatPercent } from "./shared";

export interface AccuracyStripProps {
  summary: ForecastAccuracySummary | null;
  full: AccuracyResponse | null;
  onOpenAccuracy: () => void;
}

export function AccuracyStrip({
  summary,
  full,
  onOpenAccuracy,
}: AccuracyStripProps) {
  const accuracy =
    full?.calibration_summary.value ?? summary?.calibrated_accuracy ?? null;

  const counts = computeCounts(full);
  const cal_err = summary?.avg_calibration_error_pp ?? null;

  return (
    <section className="fc-accuracy-strip" aria-label="Accuracy and Resolution">
      <div className="fc-accuracy-strip__inner">
        <div className="fc-accuracy-strip__lede">
          <span className="fc-micro-label">Accuracy & Resolution</span>
          <h2 className="fc-accuracy-strip__title">
            {formatPercent(accuracy)} calibrated accuracy
          </h2>
        </div>
        <ul className="fc-accuracy-strip__counts">
          <li>
            <span className="fc-accuracy-strip__count-value">{counts.true_}</span>
            <span className="fc-accuracy-strip__count-label">resolved true</span>
          </li>
          <li>
            <span className="fc-accuracy-strip__count-value">{counts.false_}</span>
            <span className="fc-accuracy-strip__count-label">resolved false</span>
          </li>
          <li>
            <span className="fc-accuracy-strip__count-value">{summary?.pending ?? "—"}</span>
            <span className="fc-accuracy-strip__count-label">pending</span>
          </li>
          {cal_err != null ? (
            <li>
              <span className="fc-accuracy-strip__count-value">
                ±{cal_err.toFixed(1)}pp
              </span>
              <span className="fc-accuracy-strip__count-label">avg calibration error</span>
            </li>
          ) : null}
        </ul>
        <button
          type="button"
          className="fc-accuracy-strip__cta"
          onClick={onOpenAccuracy}
        >
          View accuracy detail →
        </button>
      </div>
    </section>
  );
}

function computeCounts(full: AccuracyResponse | null) {
  if (!full) return { true_: "—" as string | number, false_: "—" as string | number };
  let t = 0;
  let f = 0;
  for (const r of full.recent_resolutions) {
    if (r.outcome === "true") t += 1;
    else if (r.outcome === "false") f += 1;
  }
  return { true_: t, false_: f };
}
