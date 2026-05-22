// Accuracy Mode (spec §26). Calibration summary + bins chart +
// resolved-forecast table + accuracy-by-domain breakdown.

import { useMemo } from "react";
import type {
  AccuracyResponse,
  ForecastAccuracySummary,
  ForecastsPagePayload,
} from "@/api/forecasts-types";
import { formatPercent } from "./shared";

export interface AccuracyModeProps {
  summary: ForecastAccuracySummary | null;
  accuracy: AccuracyResponse | null;
  payload: ForecastsPagePayload | null;
}

export function AccuracyMode({ summary, accuracy }: AccuracyModeProps) {
  const counts = useMemo(() => {
    if (!accuracy) return { true_: 0, false_: 0, partial: 0 };
    let t = 0, f = 0, p = 0;
    for (const r of accuracy.recent_resolutions) {
      if (r.outcome === "true") t += 1;
      else if (r.outcome === "false") f += 1;
      else if (r.outcome === "partial") p += 1;
    }
    return { true_: t, false_: f, partial: p };
  }, [accuracy]);

  const byDomain = useMemo(() => {
    if (!accuracy) return [];
    const m = new Map<string, { count: number; true_: number; conf_sum: number }>();
    for (const r of accuracy.recent_resolutions) {
      const cur = m.get(r.category) ?? { count: 0, true_: 0, conf_sum: 0 };
      cur.count += 1;
      cur.conf_sum += r.confidence;
      if (r.outcome === "true") cur.true_ += 1;
      m.set(r.category, cur);
    }
    return Array.from(m.entries()).map(([cat, v]) => ({
      category: cat,
      count: v.count,
      hit_rate: v.true_ / v.count,
      avg_conf: v.conf_sum / v.count,
    }));
  }, [accuracy]);

  return (
    <section className="fc-accuracy" aria-label="Accuracy mode">
      <header className="fc-accuracy__head">
        <h2 className="fc-accuracy__title">Accuracy & Calibration</h2>
        <p className="fc-accuracy__sub">
          How often Fyralis has been right, and how calibrated those probabilities have been.
        </p>
      </header>

      <section className="fc-accuracy__summary">
        <SummaryStat
          label="Calibrated accuracy"
          value={formatPercent(accuracy?.calibration_summary.value ?? summary?.calibrated_accuracy ?? null)}
        />
        <SummaryStat label="Resolved true" value={counts.true_.toString()} />
        <SummaryStat label="Resolved false" value={counts.false_.toString()} />
        <SummaryStat label="Partial" value={counts.partial.toString()} />
        <SummaryStat
          label="Resolved total"
          value={(accuracy?.calibration_summary.n_resolved_total ?? counts.true_ + counts.false_ + counts.partial).toString()}
        />
      </section>

      <section className="fc-accuracy__bins">
        <span className="fc-micro-label">Calibration by confidence bin</span>
        <CalibrationChart bins={accuracy?.bins ?? []} />
      </section>

      <section className="fc-accuracy__domain">
        <span className="fc-micro-label">Accuracy by domain</span>
        {byDomain.length === 0 ? (
          <div className="fc-empty">No resolved forecasts yet.</div>
        ) : (
          <table className="fc-accuracy__table">
            <thead>
              <tr>
                <th>Category</th>
                <th>N</th>
                <th>Avg confidence</th>
                <th>Hit rate</th>
              </tr>
            </thead>
            <tbody>
              {byDomain.map((d) => (
                <tr key={d.category}>
                  <td>{d.category.replace("_", " ")}</td>
                  <td>{d.count}</td>
                  <td>{formatPercent(d.avg_conf)}</td>
                  <td>{formatPercent(d.hit_rate)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="fc-accuracy__resolved">
        <span className="fc-micro-label">Recent resolutions</span>
        {!accuracy || accuracy.recent_resolutions.length === 0 ? (
          <div className="fc-empty">No resolutions yet.</div>
        ) : (
          <table className="fc-accuracy__table">
            <thead>
              <tr>
                <th>Forecast</th>
                <th>Initial confidence</th>
                <th>Outcome</th>
                <th>Resolution date</th>
                <th>Timeliness</th>
              </tr>
            </thead>
            <tbody>
              {accuracy.recent_resolutions.map((r) => (
                <tr key={r.id}>
                  <td>{r.statement}</td>
                  <td>{formatPercent(r.confidence)}</td>
                  <td className={`fc-outcome fc-outcome--${r.outcome}`}>{r.outcome}</td>
                  <td>{new Date(r.resolution_at).toLocaleDateString("en-US", { month: "short", day: "numeric" })}</td>
                  <td>{r.resolution_timeliness ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </section>
  );
}

function SummaryStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="fc-accuracy__stat">
      <span className="fc-accuracy__stat-value">{value}</span>
      <span className="fc-accuracy__stat-label">{label}</span>
    </div>
  );
}

function CalibrationChart({ bins }: { bins: AccuracyResponse["bins"] }) {
  if (!bins || bins.length === 0) {
    return <div className="fc-empty">Calibration not yet available.</div>;
  }
  const width = 520;
  const height = 180;
  const padding = 36;
  return (
    <svg
      width="100%"
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className="fc-calibration"
      role="img"
      aria-label="Calibration chart by confidence bin"
    >
      {/* Perfect-calibration diagonal */}
      <line
        x1={padding} y1={height - padding}
        x2={width - padding} y2={padding}
        stroke="var(--border-default)"
        strokeDasharray="4 4"
        strokeWidth={1.2}
      />
      {/* Axes */}
      <line
        x1={padding} y1={height - padding}
        x2={width - padding} y2={height - padding}
        stroke="var(--text-muted)" strokeWidth={1}
      />
      <line
        x1={padding} y1={padding}
        x2={padding} y2={height - padding}
        stroke="var(--text-muted)" strokeWidth={1}
      />
      {bins.map((b, i) => {
        const x =
          padding + ((width - 2 * padding) * b.predicted_rate);
        const y =
          height - padding -
          ((height - 2 * padding) * (b.observed_hit_rate ?? 0));
        if (b.observed_hit_rate == null) {
          return (
            <g key={i}>
              <circle cx={x} cy={height - padding} r={3} fill="var(--text-muted)" opacity={0.4} />
              <text
                x={x} y={height - padding + 16}
                textAnchor="middle" fontSize="10"
                fill="var(--text-muted)"
              >
                {b.bin_label}
              </text>
            </g>
          );
        }
        return (
          <g key={i}>
            <circle cx={x} cy={y} r={5.5} fill="var(--accent-trust)" />
            <text
              x={x} y={height - padding + 16}
              textAnchor="middle" fontSize="10"
              fill="var(--text-muted)"
            >
              {b.bin_label}
            </text>
            <text
              x={x} y={y - 9}
              textAnchor="middle" fontSize="10"
              fill="var(--text-primary)"
            >
              {Math.round((b.observed_hit_rate ?? 0) * 100)}%
            </text>
          </g>
        );
      })}
      <text x={width - padding} y={height - padding + 28} textAnchor="end" fontSize="11" fill="var(--text-muted)">
        Predicted →
      </text>
      <text x={padding} y={padding - 8} textAnchor="start" fontSize="11" fill="var(--text-muted)">
        Observed
      </text>
    </svg>
  );
}
