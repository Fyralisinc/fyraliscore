import type { SignalMetric } from "@/api/today-types";

type Props = {
  metrics: SignalMetric[];
  onShortcuts: () => void;
};

// Per spec §3.3 — four equal-width metric cells + utility cell on the right.
// Trend line color reflects metric tone (signature when up, critical when
// down, default when flat). When a metric is unavailable the value renders
// as `—` and the trend reads "unavailable, retrying".
export function SignalStrip({ metrics, onShortcuts }: Props) {
  return (
    <div className="signal-strip" role="region" aria-label="Signal strip">
      {metrics.slice(0, 4).map((m) => (
        <button className="signal" key={m.id} type="button">
          <span className="signal-label">{m.label}</span>
          {m.unavailable ? (
            <span className="signal-value unavail">—</span>
          ) : (
            <span className="signal-value">
              {m.value}
              {m.value_unit ? (
                <span className="signal-value-unit">{m.value_unit}</span>
              ) : null}
            </span>
          )}
          {m.unavailable ? (
            <span className="signal-trend">
              <em>unavailable, retrying</em>
            </span>
          ) : m.trend_html ? (
            <span
              className={"signal-trend" + (m.tone ? ` ${m.tone}` : "")}
              dangerouslySetInnerHTML={{ __html: m.trend_html }}
            />
          ) : null}
        </button>
      ))}
      <button
        className="signal-utility"
        onClick={onShortcuts}
        aria-label="Show keyboard shortcuts"
        type="button"
      >
        <span className="key">?</span>
        <span>Shortcuts</span>
      </button>
    </div>
  );
}
