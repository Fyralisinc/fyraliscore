import type { LedgerAccuracySummary } from "@/api/ledger-v2-types";

interface Props {
  summary: LedgerAccuracySummary;
  onOpenChain: (id: string) => void;
}

// Accuracy mode — "how right has Fyralis been?". Compact summary
// stats, by-domain breakdown, resolved forecast table, and
// retrospective signals (false positives / negatives / missed).
export function AccuracyMode({ summary, onOpenChain }: Props) {
  return (
    <section className="lg-accuracy" aria-label="Accuracy">
      <header className="lg-accuracy__head">
        <h2 className="lg-accuracy__title">Accuracy</h2>
        <p className="lg-accuracy__sub">Last 30 days</p>
      </header>

      <div className="lg-accuracy__summary">
        <Stat label="Calibrated accuracy" value={`${summary.calibratedAccuracyPct}%`} />
        <Stat label="Resolved true" value={summary.resolvedTrue.toString()} />
        <Stat label="Resolved false" value={summary.resolvedFalse.toString()} />
        <Stat label="Pending" value={summary.pending.toString()} />
      </div>

      <div className="lg-accuracy__panel">
        <div className="lg-micro-label">Forecast accuracy by domain</div>
        <ul className="lg-domain">
          {summary.byDomain.map((d) => (
            <li className="lg-domain__row" key={d.domain}>
              <span className="lg-domain__label">{d.domain}</span>
              <div className="lg-domain__bar" aria-hidden="true">
                <span
                  className="lg-domain__bar-fill"
                  style={{ width: `${d.accuracyPct}%` }}
                />
              </div>
              <span className="lg-domain__pct">{d.accuracyPct}%</span>
              <span className="lg-domain__meta">
                {d.resolvedTrue} true · {d.resolvedFalse} false · {d.pending} pending
              </span>
            </li>
          ))}
        </ul>
      </div>

      <div className="lg-accuracy__panel">
        <div className="lg-micro-label">Resolved forecasts</div>
        <div className="lg-table-scroll">
          <table className="lg-table">
            <thead>
              <tr>
                <th>Forecast</th>
                <th>Initial</th>
                <th>Final</th>
                <th>Outcome</th>
                <th>Resolved</th>
                <th>Calibration</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {summary.resolved.map((r) => (
                <tr key={r.chainId + r.resolvedAt}>
                  <td>{r.forecast}</td>
                  <td>{r.initialConfidencePct}%</td>
                  <td>{r.finalConfidencePct != null ? `${r.finalConfidencePct}%` : "—"}</td>
                  <td>
                    <span className={`lg-outcome lg-outcome--${r.outcome}`}>
                      {r.outcome === "true"
                        ? "Resolved true"
                        : r.outcome === "false"
                        ? "Resolved false"
                        : "Partial"}
                    </span>
                  </td>
                  <td>{shortDate(r.resolvedAt)}</td>
                  <td>
                    {r.calibrationImpactPp != null ? (
                      <span
                        className={`lg-calib lg-calib--${
                          r.calibrationImpactPp >= 0 ? "up" : "down"
                        }`}
                      >
                        {r.calibrationImpactPp >= 0 ? "+" : ""}
                        {r.calibrationImpactPp}pp
                      </span>
                    ) : (
                      "—"
                    )}
                  </td>
                  <td>
                    <button
                      type="button"
                      className="lg-link"
                      onClick={() => onOpenChain(r.chainId)}
                    >
                      Ledger →
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <div className="lg-accuracy__grid">
        <RetroBlock label="False positives" items={summary.falsePositives} tone="garnet" />
        <RetroBlock label="False negatives" items={summary.falseNegatives} tone="gold" />
        <RetroBlock label="Missed context" items={summary.missedContext} tone="lapis" />
      </div>

      <div className="lg-accuracy__panel">
        <div className="lg-micro-label">Calibration trend</div>
        <div className="lg-trend">
          <p className="lg-empty">
            Calibration sparkline will appear after more forecasts resolve.
          </p>
        </div>
      </div>
    </section>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="lg-stat">
      <div className="lg-stat__value">{value}</div>
      <div className="lg-stat__label">{label}</div>
    </div>
  );
}

function RetroBlock({
  label,
  items,
  tone,
}: {
  label: string;
  items: { label: string; note?: string }[];
  tone: "moss" | "gold" | "lapis" | "garnet";
}) {
  return (
    <div className={`lg-retro lg-retro--${tone}`}>
      <div className="lg-micro-label">{label}</div>
      {items.length === 0 ? (
        <p className="lg-empty">No matches.</p>
      ) : (
        <ul className="lg-retro__list">
          {items.map((it, i) => (
            <li key={i}>
              <div className="lg-retro__label">{it.label}</div>
              {it.note ? <p className="lg-retro__note">{it.note}</p> : null}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

function shortDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}
