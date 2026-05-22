// Confidence display — qualitative label first, numeric second, with
// movement indicator (spec §15.5). Never raw % alone.

interface Props {
  value?: number;            // 0..1
  previous?: number;         // 0..1
  basis?: string;            // "limited by missing product usage telemetry"
  compact?: boolean;
}

function qualitative(v: number | undefined): string {
  if (v == null) return "—";
  if (v >= 0.85) return "High";
  if (v >= 0.65) return "Moderate";
  if (v >= 0.4) return "Low";
  return "Weak";
}

export function Confidence({ value, previous, basis, compact }: Props) {
  if (value == null) {
    return <span className="fx-muted">No confidence yet</span>;
  }
  const pct = Math.round(value * 100);
  const move = previous != null ? Math.round((value - previous) * 100) : 0;
  const label = qualitative(value);

  if (compact) {
    return (
      <span className="fx-conf">
        <span className="fx-conf__bar"><span className="fx-conf__bar-fill" style={{ width: `${pct}%` }} /></span>
        <strong>{pct}%</strong>
        {Math.abs(move) > 0 ? (
          <span className={`fx-conf__move fx-conf__move--${move > 0 ? "up" : "down"}`}>
            {move > 0 ? `+${move}pp` : `${move}pp`}
          </span>
        ) : null}
      </span>
    );
  }

  return (
    <div className="fx-stack" style={{ gap: 6 }}>
      <div className="fx-conf">
        <strong>{label} confidence</strong>
        <span className="fx-muted">·</span>
        <span>{pct}%</span>
        <span className="fx-conf__bar"><span className="fx-conf__bar-fill" style={{ width: `${pct}%` }} /></span>
        {Math.abs(move) > 0 ? (
          <span className={`fx-conf__move fx-conf__move--${move > 0 ? "up" : "down"}`}>
            {move > 0 ? `up from ${pct - move}%` : `down from ${pct - move}%`}
          </span>
        ) : null}
      </div>
      {basis ? <div className="fx-muted" style={{ fontSize: 12 }}>{basis}</div> : null}
    </div>
  );
}
