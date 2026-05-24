import type { EvidenceSnapshot, EvidenceStrength } from "@/api/ledger-v2-types";

interface Props {
  evidence?: EvidenceSnapshot;
  onViewAll?: () => void;
}

// "Evidence at the time" block. Important nuance: this is a snapshot
// of what Fyralis had when it acted, NOT current evidence.
export function EvidenceAtTime({ evidence, onViewAll }: Props) {
  if (!evidence) return null;
  return (
    <section className="lg-block">
      <header className="lg-block__head">
        <div className="lg-micro-label">Evidence at the time</div>
        {typeof evidence.signalCount === "number" ? (
          <span className="lg-block__meta">{evidence.signalCount} signals</span>
        ) : null}
      </header>
      <ul className="lg-evidence">
        {evidence.sources.map((s, i) => (
          <li key={i} className="lg-evidence__row">
            <span className="lg-evidence__label">{s.label}</span>
            <span
              className={`lg-evidence__strength lg-evidence__strength--${s.strength}`}
            >
              {labelFor(s.strength)}
            </span>
            <StrengthBars strength={s.strength} />
          </li>
        ))}
      </ul>
      <button type="button" className="lg-link lg-block__more" onClick={onViewAll}>
        View all signals →
      </button>
    </section>
  );
}

function StrengthBars({ strength }: { strength: EvidenceStrength }) {
  const filled = bars(strength);
  const total = 5;
  return (
    <span
      className={`lg-evidence__bars lg-evidence__bars--${strength}`}
      aria-hidden="true"
    >
      {Array.from({ length: total }, (_, i) => (
        <span
          key={i}
          className={`lg-evidence__bar${i < filled ? " lg-evidence__bar--on" : ""}`}
        />
      ))}
    </span>
  );
}

function bars(s: EvidenceStrength): number {
  switch (s) {
    case "strong":
      return 5;
    case "moderate":
      return 4;
    case "partial":
      return 3;
    case "weak":
      return 2;
    case "missing":
      return 0;
  }
}

function labelFor(s: EvidenceStrength): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
