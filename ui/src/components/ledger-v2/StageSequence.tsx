import type { LedgerStage } from "@/api/ledger-v2-types";

interface Props {
  stages: LedgerStage[];
  size?: "card" | "inline";
}

// Mini "Detected → Forecasted → Proposed → ..." sequence used on the
// chain card. Dots are colored per stage state; the active one gets
// the moss accent.
export function StageSequence({ stages, size = "card" }: Props) {
  return (
    <ol
      className={`lg-stage-seq lg-stage-seq--${size}`}
      aria-label="Chain stages"
    >
      {stages.map((s, i) => (
        <li
          key={s.id}
          className={`lg-stage-seq__step lg-stage-seq__step--${s.status} lg-stage-seq__step--label-${labelKey(s.label)}`}
        >
          <span className="lg-stage-seq__dot" aria-hidden="true" />
          <span className="lg-stage-seq__label">{s.label}</span>
          {i < stages.length - 1 ? (
            <span
              className="lg-stage-seq__connector"
              aria-hidden="true"
            />
          ) : null}
        </li>
      ))}
    </ol>
  );
}

function labelKey(label: LedgerStage["label"]): string {
  return label.toLowerCase();
}
