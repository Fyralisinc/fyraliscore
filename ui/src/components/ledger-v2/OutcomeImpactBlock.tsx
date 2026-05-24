import type { OutcomeImpact } from "@/api/ledger-v2-types";

interface Props {
  outcome?: OutcomeImpact;
}

// Outcome & Impact block. Compact label/value rows. Lives in the
// Chain Inspector grid alongside Before/After and Evidence.
export function OutcomeImpactBlock({ outcome }: Props) {
  if (!outcome) return null;
  return (
    <section className="lg-block">
      <div className="lg-micro-label">Outcome & impact</div>
      <ul className="lg-kv">
        {outcome.impactRows.map((r, i) => (
          <li className="lg-kv__row" key={i}>
            <span className="lg-kv__label">{r.label}</span>
            <span
              className={`lg-kv__value${
                r.label.toLowerCase() === "business impact"
                  ? " lg-kv__value--positive"
                  : ""
              }`}
            >
              {r.value}
            </span>
          </li>
        ))}
      </ul>
      {outcome.notes ? (
        <p className="lg-block__note">{outcome.notes}</p>
      ) : null}
    </section>
  );
}
