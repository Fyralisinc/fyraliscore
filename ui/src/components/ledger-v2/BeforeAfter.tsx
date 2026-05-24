import type { ModelStateSnapshot } from "@/api/ledger-v2-types";

interface Props {
  before?: ModelStateSnapshot;
  after?: ModelStateSnapshot;
}

// "Before / After" two-card comparison used in the Chain Inspector.
// Field rows render label · value, with an optional dot-tone marker on
// the value (critical, warn, neutral, positive, muted).
export function BeforeAfter({ before, after }: Props) {
  if (!before && !after) return null;
  return (
    <div className="lg-ba">
      <StateCard heading="Before" snapshot={before} />
      <div className="lg-ba__arrow" aria-hidden="true">
        <svg width="20" height="20" viewBox="0 0 20 20">
          <path d="M3 10h13M11.5 5.5 16 10l-4.5 4.5" fill="none" stroke="currentColor" strokeWidth="1.4" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>
      <StateCard heading="After" snapshot={after} />
    </div>
  );
}

function StateCard({
  heading,
  snapshot,
}: {
  heading: string;
  snapshot?: ModelStateSnapshot;
}) {
  return (
    <div className="lg-state-card">
      <div className="lg-micro-label">{heading}</div>
      {snapshot ? (
        <ul className="lg-state-card__list">
          {snapshot.fields.map((f, i) => (
            <li className="lg-state-card__row" key={i}>
              <span className="lg-state-card__label">{f.label}</span>
              <span
                className={`lg-state-card__value lg-state-card__value--${f.tone ?? "neutral"}`}
              >
                {f.tone && f.tone !== "neutral" ? (
                  <span
                    className={`lg-tone-dot lg-tone-dot--${f.tone}`}
                    aria-hidden="true"
                  />
                ) : null}
                {f.value}
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <div className="lg-empty">No snapshot recorded.</div>
      )}
    </div>
  );
}
