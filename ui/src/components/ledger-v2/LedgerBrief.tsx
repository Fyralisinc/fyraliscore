import type { BriefItem, LedgerBriefData } from "@/api/ledger-v2-types";

interface Props {
  data: LedgerBriefData;
  onViewAllInsights?: () => void;
}

// Ledger Brief — calm summary band above the workspace.
// Left moss rail, brief statement in serif, then three semantic
// columns (Resolved / Still open / Fyralis learned) and an inline
// "View all insights →" link.
export function LedgerBrief({ data, onViewAllInsights }: Props) {
  return (
    <section className="lg-brief" aria-labelledby="lg-brief-label">
      <span className="lg-brief__rail" aria-hidden="true" />
      <div className="lg-brief__inner">
        <div className="lg-brief__statement-col">
          <div id="lg-brief-label" className="lg-micro-label">
            Ledger brief
          </div>
          <p className="lg-brief__statement">{data.statement}</p>
        </div>
        <BriefColumn
          label="Resolved"
          icon={<CheckIcon />}
          tone="moss"
          items={data.resolved}
        />
        <BriefColumn
          label="Still open"
          icon={<WarnIcon />}
          tone="gold"
          items={data.stillOpen}
        />
        <div className="lg-brief__col lg-brief__col--learned">
          <BriefColumn
            label="Fyralis learned"
            icon={<SparkIcon />}
            tone="lapis"
            items={data.learned}
            embed
          />
          <button
            type="button"
            className="lg-link lg-brief__more"
            onClick={onViewAllInsights}
          >
            View all insights →
          </button>
        </div>
      </div>
    </section>
  );
}

interface ColumnProps {
  label: string;
  icon: React.ReactNode;
  tone: "moss" | "gold" | "lapis";
  items: BriefItem[];
  embed?: boolean;
}

function BriefColumn({ label, icon, tone, items, embed }: ColumnProps) {
  const root = embed ? "lg-brief__col lg-brief__col--embed" : "lg-brief__col";
  return (
    <div className={root}>
      <div className="lg-micro-label">{label}</div>
      <ul className="lg-brief__list">
        {items.length === 0 ? (
          <li className="lg-brief__empty">—</li>
        ) : null}
        {items.map((item) => (
          <li
            key={item.id}
            className={`lg-brief__item lg-brief__item--${tone}`}
          >
            <span className="lg-brief__item-icon" aria-hidden="true">
              {icon}
            </span>
            <span className="lg-brief__item-label">{item.label}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function CheckIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
      <circle cx="7" cy="7" r="6" fill="currentColor" opacity="0.12" />
      <path
        d="M4 7.2 6.2 9.4 10 5.6"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function WarnIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
      <path
        d="M7 1.5 13 12H1z"
        fill="currentColor"
        opacity="0.18"
        stroke="currentColor"
        strokeWidth="1.1"
        strokeLinejoin="round"
      />
      <path d="M7 5.5v3M7 10.2v0.4" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
    </svg>
  );
}

function SparkIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
      <path
        d="M7 1.5 8 5.6 12 7 8 8.4 7 12.5 6 8.4 2 7 6 5.6z"
        fill="currentColor"
        opacity="0.6"
      />
    </svg>
  );
}
