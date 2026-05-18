// Local Review Queue Rail.
//
//   PRIMARY JUDGMENT   1 OF N
//   ┌─────────────────────────┐
//   │ [icon]                  │
//   │ Escalate customer risk  │
//   │ for Salesforce ...      │
//   │ At watch → Critical     │
//   │ [78% confidence]        │
//   └─────────────────────────┘
//
//   OTHER ITEMS NEEDING YOUR JUDGMENT   6
//
//   [icon] Assign owner for pricing...
//          Unassigned
//          [72% confidence]
//          Due in 5 days
//
//   ...
//
//   HANDLED WITHOUT YOU
//   [👤]  91  Signals absorbed
//   [☐]   12  Model updates applied
//   [○]    5  Items under monitoring
//   [✓]    All quiet · No new exposures

import type {
  DecisionDelta,
  HandledWithoutYouSummary,
  ModelCategoryKey,
} from "@/api/today-page-types";

interface Props {
  items: DecisionDelta[];
  selectedId: string;
  handled: HandledWithoutYouSummary;
  onSelect: (id: string) => void;
}

function confPct(c?: number | null): string | null {
  if (c == null) return null;
  return `${Math.round(c * 100)}% confidence`;
}

function statusTone(status: DecisionDelta["status"]): string {
  if (status === "needs_authority") return "authority";
  if (status === "delegatable") return "delegate";
  if (status === "monitoring") return "monitor";
  if (status === "contested" || status === "correction_submitted") return "contest";
  return "neutral";
}

function relativeDue(iso?: string | null): string | null {
  if (!iso) return null;
  const target = new Date(iso).getTime();
  if (Number.isNaN(target)) return null;
  const delta = target - Date.now();
  const days = Math.round(delta / 86_400_000);
  if (days === 0) return "Due today";
  if (days > 0) return `Due in ${days} day${days === 1 ? "" : "s"}`;
  return `${Math.abs(days)} day${days === -1 ? "" : "s"} overdue`;
}

export function ReviewQueueRail({ items, selectedId, handled, onSelect }: Props) {
  const primary = items[0];
  const others = items.slice(1);

  return (
    <aside
      className="tdv2-rail"
      data-testid="review-rail"
      aria-label="Review queue"
    >
      {primary ? (
        <div className="tdv2-rail__group">
          <header className="tdv2-rail__group-head">
            <span className="tdv2-rail__group-label">Primary judgment</span>
            <span className="tdv2-rail__group-count">1 of {items.length}</span>
          </header>
          <PrimaryRow
            delta={primary}
            selected={primary.id === selectedId}
            onSelect={() => onSelect(primary.id)}
          />
        </div>
      ) : null}

      {others.length > 0 ? (
        <div className="tdv2-rail__group">
          <header className="tdv2-rail__group-head">
            <span className="tdv2-rail__group-label">
              Other items needing your judgment
            </span>
            <span className="tdv2-rail__group-count">{others.length}</span>
          </header>
          <ul className="tdv2-rail__list">
            {others.map((d) => (
              <li key={d.id}>
                <OtherRow
                  delta={d}
                  selected={d.id === selectedId}
                  onSelect={() => onSelect(d.id)}
                />
              </li>
            ))}
          </ul>
          <a className="tdv2-rail__see-all" href="#all-items">
            View all {items.length} items →
          </a>
        </div>
      ) : null}

      <div className="tdv2-rail__group tdv2-rail__group--handled">
        <header className="tdv2-rail__group-head">
          <span className="tdv2-rail__group-label">Handled without you</span>
        </header>
        <ul className="tdv2-rail__stats">
          <HandledStat
            icon={<UsersGlyph />}
            value={handled.signalsAbsorbed}
            label="Signals absorbed"
          />
          <HandledStat
            icon={<UpdatesGlyph />}
            value={handled.modelUpdatesApplied}
            label="Model updates applied"
          />
          <HandledStat
            icon={<MonitorGlyph />}
            value={handled.itemsUnderMonitoring}
            label="Items under monitoring"
          />
          <HandledStat
            icon={<CheckGlyph />}
            label="All quiet"
            sub="No new exposures"
          />
        </ul>
      </div>
    </aside>
  );
}

function PrimaryRow({
  delta,
  selected,
  onSelect,
}: {
  delta: DecisionDelta;
  selected: boolean;
  onSelect: () => void;
}) {
  const tone = statusTone(delta.status);
  const conf = confPct(delta.confidence);
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-current={selected ? "true" : undefined}
      data-testid={`rail-row-${delta.id}`}
      className={[
        "tdv2-rail__primary",
        `tdv2-rail__primary--${tone}`,
        selected ? "tdv2-rail__primary--selected" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <span className="tdv2-rail__primary-icon" aria-hidden="true">
        <CategoryGlyph cat={delta.sourceCategory} />
      </span>
      <span className="tdv2-rail__primary-body">
        <span className="tdv2-rail__primary-title">{delta.title}</span>
        {delta.summaryLine ? (
          <span className="tdv2-rail__primary-summary">{delta.summaryLine}</span>
        ) : null}
        {conf ? (
          <span className="tdv2-confidence tdv2-confidence--moderate">{conf}</span>
        ) : null}
      </span>
    </button>
  );
}

function OtherRow({
  delta,
  selected,
  onSelect,
}: {
  delta: DecisionDelta;
  selected: boolean;
  onSelect: () => void;
}) {
  const tone = statusTone(delta.status);
  const conf = confPct(delta.confidence);
  const due = relativeDue(delta.resolutionTargetAt);
  const reason = delta.summaryLine || delta.whyThisMatters?.split(". ")[0] || "";
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-current={selected ? "true" : undefined}
      data-testid={`rail-row-${delta.id}`}
      className={[
        "tdv2-rail__other",
        `tdv2-rail__other--${tone}`,
        selected ? "tdv2-rail__other--selected" : "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <span className="tdv2-rail__other-icon" aria-hidden="true">
        <CategoryGlyph cat={delta.sourceCategory} />
      </span>
      <span className="tdv2-rail__other-body">
        <span className="tdv2-rail__other-title">{delta.title}</span>
        {reason ? <span className="tdv2-rail__other-reason">{reason}</span> : null}
        <span className="tdv2-rail__other-meta">
          {conf ? (
            <span className="tdv2-confidence tdv2-confidence--moderate">{conf}</span>
          ) : null}
          {due ? <span className="tdv2-rail__other-due">{due}</span> : null}
        </span>
      </span>
    </button>
  );
}

function HandledStat({
  icon,
  value,
  label,
  sub,
}: {
  icon: React.ReactNode;
  value?: number;
  label: string;
  sub?: string;
}) {
  return (
    <li className="tdv2-rail__stat">
      <span className="tdv2-rail__stat-icon" aria-hidden="true">{icon}</span>
      {value != null ? (
        <span className="tdv2-rail__stat-value">{value}</span>
      ) : null}
      <span className="tdv2-rail__stat-body">
        <span className="tdv2-rail__stat-label">{label}</span>
        {sub ? <span className="tdv2-rail__stat-sub">{sub}</span> : null}
      </span>
    </li>
  );
}

function CategoryGlyph({ cat }: { cat: ModelCategoryKey }) {
  // Keep glyph minimal but distinct by category. All glyphs share size
  // and stroke so the rail reads as a uniform list.
  const common = {
    width: 16,
    height: 16,
    viewBox: "0 0 16 16",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.4,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
  switch (cat) {
    case "customers_revenue":
      return (
        <svg {...common}>
          <path d="M2 12l3-4 3 2 3-4 3 3" />
          <circle cx="13" cy="3" r="1" />
        </svg>
      );
    case "decisions":
      return (
        <svg {...common}>
          <rect x="3" y="3" width="10" height="10" rx="1.5" />
          <path d="M6 8.5l1.5 1.5L11 6" />
        </svg>
      );
    case "commitments":
      return (
        <svg {...common}>
          <path d="M8 2v6l3 2" />
          <circle cx="8" cy="8" r="6" />
        </svg>
      );
    case "risks_constraints":
      return (
        <svg {...common}>
          <path d="M8 2l6 11H2z" />
          <path d="M8 6v3" />
          <circle cx="8" cy="11" r="0.4" fill="currentColor" />
        </svg>
      );
    case "people_teams":
      return (
        <svg {...common}>
          <circle cx="6" cy="6" r="2" />
          <circle cx="11" cy="7" r="1.5" />
          <path d="M2 13c.5-2 2-3 4-3s3.5 1 4 3" />
          <path d="M10 13c.3-1.4 1.2-2.2 2.5-2.2" />
        </svg>
      );
    case "systems_capacity":
      return (
        <svg {...common}>
          <rect x="2" y="3" width="12" height="8" rx="1" />
          <path d="M2 7h12" />
          <path d="M5 13h6" />
        </svg>
      );
    case "finance_capital":
      return (
        <svg {...common}>
          <path d="M8 3v10" />
          <path d="M11 5c-.6-1-1.7-1.5-3-1.5C6 3.5 5 4.5 5 6c0 1.2 1 2 3 2s3 .9 3 2.2C11 11.5 9.7 12.5 8 12.5c-1.5 0-2.6-.6-3.2-1.6" />
        </svg>
      );
    default:
      return (
        <svg {...common}>
          <circle cx="8" cy="8" r="5" />
          <circle cx="8" cy="8" r="1.5" />
        </svg>
      );
  }
}

function UsersGlyph() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="5.5" cy="5" r="2" />
      <circle cx="10" cy="6" r="1.5" />
      <path d="M2 12c.4-2 1.8-3 3.5-3s3 1 3.5 3" />
      <path d="M9 12c.2-1.3 1-2 2-2" />
    </svg>
  );
}

function UpdatesGlyph() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="2" y="2.5" width="10" height="9" rx="1" />
      <path d="M4.5 7l1.5 1.5L9.5 5" />
    </svg>
  );
}

function MonitorGlyph() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="7" cy="7" r="4" />
      <circle cx="7" cy="7" r="1.2" />
    </svg>
  );
}

function CheckGlyph() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="7" cy="7" r="5" />
      <path d="M4.5 7.2l1.6 1.6L9.5 5.4" />
    </svg>
  );
}
