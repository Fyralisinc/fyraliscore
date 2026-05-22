// Other items needing judgment — spec §4.7.
//
// Compact list, not full review cards. Each row: title, one-line
// reason, confidence/due/status metadata, chevron. Clicking opens
// Review Mode for that item.

import type { DecisionDelta } from "@/api/today-page-types";

interface Props {
  items: DecisionDelta[];
  onReview: (id: string) => void;
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

function statusTone(status: DecisionDelta["status"]): string {
  if (status === "needs_authority") return "authority";
  if (status === "delegatable") return "delegate";
  if (status === "monitoring") return "monitor";
  if (status === "contested" || status === "correction_submitted") return "contest";
  return "neutral";
}

function confidencePct(c?: number | null): string | null {
  if (c == null) return null;
  return `${Math.round(c * 100)}% confidence`;
}

export function OtherItemsList({ items, onReview }: Props) {
  if (items.length === 0) return null;
  return (
    <section className="tdv2-others" data-testid="other-items">
      <header className="tdv2-others__head">
        <h3 className="tdv2-others__heading">Other items needing your judgment</h3>
        <span className="tdv2-others__count">{items.length}</span>
      </header>
      <ul className="tdv2-others__list">
        {items.map((d) => {
          const tone = statusTone(d.status);
          const conf = confidencePct(d.confidence);
          const due = relativeDue(d.resolutionTargetAt);
          const reason = d.whyThisMatters?.split(". ")[0] ?? d.summaryLine;
          return (
            <li key={d.id}>
              <button
                type="button"
                className={`tdv2-others__row tdv2-others__row--${tone}`}
                onClick={() => onReview(d.id)}
                data-testid={`other-row-${d.id}`}
              >
                <span className="tdv2-others__rail" aria-hidden="true" />
                <span className="tdv2-others__body">
                  <span className="tdv2-others__title">{d.title}</span>
                  {reason ? (
                    <span className="tdv2-others__reason">{reason}</span>
                  ) : null}
                  <span className="tdv2-others__meta">
                    {conf ? <span>{conf}</span> : null}
                    {conf && due ? (
                      <span className="tdv2-others__dot" aria-hidden="true">·</span>
                    ) : null}
                    {due ? <span>{due}</span> : null}
                  </span>
                </span>
                <span className="tdv2-others__chev" aria-hidden="true">
                  <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                    <path
                      d="M5.5 3.5l3.5 3.5-3.5 3.5"
                      stroke="currentColor"
                      strokeWidth="1.4"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                  </svg>
                </span>
              </button>
            </li>
          );
        })}
      </ul>
    </section>
  );
}
