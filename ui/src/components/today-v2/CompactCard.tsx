// Compact judgment card — spec §8.
//
// Minimal: title, one summary line, a confidence/status chip on the
// right, and a small "created" timestamp. The whole card is a single
// button: clicking expands the item in place.

import type { DecisionDelta } from "@/api/today-page-types";

interface Props {
  delta: DecisionDelta;
  onOpen: () => void;
}

function relativeTime(iso: string): string {
  const created = new Date(iso).getTime();
  if (Number.isNaN(created)) return iso;
  const delta = Date.now() - created;
  const minutes = Math.floor(delta / 60_000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function confidenceLabel(c?: number | null): {
  pct: string;
  band: "low" | "moderate" | "high";
} | null {
  if (c == null) return null;
  const pct = Math.round(c * 100);
  let band: "low" | "moderate" | "high" = "moderate";
  if (pct >= 75) band = "high";
  else if (pct < 55) band = "low";
  return { pct: `${pct}% confidence`, band };
}

export function CompactCard({ delta, onOpen }: Props) {
  const conf = confidenceLabel(delta.confidence);
  const stateRail = railStateFor(delta.status);
  return (
    <article
      className={`tdv2-compact tdv2-compact--${stateRail}`}
      data-testid={`compact-card-${delta.id}`}
    >
      <button
        type="button"
        className="tdv2-compact__btn"
        onClick={onOpen}
        aria-expanded={false}
        aria-controls={`focused-${delta.id}`}
        data-testid={`compact-row-${delta.id}`}
      >
        <span className="tdv2-compact__rail" aria-hidden="true" />
        <span className="tdv2-compact__body">
          <span className="tdv2-compact__title">{delta.title}</span>
          {delta.summaryLine ? (
            <span className="tdv2-compact__summary">{delta.summaryLine}</span>
          ) : null}
        </span>
        <span className="tdv2-compact__meta">
          {conf ? (
            <span className={`tdv2-confidence tdv2-confidence--${conf.band}`}>
              {conf.pct}
            </span>
          ) : null}
          <span className="tdv2-compact__age">
            Created {relativeTime(delta.createdAt)}
          </span>
          <span className="tdv2-compact__chev" aria-hidden="true">
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
              <path
                d="M3.5 5.5l3.5 3.5 3.5-3.5"
                stroke="currentColor"
                strokeWidth="1.4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </span>
        </span>
      </button>
    </article>
  );
}

function railStateFor(status: DecisionDelta["status"]): string {
  if (status === "needs_authority") return "authority";
  if (status === "delegatable") return "delegate";
  if (status === "monitoring") return "monitor";
  if (status === "contested" || status === "correction_submitted") return "contest";
  return "neutral";
}
