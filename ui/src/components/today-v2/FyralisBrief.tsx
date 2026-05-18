// Fyralis Brief — three-column synthesis panel.
//
// Left column:    sparkle icon · FYRALIS BRIEF label · synthesis sentence
// Middle column:  WHAT CHANGED label · 3–5 bullet rows with direction
// Right column:   HANDLED WITHOUT YOU label · 3 stat rows with icons
// Footer:         See all activity → (right-aligned)

import type {
  DecisionDelta,
  HandledWithoutYouSummary,
} from "@/api/today-page-types";

interface Props {
  synthesis?: string;
  whatChanged: WhatChangedItem[];
  handled: HandledWithoutYouSummary;
}

export interface WhatChangedItem {
  text: string;
  direction?: "up" | "down" | "neutral";
}

export function FyralisBrief({ synthesis, whatChanged, handled }: Props) {
  return (
    <section className="tdv2-brief" data-testid="fyralis-brief">
      <div className="tdv2-brief__grid">
        <div className="tdv2-brief__synthesis-col">
          <div className="tdv2-brief__badge" aria-hidden="true">
            <SparkleIcon />
          </div>
          <div className="tdv2-brief__synthesis-text">
            <p className="tdv2-brief__eyebrow">Fyralis brief</p>
            {synthesis ? (
              <p className="tdv2-brief__synthesis">{synthesis}</p>
            ) : null}
          </div>
        </div>
        <div className="tdv2-brief__col">
          <h3 className="tdv2-brief__heading">What changed</h3>
          {whatChanged.length > 0 ? (
            <ul className="tdv2-brief__list">
              {whatChanged.slice(0, 5).map((c, i) => (
                <li key={i} className="tdv2-brief__list-item">
                  <DirectionGlyph dir={c.direction ?? "neutral"} />
                  <span>{c.text}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="tdv2-brief__empty">
              No material changes since your last session.
            </p>
          )}
        </div>
        <div className="tdv2-brief__col">
          <h3 className="tdv2-brief__heading">Handled without you</h3>
          <ul className="tdv2-brief__list">
            <li className="tdv2-brief__stat">
              <IconChip><SignalsIcon /></IconChip>
              <span className="tdv2-brief__stat-value">{handled.signalsAbsorbed}</span>
              <span>signals absorbed</span>
            </li>
            <li className="tdv2-brief__stat">
              <IconChip><UpdatesIcon /></IconChip>
              <span className="tdv2-brief__stat-value">{handled.modelUpdatesApplied}</span>
              <span>model updates applied</span>
            </li>
            <li className="tdv2-brief__stat">
              <IconChip><MonitorIcon /></IconChip>
              <span className="tdv2-brief__stat-value">{handled.itemsUnderMonitoring}</span>
              <span>items under monitoring</span>
            </li>
          </ul>
        </div>
      </div>
      <div className="tdv2-brief__footer">
        <a className="tdv2-brief__link" href="/ledger">
          See all activity →
        </a>
      </div>
    </section>
  );
}

// Derive a "What changed" list from the visible queue. Title is the
// short clause before any " — " action hint; direction comes from the
// proposed-state severity.
export function deriveWhatChanged(deltas: DecisionDelta[]): WhatChangedItem[] {
  const out: WhatChangedItem[] = [];
  for (const d of deltas) {
    if (out.length >= 5) break;
    const sev = d.proposedState.find((f) => f.severity)?.severity;
    const dir: WhatChangedItem["direction"] =
      sev === "positive"
        ? "down"
        : sev === "critical" || sev === "watch"
          ? "up"
          : "neutral";
    out.push({ text: shorten(d.title), direction: dir });
  }
  return out;
}

function shorten(s: string): string {
  const stripped = s.replace(/ — .*$/, "").replace(/ - .*$/, "");
  if (stripped.length <= 60) return stripped;
  return stripped.slice(0, 57).trimEnd() + "…";
}

function IconChip({ children }: { children: React.ReactNode }) {
  return <span className="tdv2-brief__chip" aria-hidden="true">{children}</span>;
}

function DirectionGlyph({ dir }: { dir: "up" | "down" | "neutral" }) {
  const cls = `tdv2-brief__dir tdv2-brief__dir--${dir}`;
  return (
    <span className={cls} aria-hidden="true">
      <svg
        width="10"
        height="10"
        viewBox="0 0 10 10"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        {dir === "up" ? (
          <>
            <path d="M5 8.5V2" />
            <path d="M2.5 4.5L5 2l2.5 2.5" />
          </>
        ) : dir === "down" ? (
          <>
            <path d="M5 1.5V8" />
            <path d="M2.5 5.5L5 8l2.5-2.5" />
          </>
        ) : (
          <path d="M2 5h6" />
        )}
      </svg>
    </span>
  );
}

function SparkleIcon() {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 18 18"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M9 2v3" />
      <path d="M9 13v3" />
      <path d="M2 9h3" />
      <path d="M13 9h3" />
      <path d="M4.5 4.5L6 6" />
      <path d="M12 12l1.5 1.5" />
      <path d="M4.5 13.5L6 12" />
      <path d="M12 6l1.5-1.5" />
    </svg>
  );
}

function SignalsIcon() {
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
      <circle cx="7" cy="7" r="1.4" />
      <path d="M4 4a4 4 0 0 0 0 6" />
      <path d="M10 4a4 4 0 0 1 0 6" />
    </svg>
  );
}

function UpdatesIcon() {
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
      <rect x="2.5" y="2.5" width="9" height="9" rx="1.5" />
      <path d="M5 7l1.5 1.5L9.5 5.5" />
    </svg>
  );
}

function MonitorIcon() {
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
      <circle cx="7" cy="7" r="3.5" />
      <circle cx="7" cy="7" r="1.2" />
    </svg>
  );
}
