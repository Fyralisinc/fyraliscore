// Today header.
//
//   Today                                    [Ask Fyralis  ⌘K] [avatar]
//   Fyralis reviewed the company since your last session.
//   98 signals processed · 91 absorbed · 7 need your judgment
//   May 18, 12:03 PM   View change log →
//
// "Ask Fyralis" is a global search/launcher; the per-card Ask strip
// inside the focused review sheet handles in-context follow-ups.

import type { TodaySummary } from "@/api/today-page-types";

interface Props {
  summary: TodaySummary;
  generatedAt: string;
}

function formatStamp(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return iso;
  }
}

export function BriefingHeader({ summary, generatedAt }: Props) {
  const need = summary.needJudgment;
  const absorbed = summary.signalsAbsorbed;
  const processed = summary.signalsProcessed;
  return (
    <header className="tdv2-header" data-testid="briefing-header">
      <div className="tdv2-header__top">
        <h1 className="tdv2-header__title">Today</h1>
        <div className="tdv2-header__tools">
          <button
            type="button"
            className="tdv2-header__ask"
            data-testid="header-ask"
            aria-label="Ask Fyralis"
          >
            <SearchIcon />
            <span className="tdv2-header__ask-label">Ask Fyralis</span>
            <span className="tdv2-header__ask-key" aria-hidden="true">
              ⌘ K
            </span>
          </button>
          <div className="tdv2-header__avatar" aria-label="Your profile">
            <div className="tdv2-header__avatar-img" aria-hidden="true" />
          </div>
        </div>
      </div>
      <p className="tdv2-header__briefing">
        Fyralis reviewed the company since your last session.
      </p>
      <p className="tdv2-header__receipt">
        <strong>{processed}</strong> signals processed
        <span className="tdv2-header__sep" aria-hidden="true">·</span>
        <span className="tdv2-em-absorbed">
          <strong>{absorbed}</strong> absorbed
        </span>
        <span className="tdv2-header__sep" aria-hidden="true">·</span>
        {need > 0 ? (
          <span className="tdv2-em-judgment">
            <strong>{need}</strong> need your judgment
          </span>
        ) : (
          <span className="tdv2-em-absorbed">Nothing needs your judgment</span>
        )}
      </p>
      <p className="tdv2-header__foot">
        <span className="tdv2-header__stamp">{formatStamp(generatedAt)}</span>
        <a className="tdv2-header__link" href="/ledger">
          View change log →
        </a>
      </p>
    </header>
  );
}

function SearchIcon() {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 14 14"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="6" cy="6" r="4" />
      <path d="M9 9l3 3" />
    </svg>
  );
}
