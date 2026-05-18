// Briefing header — spec §7.1. One-line, briefing-first re-entry.
//
//   Today
//   Fyralis reviewed N signals since your last session.
//   K require judgment; M were absorbed.
//
// No dashboard counters underneath. The header's job is to set the tone
// (Fyralis already protected the user's attention), not to load chrome.

import type { TodaySummary } from "@/api/today-page-types";

interface Props {
  summary: TodaySummary;
  generatedAt: string;
}

export function BriefingHeader({ summary }: Props) {
  const need = summary.needJudgment;
  const absorbed = summary.signalsAbsorbed;
  const processed = summary.signalsProcessed;

  return (
    <header className="tdv2-header" data-testid="briefing-header">
      <div className="tdv2-header__title-wrap">
        <h1 className="tdv2-header__title">Today</h1>
        <p className="tdv2-header__briefing">
          Fyralis reviewed {processed} signals since your last session.{" "}
          {need > 0 ? (
            <>
              {need} require judgment; {absorbed} were absorbed.
            </>
          ) : (
            <>Nothing needs your judgment; {absorbed} were absorbed.</>
          )}
        </p>
      </div>
    </header>
  );
}
