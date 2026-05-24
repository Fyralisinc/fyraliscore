// Handled Without You — spec §4.8.
//
// Reinforces delegated intelligence at the bottom of Briefing Mode.
// Calm receipt panel, not filler.

import type { HandledWithoutYouSummary } from "@/api/today-page-types";

interface Props {
  summary: HandledWithoutYouSummary;
}

export function HandledWithoutYou({ summary }: Props) {
  const total =
    summary.signalsAbsorbed +
    summary.modelUpdatesApplied +
    summary.itemsUnderMonitoring +
    summary.delegatedChanges +
    summary.contestedChanges;
  if (total === 0) return null;
  return (
    <section className="tdv2-handled" data-testid="handled-without-you-panel">
      <header className="tdv2-handled__head">
        <h3 className="tdv2-handled__title">Handled without you</h3>
        <p className="tdv2-handled__copy">
          Fyralis handled {summary.signalsAbsorbed} signals without needing you.
          {summary.modelUpdatesApplied > 0
            ? ` ${summary.modelUpdatesApplied} model updates were applied.`
            : ""}
          {summary.itemsUnderMonitoring > 0
            ? ` ${summary.itemsUnderMonitoring} items are under monitoring.`
            : ""}
        </p>
      </header>
      <ul className="tdv2-handled__cells">
        <Cell value={summary.signalsAbsorbed} label="signals absorbed" />
        <Cell value={summary.modelUpdatesApplied} label="model updates applied" />
        <Cell value={summary.itemsUnderMonitoring} label="items monitoring" />
        <Cell value={summary.delegatedChanges} label="delegated changes" />
      </ul>
    </section>
  );
}

function Cell({ value, label }: { value: number; label: string }) {
  return (
    <li className="tdv2-handled__cell">
      <span className="tdv2-handled__value">{value}</span>
      <span className="tdv2-handled__label">{label}</span>
    </li>
  );
}
