import type { LedgerTimelineStep } from "@/api/history-types";
import { typeMeta } from "./event-taxonomy";
import { formatTime } from "./ledger-utils";

export interface MiniTimelineProps {
  steps: LedgerTimelineStep[];
}

export function MiniTimeline({ steps }: MiniTimelineProps) {
  if (steps.length === 0) return null;
  return (
    <ol
      className="fy-ledger__mini-timeline"
      data-testid="ledger-mini-timeline"
    >
      {steps.map((step) => {
        const meta = typeMeta(step.event_type);
        return (
          <li className="fy-ledger__mini-step" key={step.id}>
            <span
              className="fy-ledger__mini-dot"
              style={{ background: meta.cssVar }}
              aria-hidden="true"
            />
            <span className="fy-ledger__mini-time">
              {formatTime(step.timestamp)}
            </span>
            <span className="fy-ledger__mini-text">{step.text}</span>
          </li>
        );
      })}
    </ol>
  );
}
