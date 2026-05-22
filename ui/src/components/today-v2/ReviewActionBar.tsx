// Page-level action bar for the active review.
//
// Sticks to the bottom of the page below the rail+sheet split.
// Buttons: Accept change · Delegate · Request changes · Report correction.
// "Request changes" submits a soft pushback that re-evaluates the
// proposed change without rejecting it outright. We route it through
// the same Correction sheet for now since the wire payload is the same
// shape; product can split the two later if needed.

import type { DecisionDelta } from "@/api/today-page-types";

interface Props {
  delta: DecisionDelta;
  applying: boolean;
  onAccept: () => void;
  onDelegate: () => void;
  onRequestChanges: () => void;
  onCorrect: () => void;
}

export function ReviewActionBar({
  delta,
  applying,
  onAccept,
  onDelegate,
  onRequestChanges,
  onCorrect,
}: Props) {
  const canAccept = delta.availableActions.includes("accept");
  const canDelegate = delta.availableActions.includes("delegate");
  const canCorrect = delta.availableActions.includes("report_correction");
  return (
    <div
      className="tdv2-actionbar"
      data-testid={`action-bar-${delta.id}`}
      role="toolbar"
      aria-label="Review actions"
    >
      <div className="tdv2-actionbar__inner">
        {canAccept ? (
          <button
            type="button"
            className="tdv2-act tdv2-act--primary"
            onClick={onAccept}
            disabled={applying}
            data-testid={`focused-accept-${delta.id}`}
          >
            <CheckIcon />
            <span>{applying ? "Applying..." : "Accept change"}</span>
          </button>
        ) : null}
        {canDelegate ? (
          <button
            type="button"
            className="tdv2-act tdv2-act--secondary"
            onClick={onDelegate}
            data-testid={`focused-delegate-${delta.id}`}
          >
            <UsersIcon />
            <span>Delegate</span>
          </button>
        ) : null}
        <button
          type="button"
          className="tdv2-act tdv2-act--secondary"
          onClick={onRequestChanges}
          data-testid={`focused-request-changes-${delta.id}`}
        >
          <EditIcon />
          <span>Request changes</span>
        </button>
        {canCorrect ? (
          <button
            type="button"
            className="tdv2-act tdv2-act--correction"
            onClick={onCorrect}
            data-testid={`focused-correct-${delta.id}`}
          >
            <FlagIcon />
            <span>Report correction</span>
          </button>
        ) : null}
      </div>
    </div>
  );
}

function CheckIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3 7.7l3 3 6-6.4" />
    </svg>
  );
}

function UsersIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <circle cx="5.6" cy="6" r="2.1" />
      <circle cx="10.3" cy="6.6" r="1.6" />
      <path d="M1.8 12.5c.5-2 2-3 3.8-3s3.3 1 3.8 3" />
      <path d="M9.6 12.5c.3-1.4 1.2-2.2 2.4-2.2" />
    </svg>
  );
}

function EditIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3 12h2L11.5 5.5l-2-2L3 10z" />
      <path d="M9.5 3.5l2 2" />
    </svg>
  );
}

function FlagIcon() {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.4"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M3.5 13V2.6" />
      <path d="M3.5 2.6h7.3l-1.3 2.5 1.3 2.5h-7.3" />
    </svg>
  );
}
