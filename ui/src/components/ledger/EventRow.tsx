import type { LedgerEvent } from "@/api/history-types";
import { typeMeta } from "./event-taxonomy";
import { formatTime } from "./ledger-utils";

export interface EventRowProps {
  event: LedgerEvent;
  selected: boolean;
  onClick: () => void;
}

function actorLabel(actor: LedgerEvent["actor"]): string {
  switch (actor.kind) {
    case "person":
      return actor.role ? `${actor.name} ${actor.role}` : actor.name;
    case "system":
    case "integration":
      return actor.name;
  }
}

export function EventRow({ event, selected, onClick }: EventRowProps) {
  const meta = typeMeta(event.type);
  return (
    <button
      type="button"
      onClick={onClick}
      data-event-id={event.id}
      data-event-type={event.type}
      data-selected={selected ? "true" : undefined}
      aria-selected={selected}
      className={
        "fy-ledger__event-row" +
        (selected ? " fy-ledger__event-row--selected" : "")
      }
      style={{ ["--ledger-accent" as never]: meta.cssVar }}
    >
      <span className="fy-ledger__event-rail">
        <span className="fy-ledger__event-time">
          {formatTime(event.timestamp)}
        </span>
        <span
          className="fy-ledger__event-dot"
          aria-hidden="true"
        />
      </span>
      <span
        className={
          "fy-ledger__event-icon fy-ledger__event-icon--" + meta.className
        }
        aria-hidden="true"
      >
        <EventIcon type={event.type} />
      </span>
      <span className="fy-ledger__event-main">
        <span
          className={
            "fy-ledger__event-type fy-ledger__event-type--" + meta.className
          }
        >
          {meta.label}
        </span>
        <span className="fy-ledger__event-title">{event.title}</span>
        <span className="fy-ledger__event-summary">{event.summary}</span>
        {event.tags.length > 0 ? (
          <span className="fy-ledger__event-tags">
            {event.tags.map((tag) => (
              <span className="fy-ledger__event-tag" key={tag}>
                {tag}
              </span>
            ))}
          </span>
        ) : null}
      </span>
      <span className="fy-ledger__event-actor">
        <span className="fy-ledger__event-actor-name">
          {actorLabel(event.actor)}
        </span>
        <svg
          width="14"
          height="14"
          viewBox="0 0 14 14"
          className="fy-ledger__event-chevron"
          aria-hidden="true"
        >
          <path
            d="M5 3 9 7l-4 4"
            fill="none"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </span>
    </button>
  );
}

function EventIcon({ type }: { type: LedgerEvent["type"] }) {
  switch (type) {
    case "action_taken":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <path
            d="M3 7l2.5 2.5L11 4"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "model_update":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <circle cx="3.5" cy="4" r="1.4" stroke="currentColor" strokeWidth="1.2" />
          <circle cx="10.5" cy="10" r="1.4" stroke="currentColor" strokeWidth="1.2" />
          <path d="M4.5 5 9.5 9" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
    case "prediction_made":
    case "prediction_resolved":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <path
            d="M2 11 5.5 6 8 8.5 12 3"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      );
    case "observation_ingested":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <path
            d="M7 3.5v7M3.5 7h7"
            stroke="currentColor"
            strokeWidth="1.4"
            strokeLinecap="round"
          />
        </svg>
      );
    case "contestation":
      return (
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
          <circle cx="7" cy="7" r="4.6" stroke="currentColor" strokeWidth="1.2" />
          <path d="M5 5l4 4M9 5l-4 4" stroke="currentColor" strokeWidth="1.2" />
        </svg>
      );
  }
}
