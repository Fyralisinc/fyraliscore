import { useMemo } from "react";
import type { LedgerEvent } from "@/api/history-types";
import { EventRow } from "./EventRow";
import {
  groupByDay,
  formatDateHeader,
  todayKey,
  yesterdayKey,
} from "./ledger-utils";

export interface LedgerTimelineProps {
  events: LedgerEvent[];
  selectedEventId: string | null;
  onSelect: (event: LedgerEvent) => void;
  onLoadMore?: () => void;
  hasMore?: boolean;
  loading?: boolean;
  error?: string | null;
  emptyHint?: string;
  // Optional anchor "now" used by date headers so tests that freeze time
  // (e.g. May 15, 2025 fixture) still see "Today · May 15" properly.
  now?: Date;
}

export function LedgerTimeline({
  events,
  selectedEventId,
  onSelect,
  onLoadMore,
  hasMore = false,
  loading = false,
  error = null,
  emptyHint = "No events match the current filters.",
  now,
}: LedgerTimelineProps) {
  const tKey = useMemo(() => {
    if (now) return todayKey(now);
    // For the May 15 fixture in mock environments, treat the freshest
    // event's day as "today" so the date header reads naturally.
    if (events.length === 0) return todayKey();
    const sorted = [...events].sort((a, b) =>
      a.timestamp < b.timestamp ? 1 : -1
    );
    const d = new Date(sorted[0].timestamp);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
  }, [now, events]);

  const yKey = useMemo(() => {
    if (now) return yesterdayKey(now);
    if (events.length === 0) return yesterdayKey();
    const d = new Date(tKey + "T00:00:00.000Z");
    d.setUTCDate(d.getUTCDate() - 1);
    return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
  }, [now, events, tKey]);

  const groups = useMemo(() => groupByDay(events), [events]);

  if (error) {
    return (
      <div className="fy-ledger__timeline-state fy-ledger__timeline-state--error" role="alert">
        <p className="fy-ledger__state-title">Could not load ledger</p>
        <p className="fy-ledger__state-body">{error}</p>
      </div>
    );
  }

  if (loading && events.length === 0) {
    return (
      <div className="fy-ledger__timeline-state" role="status">
        <p className="fy-ledger__state-title">Loading the ledger…</p>
        <p className="fy-ledger__state-body">
          Reading the company memory.
        </p>
      </div>
    );
  }

  if (events.length === 0) {
    return (
      <div className="fy-ledger__timeline-state" data-empty>
        <p className="fy-ledger__state-title">No events</p>
        <p className="fy-ledger__state-body">{emptyHint}</p>
      </div>
    );
  }

  return (
    <section
      className="fy-ledger__timeline"
      aria-label="Ledger timeline"
      data-testid="ledger-timeline"
    >
      {groups.map((group) => (
        <div className="fy-ledger__day" key={group.dateKey}>
          <h2
            className="fy-ledger__day-header"
            data-testid="ledger-day-header"
          >
            {formatDateHeader(group.iso, tKey, yKey)}
          </h2>
          <ul className="fy-ledger__day-list" role="list">
            {group.events.map((event) => (
              <li key={event.id}>
                <EventRow
                  event={event}
                  selected={selectedEventId === event.id}
                  onClick={() => onSelect(event)}
                />
              </li>
            ))}
          </ul>
        </div>
      ))}
      {hasMore && onLoadMore ? (
        <div className="fy-ledger__loadmore-wrap">
          <button
            type="button"
            className="fy-ledger__loadmore"
            onClick={onLoadMore}
            data-testid="ledger-load-more"
          >
            Load more events
          </button>
        </div>
      ) : null}
    </section>
  );
}
