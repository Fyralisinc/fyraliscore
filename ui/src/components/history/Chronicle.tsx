import { useEffect, useMemo, useState } from "react";
import type {
  Arc,
  HistoryEvent,
  HistoryFilters,
  EventType,
} from "./types";

// Spec Parts 4-7 — Chronicle layer: timeline + adaptive bucket headers +
// arc threading + three event prominence levels.

const NOW = new Date("2026-04-29T18:00:00Z");
const DAY = 24 * 60 * 60 * 1000;

type Props = {
  events: HistoryEvent[];
  arcs: Arc[];
  filters: HistoryFilters;
  onEventClick: (id: string) => void;
  onArcClick: (id: string) => void;
  onFiltersChange: (f: HistoryFilters) => void;
};

export function Chronicle({
  events,
  filters,
  onEventClick,
  onArcClick,
  onFiltersChange,
}: Props) {
  const [collapsed, setCollapsed] = useState<Set<string>>(() => new Set());
  const [expandedAggregates, setExpandedAggregates] = useState<Set<string>>(
    () => new Set()
  );

  const visibleEvents = useMemo(
    () => filterEvents(events, filters),
    [events, filters]
  );

  const buckets = useMemo(() => bucketEvents(visibleEvents), [visibleEvents]);

  // Always show Today bucket as anchor even if empty.
  const hasToday = buckets.some((b) => b.id === "today");
  if (!hasToday) {
    buckets.unshift({
      id: "today",
      title: bucketTitleFor("today"),
      events: [],
    });
  }

  function toggleBucket(id: string) {
    setCollapsed((prev) => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  }

  function toggleAggregate(id: string) {
    setExpandedAggregates((prev) => {
      const n = new Set(prev);
      if (n.has(id)) n.delete(id);
      else n.add(id);
      return n;
    });
  }

  if (visibleEvents.length === 0 && events.length > 0) {
    return (
      <div className="chronicle">
        <div className="filter-zero">
          <p>No events match your filters.</p>
          <button
            type="button"
            className="btn-text"
            onClick={() =>
              onFiltersChange({
                ...filters,
                period: "90d",
                significance: "all",
                arcsOn: true,
                search: "",
                arcId: null,
                types: new Set<EventType>(),
              })
            }
          >
            Clear filters
          </button>
        </div>
      </div>
    );
  }

  if (events.length === 0) {
    return (
      <div className="chronicle">
        <div className="empty-state-overlay">
          <p className="empty-state-text">
            Your timeline will fill out as the substrate observes your
            company. Right now I'm just starting to listen.
          </p>
          <p className="empty-state-attribution">— Driftwood</p>
        </div>
      </div>
    );
  }

  let staggerIndex = 0;
  return (
    <div className="chronicle" role="feed" aria-label="Event timeline">
      {buckets.map((bucket, bIdx) => {
        const isCollapsed = collapsed.has(bucket.id);
        return (
          <section
            key={bucket.id}
            className={"bucket" + (isCollapsed ? " collapsed" : "")}
            data-bucket-id={bucket.id}
          >
            <header
              className="bucket-header"
              onClick={() => toggleBucket(bucket.id)}
              style={
                {
                  ["--stagger-index" as string]: `${bIdx}`,
                } as React.CSSProperties
              }
            >
              <h3 className="bucket-title">{bucket.title}</h3>
              <span className="bucket-rule" />
              <span
                className={
                  "bucket-chevron" + (isCollapsed ? " collapsed" : "")
                }
                aria-hidden="true"
              >
                ▾
              </span>
            </header>
            <div className="bucket-events">
              {bucket.events.length === 0 ? (
                <p className="bucket-empty-note">Nothing of note today.</p>
              ) : (
                bucket.events.map((evt) => {
                  staggerIndex += 1;
                  return (
                    <EventCard
                      key={evt.id}
                      evt={evt}
                      staggerIndex={staggerIndex}
                      expanded={expandedAggregates.has(evt.id)}
                      onClick={() => onEventClick(evt.id)}
                      onArcThreadClick={() =>
                        evt.arc && onArcClick(evt.arc)
                      }
                      onToggleAggregate={() => toggleAggregate(evt.id)}
                    />
                  );
                })
              )}
            </div>
          </section>
        );
      })}
    </div>
  );
}

type EventCardProps = {
  evt: HistoryEvent;
  staggerIndex: number;
  expanded: boolean;
  onClick: () => void;
  onArcThreadClick: () => void;
  onToggleAggregate: () => void;
};

function EventCard({
  evt,
  staggerIndex,
  expanded,
  onClick,
  onArcThreadClick,
  onToggleAggregate,
}: EventCardProps) {
  const time = new Date(evt.timestamp);
  const timeText = time.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const isAggregated = !!evt.aggregated;

  return (
    <article
      className={"event" + (isAggregated && expanded ? " aggregated expanded" : isAggregated ? " aggregated" : "")}
      data-prominence={evt.prominence}
      data-event-type={evt.type}
      data-arc={evt.arc}
      data-arc-position={evt.arc_position}
      data-id={evt.id}
      role="article"
      aria-label={`${evt.prominence} event: ${evt.title || evt.descriptor}`}
      onClick={(e) => {
        // don't trigger panel when clicking the see-all expander
        const target = e.target as HTMLElement;
        if (target.closest(".event-expand")) return;
        if (target.closest(".event-arc-thread")) return;
        onClick();
      }}
      style={
        {
          ["--stagger-index" as string]: `${staggerIndex}`,
        } as React.CSSProperties
      }
    >
      {evt.arc ? (
        <span
          className="event-arc-thread"
          data-arc-position={evt.arc_position}
          onClick={(e) => {
            e.stopPropagation();
            onArcThreadClick();
          }}
          title={`Part of arc ${evt.arc}`}
        />
      ) : null}
      <span className="event-marker" aria-hidden="true" />
      <div className="event-content">
        {evt.prominence === "minor" ? (
          <>
            <span className="event-time">{timeText}</span>
            <span className="event-descriptor">
              {isAggregated ? <strong>{evt.descriptor}</strong> : evt.descriptor}
            </span>
            {isAggregated ? (
              <button
                type="button"
                className="event-expand"
                aria-label={expanded ? "Collapse list" : "Expand list"}
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleAggregate();
                }}
              >
                {expanded ? "hide" : "see all"}
              </button>
            ) : null}
            {isAggregated && expanded ? (
              <ul className="event-expanded-list">
                {evt.aggregated!.map((a) => (
                  <li key={a.id}>
                    <span className="event-time">
                      {new Date(a.timestamp).toLocaleTimeString("en-US", {
                        hour: "2-digit",
                        minute: "2-digit",
                        hour12: false,
                      })}
                    </span>
                    <span>{a.descriptor}</span>
                  </li>
                ))}
              </ul>
            ) : null}
          </>
        ) : (
          <>
            <header className="event-header">
              <span className="event-title">{evt.title}</span>
              <span className="event-time">{timeText}</span>
            </header>
            <p className="event-descriptor">{evt.descriptor}</p>
            {evt.substrate_voice ? (
              <p className="event-substrate-voice">↑ {evt.substrate_voice}</p>
            ) : null}
            {evt.links && evt.links.length > 0 ? (
              <div className="event-links">
                <span className="event-links-label">Linked:</span>
                {evt.links.map((l, i) => (
                  <span key={l.id}>
                    <a className="event-link" data-target={l.id} href={`#${l.id}`} onClick={(e) => e.preventDefault()}>
                      {l.label ?? l.id}
                    </a>
                    {i < evt.links!.length - 1 ? ", " : ""}
                  </span>
                ))}
              </div>
            ) : null}
          </>
        )}
      </div>
    </article>
  );
}

// — bucketing logic —

function bucketEvents(events: HistoryEvent[]) {
  type Bucket = { id: string; title: string; events: HistoryEvent[] };
  const buckets: Bucket[] = [];
  const order = ["today", "yesterday", "this-week", "last-week"];
  const monthBuckets = new Map<string, HistoryEvent[]>();

  for (const evt of events) {
    const id = bucketIdFor(evt.timestamp);
    if (order.includes(id)) {
      let bucket = buckets.find((b) => b.id === id);
      if (!bucket) {
        bucket = { id, title: bucketTitleFor(id), events: [] };
        buckets.push(bucket);
      }
      bucket.events.push(evt);
    } else {
      const arr = monthBuckets.get(id) ?? [];
      arr.push(evt);
      monthBuckets.set(id, arr);
    }
  }

  // sort fixed buckets by recency
  buckets.sort((a, b) => order.indexOf(a.id) - order.indexOf(b.id));

  // append month/quarter buckets newest first
  const monthIds = [...monthBuckets.keys()].sort((a, b) => (a < b ? 1 : -1));
  for (const id of monthIds) {
    buckets.push({
      id,
      title: bucketTitleFor(id),
      events: monthBuckets.get(id)!.sort((a, b) =>
        a.timestamp < b.timestamp ? 1 : -1
      ),
    });
  }

  for (const b of buckets) {
    b.events.sort((a, b) => (a.timestamp < b.timestamp ? 1 : -1));
  }

  return buckets;
}

function bucketIdFor(iso: string): string {
  const t = new Date(iso);
  const days = Math.floor((NOW.getTime() - t.getTime()) / DAY);
  if (days <= 0) return "today";
  if (days === 1) return "yesterday";
  if (days <= 7) return "this-week";
  if (days <= 14) return "last-week";
  if (days <= 90) {
    return `month-${t.getUTCFullYear()}-${String(t.getUTCMonth() + 1).padStart(2, "0")}`;
  }
  if (days <= 365) {
    return `month-${t.getUTCFullYear()}-${String(t.getUTCMonth() + 1).padStart(2, "0")}`;
  }
  const quarter = Math.floor(t.getUTCMonth() / 3) + 1;
  return `q${quarter}-${t.getUTCFullYear()}`;
}

function bucketTitleFor(id: string): string {
  if (id === "today") {
    const d = NOW;
    return `TODAY · ${d.toLocaleDateString("en-US", { month: "long", day: "numeric" }).toUpperCase()}`;
  }
  if (id === "yesterday") {
    const d = new Date(NOW.getTime() - DAY);
    return `YESTERDAY · ${d
      .toLocaleDateString("en-US", { month: "long", day: "numeric" })
      .toUpperCase()}`;
  }
  if (id === "this-week") {
    const start = new Date(NOW.getTime() - 7 * DAY);
    return `THIS WEEK · ${monthDay(start)}–${monthDay(NOW)}`;
  }
  if (id === "last-week") {
    const start = new Date(NOW.getTime() - 14 * DAY);
    const end = new Date(NOW.getTime() - 8 * DAY);
    return `LAST WEEK · ${monthDay(start)}–${monthDay(end)}`;
  }
  if (id.startsWith("month-")) {
    const [, year, month] = id.split("-");
    const d = new Date(Number(year), Number(month) - 1, 1);
    const sameYear = d.getUTCFullYear() === NOW.getUTCFullYear();
    return sameYear
      ? d.toLocaleDateString("en-US", { month: "long" }).toUpperCase()
      : `${d.toLocaleDateString("en-US", { month: "long" }).toUpperCase()} ${d.getUTCFullYear()}`;
  }
  if (id.startsWith("q")) {
    const [q, year] = id.replace("q", "").split("-");
    return `Q${q} ${year}`;
  }
  return id.toUpperCase();
}

function monthDay(d: Date): string {
  return d
    .toLocaleDateString("en-US", { month: "short", day: "numeric" })
    .toUpperCase();
}

// — filter logic —

function filterEvents(
  events: HistoryEvent[],
  filters: HistoryFilters
): HistoryEvent[] {
  const cutoff = periodCutoff(filters.period);
  return events.filter((e) => {
    if (filters.arcId && e.arc !== filters.arcId) return false;
    const t = new Date(e.timestamp).getTime();
    if (cutoff && t < cutoff) return false;
    if (filters.significance === "major" && e.prominence !== "major")
      return false;
    if (
      filters.significance === "major-standard" &&
      e.prominence === "minor"
    )
      return false;
    if (filters.types.size > 0 && !filters.types.has(e.type)) return false;
    if (filters.search.trim()) {
      const q = filters.search.toLowerCase();
      const haystack = [
        e.title,
        e.descriptor,
        e.substrate_voice ?? "",
      ]
        .join(" ")
        .toLowerCase();
      if (!haystack.includes(q)) return false;
    }
    return true;
  });
}

function periodCutoff(period: HistoryFilters["period"]): number | null {
  const now = NOW.getTime();
  if (period === "7d") return now - 7 * DAY;
  if (period === "30d") return now - 30 * DAY;
  if (period === "90d") return now - 90 * DAY;
  if (period === "365d") return now - 365 * DAY;
  return null;
}

// suppress unused-warning in some configs
useEffect;
