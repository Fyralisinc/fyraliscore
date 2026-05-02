import { useEffect, useMemo, useState } from "react";
import type { Arc, HistoryEvent } from "./types";

// Spec Part 9 — Arcs layer: two-pane (list + detail).

type Props = {
  arcs: Arc[];
  events: HistoryEvent[];
  selectedArcId: string | null;
  onSelect: (id: string) => void;
  onEventClick: (id: string) => void;
};

export function Arcs({
  arcs,
  events,
  selectedArcId,
  onSelect,
  onEventClick,
}: Props) {
  const [search, setSearch] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "open" | "resolved">(
    "all"
  );

  const filtered = useMemo(() => {
    let out = arcs.slice();
    if (statusFilter !== "all") out = out.filter((a) => a.status === statusFilter);
    if (search.trim()) {
      const q = search.toLowerCase();
      out = out.filter((a) => a.name.toLowerCase().includes(q));
    }
    return out;
  }, [arcs, statusFilter, search]);

  const open = filtered.filter((a) => a.status === "open");
  const resolved = filtered.filter((a) => a.status === "resolved");

  const activeArc = useMemo(() => {
    if (selectedArcId)
      return arcs.find((a) => a.id === selectedArcId) ?? null;
    return arcs[0] ?? null;
  }, [arcs, selectedArcId]);

  // auto-select first arc on mount if none selected
  useEffect(() => {
    if (!selectedArcId && arcs.length > 0) onSelect(arcs[0].id);
  }, [selectedArcId, arcs, onSelect]);

  if (arcs.length === 0) {
    return (
      <div className="arcs-layer">
        <div className="empty-state-overlay">
          <p className="empty-state-text">
            No narrative arcs yet. Arcs form when multiple related events tell
            a single story over days or weeks.
          </p>
          <p className="empty-state-attribution">— Driftwood</p>
        </div>
      </div>
    );
  }

  return (
    <div className="arcs-layer">
      <aside className="arcs-list">
        <div className="arcs-controls">
          <select
            className="filter-select"
            value={statusFilter}
            onChange={(e) =>
              setStatusFilter(e.target.value as typeof statusFilter)
            }
            aria-label="Filter arcs by status"
          >
            <option value="all">All</option>
            <option value="open">Open only</option>
            <option value="resolved">Resolved only</option>
          </select>
          <input
            type="search"
            className="search-input"
            placeholder="Search arcs…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>

        {open.length > 0 ? (
          <>
            <h3 className="arcs-list-section">Active arcs</h3>
            <ul className="arcs-list-items">
              {open.map((a) => (
                <ArcListItem
                  key={a.id}
                  arc={a}
                  active={activeArc?.id === a.id}
                  onClick={() => onSelect(a.id)}
                />
              ))}
            </ul>
          </>
        ) : null}
        {resolved.length > 0 ? (
          <>
            <h3 className="arcs-list-section">Resolved arcs</h3>
            <ul className="arcs-list-items">
              {resolved.map((a) => (
                <ArcListItem
                  key={a.id}
                  arc={a}
                  active={activeArc?.id === a.id}
                  onClick={() => onSelect(a.id)}
                />
              ))}
            </ul>
          </>
        ) : null}
      </aside>

      <section className="arcs-detail">
        {activeArc ? (
          <ArcDetail arc={activeArc} events={events} onEventClick={onEventClick} />
        ) : null}
      </section>
    </div>
  );
}

function ArcListItem({
  arc,
  active,
  onClick,
}: {
  arc: Arc;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <li
      className={"arc-item" + (active ? " active" : "")}
      data-arc={arc.id}
      onClick={onClick}
    >
      <span className="arc-marker" aria-hidden="true" />
      <div className="arc-item-content">
        <span className="arc-name">{arc.name}</span>
        <span className="arc-period">
          {formatRange(arc.started, arc.ended)}
        </span>
        <span className={"arc-status " + arc.status}>{arc.status}</span>
      </div>
    </li>
  );
}

function ArcDetail({
  arc,
  events,
  onEventClick,
}: {
  arc: Arc;
  events: HistoryEvent[];
  onEventClick: (id: string) => void;
}) {
  const arcEvents = arc.events
    .map((id) => events.find((e) => e.id === id))
    .filter((e): e is HistoryEvent => !!e)
    .sort((a, b) => (a.timestamp < b.timestamp ? 1 : -1));

  return (
    <section className="arc-detail">
      <header className="arc-detail-header">
        <h2 className="arc-detail-name">{arc.name}</h2>
        <div className="arc-detail-meta">
          <span className="arc-detail-period">
            {formatRange(arc.started, arc.ended)}
          </span>
          <span className="arc-detail-separator">·</span>
          <span className={"arc-detail-status " + arc.status}>{arc.status}</span>
        </div>
      </header>

      <hr className="arc-detail-divider" />

      <section className="arc-narrative">
        <span className="arc-section-label">Arc narrative</span>
        <p className="arc-narrative-text">{arc.narrative}</p>
      </section>

      {arcEvents.length > 0 ? (
        <>
          <hr className="arc-detail-divider" />
          <section className="arc-events">
            <span className="arc-section-label">Events in this arc</span>
            <div className="arc-events-timeline">
              {arcEvents.map((e) => (
                <article
                  key={e.id}
                  className="event"
                  data-prominence={e.prominence}
                  role="article"
                  onClick={() => onEventClick(e.id)}
                >
                  <span className="event-marker" />
                  <div className="event-content">
                    {e.prominence === "minor" ? (
                      <>
                        <span className="event-time">
                          {formatShortDate(e.timestamp)}
                        </span>
                        <span className="event-descriptor">{e.descriptor}</span>
                      </>
                    ) : (
                      <>
                        <header className="event-header">
                          <span className="event-title">{e.title}</span>
                          <span className="event-time">
                            {formatShortDate(e.timestamp)}
                          </span>
                        </header>
                        <p className="event-descriptor">{e.descriptor}</p>
                      </>
                    )}
                  </div>
                </article>
              ))}
            </div>
          </section>
        </>
      ) : null}
    </section>
  );
}

function formatRange(start: string, end?: string): string {
  const s = new Date(start);
  const sLabel = s.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  if (!end) return `${sLabel} → present`;
  const e = new Date(end);
  const eLabel = e.toLocaleDateString("en-US", { month: "short", day: "numeric" });
  return `${sLabel} → ${eLabel}`;
}

function formatShortDate(iso: string): string {
  return new Date(iso)
    .toLocaleDateString("en-US", { month: "short", day: "numeric" })
    .toLowerCase();
}
