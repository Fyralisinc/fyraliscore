import { useEffect, useRef, useState } from "react";
import type {
  CommitmentStatus,
  EntityKind,
  Filters,
  TimeWindow,
} from "./types";

// Sole control surface for the relational view: a Filter button.
// View-mode and color-by toggles were removed when the relational view
// became the only mode. The filter panel now also drives an entity-kind
// toggle so the left list can show goals only, commitments only, or both.
type Props = {
  filters: Filters;
  ownerOptions: { id: string; label: string }[];
  customerOptions: { id: string; label: string }[];
  onFiltersChange: (f: Filters) => void;
};

const STATUS_LABEL: Record<CommitmentStatus, string> = {
  "on-track": "On track",
  slipping: "Slipping",
  "at-risk": "At risk",
  blocked: "Blocked",
};

const ENTITY_LABEL: Record<EntityKind, string> = {
  all: "All",
  goals: "Goals only",
  commitments: "Commitments only",
  people: "Team only",
};

export function MapControls({
  filters,
  ownerOptions,
  customerOptions,
  onFiltersChange,
}: Props) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function onDoc(e: MouseEvent) {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target as Node)) setOpen(false);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, []);

  const filterActive =
    filters.entityKind !== "all" ||
    filters.time !== "quarter" ||
    filters.statuses.size !== 4 ||
    filters.owner !== null ||
    filters.customer !== null;

  function toggleStatus(s: CommitmentStatus) {
    const next = new Set(filters.statuses);
    if (next.has(s)) next.delete(s);
    else next.add(s);
    onFiltersChange({ ...filters, statuses: next });
  }

  function resetFilters() {
    onFiltersChange({
      entityKind: "all",
      time: "quarter",
      statuses: new Set<CommitmentStatus>([
        "on-track", "slipping", "at-risk", "blocked",
      ]),
      owner: null,
      customer: null,
    });
  }

  return (
    <div className="map-controls" ref={wrapRef}>
      <div className="control-wrap">
        <button
          type="button"
          className="control-toggle"
          data-active={filterActive ? "true" : "false"}
          aria-haspopup="dialog"
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
        >
          Filter
          <span className="chevron" aria-hidden="true">▾</span>
        </button>
        {open ? (
          <div className="filter-panel" role="dialog" aria-label="Filter">
            <div className="filter-section">
              <span className="filter-section-label">Show</span>
              <div className="filter-radio-group">
                {(Object.keys(ENTITY_LABEL) as EntityKind[]).map((k) => (
                  <label key={k}>
                    <input
                      type="radio"
                      name="entity-kind"
                      checked={filters.entityKind === k}
                      onChange={() => onFiltersChange({ ...filters, entityKind: k })}
                    />
                    {ENTITY_LABEL[k]}
                  </label>
                ))}
              </div>
            </div>
            <hr className="filter-divider" />
            <div className="filter-section">
              <span className="filter-section-label">Time window</span>
              <div className="filter-radio-group">
                {([
                  ["next-7", "Next 7 days"],
                  ["quarter", "This quarter"],
                  ["all", "All"],
                ] as [TimeWindow, string][]).map(([v, l]) => (
                  <label key={v}>
                    <input
                      type="radio"
                      name="time"
                      checked={filters.time === v}
                      onChange={() => onFiltersChange({ ...filters, time: v })}
                    />
                    {l}
                  </label>
                ))}
              </div>
            </div>
            <hr className="filter-divider" />
            <div className="filter-section">
              <span className="filter-section-label">Status</span>
              <div className="filter-checkbox-group">
                {(Object.keys(STATUS_LABEL) as CommitmentStatus[]).map((s) => (
                  <label key={s}>
                    <input
                      type="checkbox"
                      checked={filters.statuses.has(s)}
                      onChange={() => toggleStatus(s)}
                    />
                    {STATUS_LABEL[s]}
                  </label>
                ))}
              </div>
            </div>
            <hr className="filter-divider" />
            <div className="filter-section">
              <span className="filter-section-label">Owner</span>
              <select
                className="filter-select"
                value={filters.owner ?? ""}
                onChange={(e) =>
                  onFiltersChange({
                    ...filters,
                    owner: e.target.value || null,
                  })
                }
              >
                <option value="">All</option>
                {ownerOptions.map((o) => (
                  <option key={o.id} value={o.id}>
                    {o.label}
                  </option>
                ))}
              </select>
            </div>
            <div className="filter-section">
              <span className="filter-section-label">Customer</span>
              <select
                className="filter-select"
                value={filters.customer ?? ""}
                onChange={(e) =>
                  onFiltersChange({
                    ...filters,
                    customer: e.target.value || null,
                  })
                }
              >
                <option value="">All</option>
                {customerOptions.map((c) => (
                  <option key={c.id} value={c.id}>
                    {c.label}
                  </option>
                ))}
              </select>
            </div>
            <div className="filter-actions">
              <button type="button" className="btn-text" onClick={resetFilters}>
                Reset
              </button>
              <button
                type="button"
                className="btn-primary"
                onClick={() => setOpen(false)}
              >
                Apply
              </button>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
