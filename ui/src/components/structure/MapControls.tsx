import { useEffect, useRef, useState } from "react";
import type {
  ColorMode,
  CommitmentStatus,
  Filters,
  LayoutMode,
  TimeWindow,
} from "./types";

// Spec Part 6 — three controls (Layout / Color by / Filter), each a
// toggle button that opens a dropdown panel below it.
type Props = {
  layout: LayoutMode;
  color: ColorMode;
  filters: Filters;
  ownerOptions: { id: string; label: string }[];
  customerOptions: { id: string; label: string }[];
  onLayoutChange: (m: LayoutMode) => void;
  onColorChange: (c: ColorMode) => void;
  onFiltersChange: (f: Filters) => void;
};

const STATUS_LABEL: Record<CommitmentStatus, string> = {
  "on-track": "On track",
  slipping: "Slipping",
  "at-risk": "At risk",
  blocked: "Blocked",
};

export function MapControls({
  layout,
  color,
  filters,
  ownerOptions,
  customerOptions,
  onLayoutChange,
  onColorChange,
  onFiltersChange,
}: Props) {
  const [open, setOpen] = useState<null | "layout" | "color" | "filter">(null);
  const wrapRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    function onDoc(e: MouseEvent) {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target as Node)) setOpen(null);
    }
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") setOpen(null);
    }
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, []);

  const filterActive =
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
      time: "quarter",
      statuses: new Set<CommitmentStatus>([
        "on-track",
        "slipping",
        "at-risk",
        "blocked",
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
          aria-haspopup="menu"
          aria-expanded={open === "layout"}
          onClick={() => setOpen(open === "layout" ? null : "layout")}
        >
          Layout:{" "}
          <span className="control-value">
            {layout === "territory" ? "Territory" : "Two-axis"}
          </span>
          <span className="chevron" aria-hidden="true">▾</span>
        </button>
        {open === "layout" ? (
          <div className="control-menu" role="menu">
            <button
              type="button"
              className="menu-item"
              data-selected={layout === "territory"}
              onClick={() => {
                onLayoutChange("territory");
                setOpen(null);
              }}
            >
              <span className="menu-check">✓</span>Territory
            </button>
            <button
              type="button"
              className="menu-item"
              data-selected={layout === "two-axis"}
              onClick={() => {
                onLayoutChange("two-axis");
                setOpen(null);
              }}
            >
              <span className="menu-check">✓</span>Two-axis (priority × time)
            </button>
          </div>
        ) : null}
      </div>

      <div className="control-wrap">
        <button
          type="button"
          className="control-toggle"
          aria-haspopup="menu"
          aria-expanded={open === "color"}
          onClick={() => setOpen(open === "color" ? null : "color")}
        >
          Color by: <span className="control-value">{labelForColor(color)}</span>
          <span className="chevron" aria-hidden="true">▾</span>
        </button>
        {open === "color" ? (
          <div className="control-menu" role="menu">
            {(["status", "owner", "customer", "decision"] as ColorMode[]).map(
              (m) => (
                <button
                  key={m}
                  type="button"
                  className="menu-item"
                  data-selected={color === m}
                  onClick={() => {
                    onColorChange(m);
                    setOpen(null);
                  }}
                >
                  <span className="menu-check">✓</span>
                  {labelForColor(m)}
                </button>
              )
            )}
          </div>
        ) : null}
      </div>

      <div className="control-wrap">
        <button
          type="button"
          className="control-toggle"
          data-active={filterActive ? "true" : "false"}
          aria-haspopup="dialog"
          aria-expanded={open === "filter"}
          onClick={() => setOpen(open === "filter" ? null : "filter")}
        >
          Filter
          <span className="chevron" aria-hidden="true">▾</span>
        </button>
        {open === "filter" ? (
          <div className="filter-panel" role="dialog" aria-label="Filter map">
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
                onClick={() => setOpen(null)}
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

function labelForColor(m: ColorMode): string {
  if (m === "status") return "Status";
  if (m === "owner") return "Owner";
  if (m === "customer") return "Customer";
  return "Decision lineage";
}
