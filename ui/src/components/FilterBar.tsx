import { useEffect, useRef, useState } from "react";
import type { Severity } from "@/api/today-types";

// Multi-dimensional filter for the Today feed. Replaces the
// All / Operational / Strategic tab strip with a single Filter dropdown
// that exposes more useful axes: category, severity, owner, target
// kind, and a "new only" toggle.
export type TodayFilters = {
  category: "all" | "operational" | "strategic";
  severities: Set<Severity>;
  owners: Set<string>;
  targetKinds: Set<string>;
  newOnly: boolean;
};

export const DEFAULT_FILTERS: TodayFilters = {
  category: "all",
  severities: new Set(),
  owners: new Set(),
  targetKinds: new Set(),
  newOnly: false,
};

type Props = {
  filters: TodayFilters;
  onChange: (next: TodayFilters) => void;
  ownerOptions: string[];
  targetKindOptions: string[];
  visibleCount: number;
  totalCount: number;
  cleared: number;
};

const SEVERITY_LABEL: Record<Severity, string> = {
  critical: "Critical",
  strategic: "Strategic",
  high: "High",
  med: "Med",
  low: "Low",
};

const TARGET_KIND_LABEL: Record<string, string> = {
  commitment: "Commitment",
  goal: "Goal",
  decision: "Decision",
  resource: "Resource",
};

export function FilterBar({
  filters,
  onChange,
  ownerOptions,
  targetKindOptions,
  visibleCount,
  totalCount,
  cleared,
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

  const activeCount =
    (filters.category !== "all" ? 1 : 0) +
    filters.severities.size +
    filters.owners.size +
    filters.targetKinds.size +
    (filters.newOnly ? 1 : 0);

  function toggleSeverity(s: Severity) {
    const next = new Set(filters.severities);
    if (next.has(s)) next.delete(s);
    else next.add(s);
    onChange({ ...filters, severities: next });
  }
  function toggleOwner(o: string) {
    const next = new Set(filters.owners);
    if (next.has(o)) next.delete(o);
    else next.add(o);
    onChange({ ...filters, owners: next });
  }
  function toggleKind(k: string) {
    const next = new Set(filters.targetKinds);
    if (next.has(k)) next.delete(k);
    else next.add(k);
    onChange({ ...filters, targetKinds: next });
  }
  function reset() {
    onChange({
      category: "all",
      severities: new Set(),
      owners: new Set(),
      targetKinds: new Set(),
      newOnly: false,
    });
  }

  return (
    <div className="filter-bar">
      <div className="filter-bar-left" ref={wrapRef}>
        <button
          type="button"
          className="filter-bar-trigger"
          data-active={activeCount > 0 ? "true" : "false"}
          aria-haspopup="dialog"
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
        >
          <span className="filter-bar-trigger-icon" aria-hidden>⌃</span>
          Filter
          {activeCount > 0 ? (
            <span className="filter-bar-trigger-count">{activeCount}</span>
          ) : null}
          <span className="chevron" aria-hidden="true">▾</span>
        </button>
        {activeCount > 0 ? (
          <button
            type="button"
            className="filter-bar-reset"
            onClick={reset}
            title="Clear all filters"
          >
            Clear
          </button>
        ) : null}

        {open ? (
          <div className="filter-bar-panel" role="dialog" aria-label="Filter cards">
            <section className="fbp-section">
              <div className="fbp-section-label">Category</div>
              <div className="fbp-radio-row">
                {([
                  ["all", "All"],
                  ["operational", "Operational"],
                  ["strategic", "Strategic"],
                ] as const).map(([v, l]) => (
                  <label key={v} className="fbp-radio">
                    <input
                      type="radio"
                      name="cat"
                      checked={filters.category === v}
                      onChange={() => onChange({ ...filters, category: v })}
                    />
                    <span>{l}</span>
                  </label>
                ))}
              </div>
            </section>

            <section className="fbp-section">
              <div className="fbp-section-label">Severity</div>
              <div className="fbp-chip-row">
                {(Object.keys(SEVERITY_LABEL) as Severity[]).map((s) => (
                  <button
                    key={s}
                    type="button"
                    className={
                      "fbp-chip fbp-chip-sev s-" + s +
                      (filters.severities.has(s) ? " active" : "")
                    }
                    onClick={() => toggleSeverity(s)}
                  >
                    {SEVERITY_LABEL[s]}
                  </button>
                ))}
              </div>
            </section>

            <section className="fbp-section">
              <div className="fbp-section-label">Target type</div>
              <div className="fbp-chip-row">
                {targetKindOptions.length === 0 ? (
                  <span className="fbp-empty">no target types in current feed</span>
                ) : (
                  targetKindOptions.map((k) => (
                    <button
                      key={k}
                      type="button"
                      className={
                        "fbp-chip" + (filters.targetKinds.has(k) ? " active" : "")
                      }
                      onClick={() => toggleKind(k)}
                    >
                      {TARGET_KIND_LABEL[k] ?? k}
                    </button>
                  ))
                )}
              </div>
            </section>

            <section className="fbp-section">
              <div className="fbp-section-label">People</div>
              {ownerOptions.length === 0 ? (
                <span className="fbp-empty">no owners on visible cards</span>
              ) : (
                <div className="fbp-chip-row">
                  {ownerOptions.map((o) => (
                    <button
                      key={o}
                      type="button"
                      className={
                        "fbp-chip" + (filters.owners.has(o) ? " active" : "")
                      }
                      onClick={() => toggleOwner(o)}
                    >
                      {o}
                    </button>
                  ))}
                </div>
              )}
            </section>

            <section className="fbp-section">
              <label className="fbp-toggle">
                <input
                  type="checkbox"
                  checked={filters.newOnly}
                  onChange={() =>
                    onChange({ ...filters, newOnly: !filters.newOnly })
                  }
                />
                <span>New in last 24h only</span>
              </label>
            </section>

            <div className="fbp-actions">
              <button type="button" className="btn-text" onClick={reset}>
                Reset
              </button>
              <button
                type="button"
                className="btn-primary"
                onClick={() => setOpen(false)}
              >
                Done
              </button>
            </div>
          </div>
        ) : null}
      </div>

      <div className="filter-bar-state">
        <span>
          <b className="clear-count">{visibleCount}</b>
          {visibleCount !== totalCount ? (
            <span className="filter-bar-of"> of {totalCount}</span>
          ) : null}{" "}
          open
        </span>
        <span>·</span>
        <span>
          <b className="clear-count">{cleared}</b> cleared today
        </span>
      </div>
    </div>
  );
}
