import type { MindFilters } from "./types";

// Spec Part 13.2 — Filter dropdown.
type Props = {
  filters: MindFilters;
  people: string[];
  onChange: (filters: MindFilters) => void;
  onClose: () => void;
  onReset: () => void;
};

export function FilterPanel({
  filters,
  people,
  onChange,
  onClose,
  onReset,
}: Props) {
  function toggleCategory(c: "loop" | "note" | "reminder") {
    const next = new Set(filters.categories);
    if (next.has(c)) next.delete(c);
    else next.add(c);
    onChange({ ...filters, categories: next });
  }

  return (
    <div className="filter-panel" role="dialog" aria-label="Filter items">
      <div className="filter-section">
        <span className="filter-section-label">Categories</span>
        <div className="filter-checkbox-group">
          <label>
            <input
              type="checkbox"
              checked={filters.categories.has("loop")}
              onChange={() => toggleCategory("loop")}
            />{" "}
            Loops
          </label>
          <label>
            <input
              type="checkbox"
              checked={filters.categories.has("note")}
              onChange={() => toggleCategory("note")}
            />{" "}
            Notes
          </label>
          <label>
            <input
              type="checkbox"
              checked={filters.categories.has("reminder")}
              onChange={() => toggleCategory("reminder")}
            />{" "}
            Reminders
          </label>
        </div>
      </div>

      <hr className="filter-divider" />

      <div className="filter-section">
        <span className="filter-section-label">Age</span>
        <div className="filter-radio-group">
          <label>
            <input
              type="radio"
              name="age"
              checked={filters.age === "all"}
              onChange={() => onChange({ ...filters, age: "all" })}
            />{" "}
            All
          </label>
          <label>
            <input
              type="radio"
              name="age"
              checked={filters.age === "aging"}
              onChange={() => onChange({ ...filters, age: "aging" })}
            />{" "}
            Aging only
          </label>
          <label>
            <input
              type="radio"
              name="age"
              checked={filters.age === "recent"}
              onChange={() => onChange({ ...filters, age: "recent" })}
            />{" "}
            Recent only
          </label>
        </div>
      </div>

      {people.length > 0 ? (
        <>
          <hr className="filter-divider" />
          <div className="filter-section">
            <span className="filter-section-label">Person</span>
            <select
              value={filters.person ?? ""}
              onChange={(e) =>
                onChange({ ...filters, person: e.target.value || null })
              }
            >
              <option value="">All</option>
              {people.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </div>
        </>
      ) : null}

      <div className="filter-actions">
        <button type="button" className="item-action" onClick={onReset}>
          Reset
        </button>
        <button type="button" className="item-action primary" onClick={onClose}>
          Apply
        </button>
      </div>
    </div>
  );
}
