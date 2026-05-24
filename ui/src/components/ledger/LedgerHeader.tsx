import { forwardRef, useEffect, useState } from "react";
import type { ChangeEvent } from "react";
import type { LedgerEventType } from "@/api/history-types";
import { EVENT_TYPE_META } from "./event-taxonomy";

export interface LedgerHeaderProps {
  dateRangeLabel: string;
  searchValue: string;
  onSearchChange: (next: string) => void;
  activeFilters: LedgerEventType[];
  onFiltersChange: (next: LedgerEventType[]) => void;
  searchInputRef?: React.RefObject<HTMLInputElement>;
}

export const LedgerHeader = forwardRef<HTMLInputElement, LedgerHeaderProps>(
  function LedgerHeader(
    {
      dateRangeLabel,
      searchValue,
      onSearchChange,
      activeFilters,
      onFiltersChange,
      searchInputRef,
    },
    _ref
  ) {
    const [filtersOpen, setFiltersOpen] = useState(false);

    useEffect(() => {
      function onKey(e: KeyboardEvent) {
        if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
          e.preventDefault();
          searchInputRef?.current?.focus();
        }
      }
      window.addEventListener("keydown", onKey);
      return () => window.removeEventListener("keydown", onKey);
    }, [searchInputRef]);

    function toggleType(type: LedgerEventType) {
      const has = activeFilters.includes(type);
      const next = has
        ? activeFilters.filter((t) => t !== type)
        : [...activeFilters, type];
      onFiltersChange(next);
    }

    return (
      <header className="fy-ledger__header">
        <div className="fy-ledger__header-text">
          <h1 className="fy-ledger__title">Ledger</h1>
          <p className="fy-ledger__subtitle">
            The history of what changed, what was predicted, and how it
            resolved.
          </p>
        </div>
        <div className="fy-ledger__header-controls">
          <span className="fy-ledger__date-range">{dateRangeLabel}</span>
          <div className="fy-ledger__filters">
            <button
              type="button"
              className="fy-ledger__filter-btn"
              aria-expanded={filtersOpen}
              aria-haspopup="listbox"
              onClick={() => setFiltersOpen((v) => !v)}
              data-testid="ledger-filters-toggle"
            >
              Filters
              <svg
                width="12"
                height="12"
                viewBox="0 0 12 12"
                aria-hidden="true"
              >
                <path
                  d="M2.5 4.5 6 8l3.5-3.5"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.4"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
              {activeFilters.length > 0 ? (
                <span className="fy-ledger__filter-count">
                  {activeFilters.length}
                </span>
              ) : null}
            </button>
            {filtersOpen ? (
              <div
                className="fy-ledger__filter-menu"
                role="listbox"
                aria-label="Filter event types"
              >
                {(Object.keys(EVENT_TYPE_META) as LedgerEventType[]).map(
                  (type) => {
                    const meta = EVENT_TYPE_META[type];
                    const checked = activeFilters.includes(type);
                    return (
                      <label
                        key={type}
                        className="fy-ledger__filter-option"
                      >
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => toggleType(type)}
                        />
                        <span
                          className="fy-ledger__filter-dot"
                          style={{ background: meta.cssVar }}
                          aria-hidden="true"
                        />
                        <span>{meta.shortLabel}</span>
                      </label>
                    );
                  }
                )}
                <div className="fy-ledger__filter-actions">
                  <button
                    type="button"
                    className="fy-ledger__filter-clear"
                    onClick={() => onFiltersChange([])}
                  >
                    Clear
                  </button>
                  <button
                    type="button"
                    className="fy-ledger__filter-apply"
                    onClick={() => setFiltersOpen(false)}
                  >
                    Done
                  </button>
                </div>
              </div>
            ) : null}
          </div>
          <div className="fy-ledger__search">
            <svg
              width="14"
              height="14"
              viewBox="0 0 14 14"
              aria-hidden="true"
              className="fy-ledger__search-icon"
            >
              <circle
                cx="6"
                cy="6"
                r="4"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.4"
              />
              <path
                d="M9.4 9.4 12 12"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.4"
                strokeLinecap="round"
              />
            </svg>
            <input
              ref={searchInputRef}
              type="search"
              className="fy-ledger__search-input"
              placeholder="Search ledger..."
              aria-label="Search ledger"
              value={searchValue}
              data-testid="ledger-search-input"
              onChange={(e: ChangeEvent<HTMLInputElement>) =>
                onSearchChange(e.target.value)
              }
            />
            <kbd className="fy-ledger__kbd" aria-hidden="true">
              ⌘K
            </kbd>
          </div>
        </div>
      </header>
    );
  }
);
