import { useCallback, useEffect, useState } from "react";
import type { NavSection } from "@/api/today-types";

type Props = {
  brand: { name: string; mark: string; pulse_day: number };
  nav: NavSection[];
  /** Called when the user clicks the brand mark or wordmark.
   *  Pages use this to reset to their default view. */
  onBrandClick?: () => void;
  onNavigate?: (sectionId: string, itemId: string) => void;
};

// Sidebar — brand zone + page links only. Vitals, watching widgets,
// and other dashboard-y noise have been cut. Carries a single
// expand/collapse toggle (persisted via localStorage). When collapsed,
// only the brand zone is visible; when expanded, the nav links appear
// below it.
export function Sidebar({ brand, nav, onBrandClick, onNavigate }: Props) {
  const [collapsed, setCollapsed] = useState<boolean>(() => {
    try {
      return localStorage.getItem("sidebarCollapsed") === "1";
    } catch {
      return false;
    }
  });
  const toggle = useCallback(() => {
    setCollapsed((v) => {
      const next = !v;
      try {
        localStorage.setItem("sidebarCollapsed", next ? "1" : "0");
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  // Reflect collapsed state on the body so the cockpit grid can
  // narrow the sidebar track via [data-sidebar-collapsed].
  useEffect(() => {
    document.body.dataset.sidebarCollapsed = collapsed ? "true" : "false";
    return () => {
      delete document.body.dataset.sidebarCollapsed;
    };
  }, [collapsed]);

  return (
    <aside
      className={"sidebar" + (collapsed ? " collapsed" : "")}
      aria-label="Navigation"
    >
      <div className="sidebar-brand">
        <button
          className="brand-mark"
          onClick={onBrandClick}
          aria-label="Home"
          type="button"
        >
          {brand.mark}
        </button>
        {!collapsed ? (
          <button
            className="brand-wordmark"
            onClick={onBrandClick}
            type="button"
            title="Reset view"
          >
            {brand.name}
          </button>
        ) : null}
        <button
          className="sidebar-toggle"
          onClick={toggle}
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          aria-pressed={collapsed}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          type="button"
        >
          {collapsed ? "›" : "‹"}
        </button>
      </div>

      {!collapsed
        ? nav.map((section) => (
            <div className="nav-section" key={section.id}>
              <div className="nav-section-label">{section.label}</div>
              {section.items.map((item) => (
                <button
                  key={item.id}
                  className={
                    "nav-item" +
                    (item.active ? " active" : "") +
                    (item.disabled ? " disabled" : "")
                  }
                  disabled={item.disabled}
                  onClick={() => onNavigate?.(section.id, item.id)}
                  type="button"
                >
                  <span className="nav-icon" aria-hidden="true">
                    <NavGlyph active={item.active} />
                  </span>
                  <span>{item.label}</span>
                  {item.badge ? (
                    <span
                      className={
                        "nav-badge" +
                        (item.badge_warn ? " warn" : "") +
                        (item.badge === "soon" ? " soon" : "")
                      }
                    >
                      {item.badge}
                    </span>
                  ) : item.shortcut ? (
                    <span className="nav-badge">{item.shortcut}</span>
                  ) : null}
                </button>
              ))}
            </div>
          ))
        : null}
    </aside>
  );
}

function NavGlyph({ active }: { active?: boolean }) {
  return (
    <svg
      width="15"
      height="15"
      viewBox="0 0 15 15"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
    >
      {active ? (
        <path d="M3 7.5 L7 3.5 L11.5 7.5 L7 11.5 Z" fill="currentColor" />
      ) : (
        <path d="M3 7.5 L7 3.5 L11.5 7.5 L7 11.5 Z" />
      )}
    </svg>
  );
}
