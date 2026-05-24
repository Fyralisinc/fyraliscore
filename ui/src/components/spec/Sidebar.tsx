import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";

import { useFyralisStore } from "@/lib/store";

export type SpecRoute =
  | "today"
  | "model"
  | "forecasts"
  | "ledger"
  | "ask"
  | "sources"
  | "settings";

interface SpecSidebarProps {
  active: SpecRoute;
}

interface Item {
  route: SpecRoute;
  label: string;
  to: string;
  icon: ReactNode;
}

const PRIMARY: Item[] = [
  { route: "today", label: "Today", to: "/", icon: <IconToday /> },
  { route: "model", label: "Model", to: "/model", icon: <IconModel /> },
  { route: "forecasts", label: "Forecasts", to: "/forecasts", icon: <IconForecast /> },
  { route: "ledger", label: "Ledger", to: "/ledger", icon: <IconLedger /> },
];

const UTILITY: Item[] = [
  { route: "ask", label: "Ask Fyralis", to: "#ask", icon: <IconAsk /> },
  { route: "sources", label: "Sources", to: "/sources", icon: <IconSources /> },
  { route: "settings", label: "Settings", to: "/settings", icon: <IconSettings /> },
];

// Per spec §2.1 + §3.4: primary nav up top; utility nav below; live
// status + user card at the bottom; subtle forest decoration concentrated
// in the lower half of the sidebar.
export function SpecSidebar({ active }: SpecSidebarProps) {
  const setPaletteOpen = useFyralisStore((s) => s.setPaletteOpen);

  return (
    <nav className="fx-sidebar" aria-label="Primary">
      <div className="fx-sidebar__forest" aria-hidden="true" />

      <div className="fx-sidebar__brand">
        <div className="fx-sidebar__logomark" aria-hidden="true">F</div>
        <div className="fx-sidebar__wordmark">Fyralis</div>
      </div>

      <div className="fx-sidebar__group">
        {PRIMARY.map((it) => (
          <NavLink
            key={it.route}
            to={it.to}
            end={it.to === "/"}
            className={({ isActive }) =>
              `fx-sidebar__nav${(isActive || active === it.route) ? " fx-sidebar__nav--active" : ""}`
            }
          >
            <span className="fx-sidebar__nav-icon" aria-hidden="true">{it.icon}</span>
            {it.label}
          </NavLink>
        ))}
      </div>

      <div className="fx-sidebar__group-label">Utilities</div>
      <div className="fx-sidebar__group">
        {UTILITY.map((it) => {
          if (it.route === "ask") {
            return (
              <button
                key={it.route}
                type="button"
                className={`fx-sidebar__nav${active === it.route ? " fx-sidebar__nav--active" : ""}`}
                onClick={() => setPaletteOpen(true)}
              >
                <span className="fx-sidebar__nav-icon" aria-hidden="true">{it.icon}</span>
                {it.label}
                <span style={{ marginLeft: "auto", fontSize: 11, opacity: 0.5 }}>⌘K</span>
              </button>
            );
          }
          return (
            <NavLink
              key={it.route}
              to={it.to}
              className={({ isActive }) =>
                `fx-sidebar__nav${(isActive || active === it.route) ? " fx-sidebar__nav--active" : ""}`
              }
            >
              <span className="fx-sidebar__nav-icon" aria-hidden="true">{it.icon}</span>
              {it.label}
            </NavLink>
          );
        })}
      </div>

      <div className="fx-sidebar__spacer" />

      <div className="fx-sidebar__live">
        <div className="fx-sidebar__live-row">
          <span className="fx-sidebar__live-dot" aria-hidden="true" />
          Model live
        </div>
        <div className="fx-sidebar__live-detail">Last sync 38s ago</div>
      </div>

      <div className="fx-sidebar__user">
        <div className="fx-sidebar__user-avatar" aria-hidden="true">D</div>
        <div>
          <div className="fx-sidebar__user-name">Diana</div>
          <div className="fx-sidebar__user-role">CEO</div>
        </div>
      </div>
    </nav>
  );
}

function IconToday() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <rect x="2.5" y="3.5" width="11" height="10" rx="1.5" />
      <path d="M2.5 6.5h11M5 2.5v2M11 2.5v2" />
    </svg>
  );
}
function IconModel() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <circle cx="4" cy="4" r="1.6" />
      <circle cx="12" cy="4" r="1.6" />
      <circle cx="8" cy="12" r="1.6" />
      <path d="M5.4 4.6 10.6 4.6M4.6 5.4 7.2 10.6M11.4 5.4 8.8 10.6" />
    </svg>
  );
}
function IconForecast() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <path d="M2 12 L5.5 8 L8.5 10 L13.5 4" />
      <path d="M10 4h3.5v3.5" />
    </svg>
  );
}
function IconLedger() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <rect x="3" y="2.5" width="10" height="11" rx="1.4" />
      <path d="M5.5 5.5h5M5.5 8h5M5.5 10.5h3" />
    </svg>
  );
}
function IconAsk() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <circle cx="8" cy="8" r="5.5" />
      <path d="M6.3 6.5a1.7 1.7 0 1 1 2.3 1.6c-.4.2-.6.5-.6.9V10" />
      <circle cx="8" cy="11.6" r="0.5" fill="currentColor" stroke="none" />
    </svg>
  );
}
function IconSources() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <ellipse cx="8" cy="4" rx="5" ry="1.8" />
      <path d="M3 4v8c0 1 2.2 1.8 5 1.8s5-.8 5-1.8V4M3 8c0 1 2.2 1.8 5 1.8s5-.8 5-1.8" />
    </svg>
  );
}
function IconSettings() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <circle cx="8" cy="8" r="2" />
      <path d="M8 1.5v2M8 12.5v2M14.5 8h-2M3.5 8h-2M12.6 3.4l-1.4 1.4M4.8 11.2l-1.4 1.4M12.6 12.6l-1.4-1.4M4.8 4.8 3.4 3.4" />
    </svg>
  );
}
