import type { ReactNode } from "react";
import { NavLink } from "react-router-dom";

export type ActiveRoute =
  | "today"
  | "model"
  | "forecasts"
  | "ledger"
  | "commitments"
  | "customers"
  | "risks"
  | "decisions"
  | "owners"
  | "teams"
  | "ask"
  | "sources"
  | "settings";

export interface SidebarProps {
  activeRoute?: ActiveRoute;
}

interface NavItem {
  route: ActiveRoute;
  label: string;
  to: string;
  icon: ReactNode;
}

const primaryNav: NavItem[] = [
  { route: "today", label: "Today", to: "/today", icon: <IconToday /> },
  { route: "model", label: "Model", to: "/model", icon: <IconModel /> },
  { route: "forecasts", label: "Forecasts", to: "/forecasts", icon: <IconForecasts /> },
  { route: "ledger", label: "Ledger", to: "/ledger", icon: <IconLedger /> },
];

const utilityNav: NavItem[] = [
  { route: "ask", label: "Ask Fyralis", to: "/ask", icon: <IconAsk /> },
  { route: "sources", label: "Sources", to: "/sources", icon: <IconSources /> },
  { route: "settings", label: "Settings", to: "/settings", icon: <IconSettings /> },
];

export function Sidebar({ activeRoute = "today" }: SidebarProps) {
  return (
    <nav className="fy-sidebar">
      <ForestDecoration />

      <div className="fy-sidebar__brand">
        <div className="fy-sidebar__logomark" aria-hidden="true">F</div>
        <span className="fy-sidebar__wordmark">Fyralis</span>
      </div>

      <div className="fy-sidebar__group" role="group" aria-label="Primary">
        {primaryNav.map((item) => (
          <SidebarLink key={item.route} item={item} active={activeRoute === item.route} />
        ))}
      </div>

      <div className="fy-sidebar__spacer" />

      <div className="fy-sidebar__group-label">Utilities</div>
      <div className="fy-sidebar__group" role="group" aria-label="Utilities">
        {utilityNav.map((item) => (
          <SidebarLink
            key={item.route}
            item={item}
            active={activeRoute === item.route}
            variant="secondary"
          />
        ))}
      </div>

      <ModelHealthCard />
      <UserCard />
    </nav>
  );
}

interface SidebarLinkProps {
  item: NavItem;
  active: boolean;
  variant?: "primary" | "secondary";
}

function SidebarLink({ item, active, variant = "primary" }: SidebarLinkProps) {
  const className = [
    "fy-sidebar__nav-item",
    active ? "fy-sidebar__nav-item--active" : "",
    variant === "secondary" ? "fy-sidebar__nav-item--secondary" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <NavLink to={item.to} className={className} end={item.to === "/today"}>
      <span className="fy-sidebar__nav-icon" aria-hidden="true">
        {item.icon}
      </span>
      <span>{item.label}</span>
    </NavLink>
  );
}

function ModelHealthCard() {
  return (
    <div className="fy-sidebar__health" aria-label="Model health">
      <div className="fy-sidebar__health-row">
        <span className="fy-sidebar__health-dot" aria-hidden="true" />
        <span className="fy-sidebar__health-label">Live</span>
      </div>
      <div className="fy-sidebar__health-status">All systems normal</div>
      <svg
        className="fy-sidebar__health-spark"
        viewBox="0 0 120 18"
        preserveAspectRatio="none"
        aria-hidden="true"
      >
        <polyline
          fill="none"
          stroke="currentColor"
          strokeWidth="1.4"
          points="0,12 10,10 20,11 30,8 40,9 50,7 60,8 70,6 80,7 90,5 100,6 110,4 120,5"
        />
      </svg>
    </div>
  );
}

function UserCard() {
  return (
    <div className="fy-sidebar__user" aria-label="Current user">
      <div className="fy-sidebar__avatar" aria-hidden="true">D</div>
      <div>
        <div className="fy-sidebar__user-name">Diana</div>
        <div className="fy-sidebar__user-role">CEO</div>
      </div>
    </div>
  );
}

function ForestDecoration() {
  return (
    <>
      <div className="fy-sidebar__forest" aria-hidden="true" />
      <svg
        className="fy-sidebar__forest-svg"
        viewBox="0 0 264 220"
        preserveAspectRatio="xMidYMax slice"
        aria-hidden="true"
      >
        <defs>
          <linearGradient id="fyForestFade" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="#071713" stopOpacity="0.9" />
            <stop offset="100%" stopColor="#071713" stopOpacity="0" />
          </linearGradient>
        </defs>
        <g fill="#0F2A24" opacity="0.55">
          <polygon points="0,220 24,150 48,220" />
          <polygon points="30,220 56,120 84,220" />
          <polygon points="70,220 96,160 122,220" />
          <polygon points="110,220 140,100 174,220" />
          <polygon points="158,220 184,150 212,220" />
          <polygon points="196,220 224,130 252,220" />
          <polygon points="232,220 252,170 264,220" />
        </g>
        <g fill="#1B3A33" opacity="0.45">
          <polygon points="0,220 14,180 30,220" />
          <polygon points="42,220 64,170 88,220" />
          <polygon points="82,220 106,180 130,220" />
          <polygon points="128,220 150,170 178,220" />
          <polygon points="170,220 196,180 220,220" />
          <polygon points="210,220 232,170 254,220" />
        </g>
        <rect x="0" y="0" width="264" height="220" fill="url(#fyForestFade)" />
      </svg>
    </>
  );
}

function IconToday() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <rect x="2.5" y="3.5" width="11" height="10" rx="1.5" />
      <path d="M2.5 6.5h11" />
      <path d="M5 2.5v2M11 2.5v2" />
    </svg>
  );
}

function IconModel() {
  return (
    <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
      <circle cx="4" cy="4" r="1.8" />
      <circle cx="12" cy="4" r="1.8" />
      <circle cx="8" cy="12" r="1.8" />
      <path d="M5.5 4.7 10.5 4.7" />
      <path d="M4.6 5.4 7.2 10.6" />
      <path d="M11.4 5.4 8.8 10.6" />
    </svg>
  );
}

function IconForecasts() {
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
      <path d="M3 4v8c0 1 2.2 1.8 5 1.8s5-.8 5-1.8V4" />
      <path d="M3 8c0 1 2.2 1.8 5 1.8s5-.8 5-1.8" />
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

export default Sidebar;
