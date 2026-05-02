import type { NavSection, VitalRow } from "@/api/today-types";

type Props = {
  brand: { name: string; mark: string; pulse_day: number };
  nav: NavSection[];
  vitals: VitalRow[];
  onRename?: () => void;
  onNavigate?: (sectionId: string, itemId: string) => void;
};

// Per spec §3.2 — sticky 240px, brand zone + nav sections + vitals at bottom.
// Brand mark + wordmark are clickable to rename. Vitals row is what the
// substrate is actively watching (not generic dashboard metrics).
export function Sidebar({ brand, nav, vitals, onRename, onNavigate }: Props) {
  return (
    <aside className="sidebar" aria-label="Navigation">
      <div className="sidebar-brand">
        <button
          className="brand-mark"
          onClick={onRename}
          aria-label="Brand mark — click to rename"
          type="button"
        >
          {brand.mark}
        </button>
        <button
          className="brand-wordmark"
          onClick={onRename}
          type="button"
          title="Click to rename"
        >
          {brand.name}
        </button>
        <span
          className="brand-pulse"
          aria-hidden="true"
          title={`Perceiving · day ${brand.pulse_day}`}
        />
      </div>

      {nav.map((section) => (
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
      ))}

      {vitals.length > 0 ? (
        <div className="vitals">
          <div className="vitals-label">Watching</div>
          {vitals.map((v) => (
            <div className="vital-row" key={v.id}>
              <span className="vital-label">{v.label}</span>
              <span className={"vital-value" + (v.tone ? ` ${v.tone}` : "")}>
                {v.value}
              </span>
            </div>
          ))}
        </div>
      ) : null}
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
