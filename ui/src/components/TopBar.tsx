import type { Status } from "@/api/types";

type Props = { status?: Status };

// Fixed 44px top bar. Logo + view-label on the left; three live stats on
// the right (substrate, calibration, needs-you). Shape matches
// /company-os.html lines 829-842 and design doc §10.1.
export function TopBar({ status }: Props) {
  const alive = status?.substrate_alive ?? true;
  const calibration = status?.calibration_pct ?? 0;
  const needsYou = status?.needs_you_count ?? 0;
  const warm = needsYou > 0;
  return (
    <header className="top">
      <div className="left">
        <span className="logo">
          <span className="logo-mark" aria-hidden="true" />
          Company OS
        </span>
        <span className="view-label">
          view / <b>ceo · rachin</b>
        </span>
      </div>
      <div className="right">
        <span className="stat">
          <span className="pip" />
          substrate <b>{alive ? "live" : "stale"}</b>
        </span>
        <span className="stat">
          calibration <b>{calibration}%</b>
        </span>
        <span className="stat">
          <span className={warm ? "pip warm" : "pip"} />
          {needsYou} <b>needs you</b>
        </span>
      </div>
    </header>
  );
}
