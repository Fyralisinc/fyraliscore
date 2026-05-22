// Current vs. Proposed diff — spec §11.
//
// Two comparison panels joined by a quiet center axis of small icons.
// The center icon is picked by valueType (status/owner/duration/...) so
// the axis visually anchors each row even when the user is scanning
// only one side. Changed values on the Proposed side get severity
// tinting; unchanged values stay neutral so the eye is drawn to
// what actually moved.

import type { DeltaField, ValueSeverity } from "@/api/today-page-types";

interface Props {
  current: DeltaField[];
  proposed: DeltaField[];
}

interface DiffRow {
  key: string;
  label: string;
  from?: DeltaField;
  to?: DeltaField;
  changed: boolean;
}

function buildRows(current: DeltaField[], proposed: DeltaField[]): DiffRow[] {
  const map = new Map<string, DiffRow>();
  for (const f of current) {
    map.set(f.key, { key: f.key, label: f.label, from: f, changed: false });
  }
  for (const f of proposed) {
    const prev = map.get(f.key);
    if (prev) {
      prev.to = f;
      prev.changed = (prev.from?.value ?? "") !== f.value;
    } else {
      map.set(f.key, { key: f.key, label: f.label, to: f, changed: true });
    }
  }
  return Array.from(map.values());
}

export function ChangeDiff({ current, proposed }: Props) {
  const rows = buildRows(current, proposed);
  if (rows.length === 0) return null;
  return (
    <section className="tdv2-diff2" aria-label="Current versus proposed" data-testid="change-diff">
      <h3 className="tdv2-diff2__heading">Current vs. proposed</h3>
      <div className="tdv2-diff2__grid">
        <Panel side="current" rows={rows} />
        <Axis rows={rows} />
        <Panel side="proposed" rows={rows} />
      </div>
    </section>
  );
}

function Panel({
  side,
  rows,
}: {
  side: "current" | "proposed";
  rows: DiffRow[];
}) {
  return (
    <div className={`tdv2-diff2__panel tdv2-diff2__panel--${side}`}>
      <div className="tdv2-diff2__panel-head">
        {side === "current" ? "Current" : "Proposed"}
      </div>
      <dl className="tdv2-diff2__rows">
        {rows.map((r) => {
          const field = side === "current" ? r.from : r.to;
          const showSeverity =
            side === "proposed" && r.changed && field?.severity != null;
          const valueClass = [
            "tdv2-diff2__value",
            side === "current" ? "tdv2-diff2__value--from" : "tdv2-diff2__value--to",
            showSeverity ? `tdv2-diff2__value--${field?.severity}` : "",
          ]
            .filter(Boolean)
            .join(" ");
          return (
            <div key={r.key} className="tdv2-diff2__row">
              <dt className="tdv2-diff2__label">{r.label}</dt>
              <dd className={valueClass}>{field?.value ?? "—"}</dd>
            </div>
          );
        })}
      </dl>
    </div>
  );
}

function Axis({ rows }: { rows: DiffRow[] }) {
  return (
    <div className="tdv2-diff2__axis" aria-hidden="true">
      <span className="tdv2-diff2__axis-spacer" />
      {rows.map((r) => (
        <span
          key={r.key}
          className={`tdv2-diff2__axis-cell${
            r.changed ? " tdv2-diff2__axis-cell--changed" : ""
          }`}
        >
          <AxisIcon
            kind={axisKind(r)}
            severity={r.to?.severity}
            changed={r.changed}
          />
        </span>
      ))}
    </div>
  );
}

function axisKind(r: DiffRow): AxisIconKind {
  const t = r.to?.valueType ?? r.from?.valueType;
  switch (t) {
    case "status":   return "state";
    case "owner":    return "owner";
    case "date":
    case "duration": return "calendar";
    case "money":    return "money";
    default:
      // Heuristic on label so untyped fields still get a useful icon.
      if (/owner/i.test(r.label)) return "owner";
      if (/scope/i.test(r.label)) return "scope";
      if (/re[- ]?eval|cadence|due/i.test(r.label)) return "calendar";
      return "scope";
  }
}

type AxisIconKind = "state" | "scope" | "owner" | "calendar" | "money";

function AxisIcon({
  kind,
  changed,
}: {
  kind: AxisIconKind;
  severity?: ValueSeverity;
  changed: boolean;
}) {
  // 18×18 icons. Stroke-only, low-contrast on unchanged rows so the
  // axis stays quiet on rows that don't need attention.
  const stroke = changed ? "currentColor" : "rgba(102, 114, 106, 0.45)";
  const common = {
    width: 18,
    height: 18,
    viewBox: "0 0 18 18",
    fill: "none",
    stroke,
    strokeWidth: 1.4,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
  switch (kind) {
    case "state":
      return (
        <svg {...common} aria-hidden="true">
          <path d="M4 9l3 3 7-7" />
        </svg>
      );
    case "scope":
      return (
        <svg {...common} aria-hidden="true">
          <circle cx="9" cy="9" r="5" />
          <circle cx="9" cy="9" r="1.6" />
        </svg>
      );
    case "owner":
      return (
        <svg {...common} aria-hidden="true">
          <circle cx="9" cy="7" r="2.6" />
          <path d="M3.6 14.6c.7-2.4 2.9-3.8 5.4-3.8s4.7 1.4 5.4 3.8" />
        </svg>
      );
    case "calendar":
      return (
        <svg {...common} aria-hidden="true">
          <rect x="3" y="4.5" width="12" height="10" rx="1.4" />
          <path d="M3 7.5h12" />
          <path d="M6.5 3v3M11.5 3v3" />
        </svg>
      );
    case "money":
      return (
        <svg {...common} aria-hidden="true">
          <path d="M9 3v12" />
          <path d="M12 6.2c-.6-.9-1.7-1.4-3-1.4-1.7 0-3 .9-3 2.1 0 1.2 1.2 2 3 2 1.7 0 3 .9 3 2.1 0 1.2-1.3 2.2-3 2.2-1.5 0-2.6-.6-3.1-1.6" />
        </svg>
      );
  }
}
