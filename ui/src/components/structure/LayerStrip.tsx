import type { LayerId, LayerStripCounts } from "./types";

// Spec Part 2.2 — five layer entry points + utility cell. Same visual
// shell as Today's signal strip; active cell gets the signature rail.
type Props = {
  active: LayerId;
  counts: LayerStripCounts;
  onSwitch: (id: LayerId) => void;
  onShortcuts?: () => void;
};

type Cell = {
  id: LayerId;
  label: string;
  primary: string;
  secondary: string;
  secondaryWarn?: boolean;
};

export function LayerStrip({ active, counts, onSwitch, onShortcuts }: Props) {
  const cells: Cell[] = [
    {
      id: "commits",
      label: "COMMITS",
      primary: `${counts.commits.active} active`,
      secondary: `${counts.commits.at_risk} at risk`,
      secondaryWarn: counts.commits.at_risk > 0,
    },
    {
      id: "decisions",
      label: "DECISIONS",
      primary: `${counts.decisions.in_force} in force`,
      secondary: `${counts.decisions.in_drift} in drift`,
      secondaryWarn: counts.decisions.in_drift > 0,
    },
    {
      id: "people",
      label: "PEOPLE",
      primary: `${counts.people.count} in ${counts.people.teams} teams`,
      secondary: "",
    },
    {
      id: "customers",
      label: "CUSTOMERS",
      primary: `${counts.customers.active} active`,
      secondary: `${counts.customers.healthy_pct}% healthy`,
    },
    {
      id: "model",
      label: "MODEL",
      primary: `${counts.model.calibration.toFixed(2)} cal.`,
      secondary: `${counts.model.contested} contested`,
    },
  ];

  return (
    <nav
      className="layer-strip"
      aria-label="Structure layers"
      role="tablist"
    >
      {cells.map((c) => {
        const isActive = c.id === active;
        return (
          <button
            key={c.id}
            role="tab"
            aria-selected={isActive}
            type="button"
            className={"layer-cell" + (isActive ? " active" : "")}
            onClick={() => onSwitch(c.id)}
          >
            <span className="layer-cell-label">{c.label}</span>
            <span className="layer-cell-primary">{c.primary}</span>
            {c.secondary ? (
              <span
                className={
                  "layer-cell-secondary" + (c.secondaryWarn ? " warn" : "")
                }
              >
                {c.secondary}
              </span>
            ) : (
              <span className="layer-cell-secondary" aria-hidden="true">
                &nbsp;
              </span>
            )}
          </button>
        );
      })}
      <button
        type="button"
        className="layer-cell layer-cell-utility"
        onClick={onShortcuts}
        aria-label="Keyboard shortcuts"
      >
        <span className="kbd-hint">
          <span className="key">?</span>
          <span className="kbd-hint-label">Shortcuts</span>
        </span>
      </button>
    </nav>
  );
}
