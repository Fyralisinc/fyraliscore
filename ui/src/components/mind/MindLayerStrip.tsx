import type { LayerStripCounts, MindLayerId } from "./types";

// Spec Part 3.2 — four entry points + utility cell.
type Props = {
  active: MindLayerId;
  counts: LayerStripCounts;
  onSwitch: (id: MindLayerId) => void;
  onShortcuts?: () => void;
};

export function MindLayerStrip({
  active,
  counts,
  onSwitch,
  onShortcuts,
}: Props) {
  const cells = [
    {
      id: "all" as const,
      label: "ALL",
      primary: counts.all.items === 0 ? "0" : `${counts.all.items} items`,
      secondary: counts.all.due > 0 ? `${counts.all.due} due` : null,
    },
    {
      id: "loops" as const,
      label: "LOOPS",
      primary: `${counts.loops.count}`,
      secondary: counts.loops.aging > 0 ? `${counts.loops.aging} aging` : null,
    },
    {
      id: "notes" as const,
      label: "NOTES",
      primary: `${counts.notes.count}`,
      secondary: counts.notes.today > 0 ? `${counts.notes.today} today` : null,
    },
    {
      id: "reminders" as const,
      label: "REMINDERS",
      primary: `${counts.reminders.count}`,
      secondary:
        counts.reminders.pending > 0
          ? `${counts.reminders.pending} pending`
          : null,
    },
  ];

  return (
    <nav className="layer-strip mind-layer-strip" aria-label="My Mind layers" role="tablist">
      {cells.map((c) => {
        const isActive = c.id === active;
        return (
          <button
            key={c.id}
            type="button"
            role="tab"
            aria-selected={isActive}
            className={"layer-cell" + (isActive ? " active" : "")}
            onClick={() => onSwitch(c.id)}
          >
            <span className="layer-cell-label">{c.label}</span>
            <span className="layer-cell-primary">{c.primary}</span>
            {c.secondary ? (
              <span className="layer-cell-secondary">{c.secondary}</span>
            ) : null}
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
