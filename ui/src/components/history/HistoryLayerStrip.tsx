import type { HistoryLayerId, LayerStripCounts } from "./types";

// Spec Part 2.2 — three layer entry points + utility cell.
type Props = {
  active: HistoryLayerId;
  counts: LayerStripCounts;
  onSwitch: (id: HistoryLayerId) => void;
  onShortcuts?: () => void;
};

export function HistoryLayerStrip({
  active,
  counts,
  onSwitch,
  onShortcuts,
}: Props) {
  const cells = [
    {
      id: "chronicle" as const,
      label: "CHRONICLE",
      primary: `${counts.chronicle.events} events`,
      secondary: counts.chronicle.period_label,
    },
    {
      id: "predictions" as const,
      label: "PREDICTIONS",
      // Render "—" when nothing has resolved yet — `0.00 cal.` reads
      // like a meaningful score of zero and "0/0 right" looks broken.
      primary:
        counts.predictions.total > 0
          ? `${counts.predictions.calibration.toFixed(2)} cal.`
          : "— cal.",
      secondary:
        counts.predictions.total > 0
          ? `${counts.predictions.correct}/${counts.predictions.total} right`
          : "no resolved yet",
    },
    {
      id: "arcs" as const,
      label: "ARCS",
      primary: `${counts.arcs.active} active`,
      secondary: `${counts.arcs.resolved} resolved`,
    },
  ];

  return (
    <nav className="layer-strip" aria-label="History layers" role="tablist">
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
            <span className="layer-cell-secondary">{c.secondary}</span>
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
