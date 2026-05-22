import type { CausalRibbonCell } from "@/api/operating-thread-types";

interface Props {
  cells: CausalRibbonCell[];
}

// Vertical (inspector) version of the causal ribbon.
export function CausalSpine({ cells }: Props) {
  return (
    <div className="fx-spine" aria-label="Causal spine">
      {cells.map((c, i) => (
        <div key={i} className="fx-spine__cell">
          <div className="fx-spine__label">{c.label}</div>
          <div className="fx-spine__value">{c.value}</div>
        </div>
      ))}
    </div>
  );
}
