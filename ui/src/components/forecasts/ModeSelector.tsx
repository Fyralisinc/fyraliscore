// Mode tabs (spec §11): Horizon | Patterns | Scenarios | Accuracy.

import type { ForecastMode } from "@/api/forecasts-types";

const MODES: Array<{ id: ForecastMode; label: string }> = [
  { id: "horizon", label: "Horizon" },
  { id: "patterns", label: "Patterns" },
  { id: "scenarios", label: "Scenarios" },
  { id: "accuracy", label: "Accuracy" },
];

export interface ModeSelectorProps {
  mode: ForecastMode;
  onChange: (mode: ForecastMode) => void;
}

export function ModeSelector({ mode, onChange }: ModeSelectorProps) {
  return (
    <div className="fc-modes" role="tablist" aria-label="Forecasts mode">
      {MODES.map((m) => {
        const active = mode === m.id;
        return (
          <button
            key={m.id}
            type="button"
            role="tab"
            aria-selected={active}
            tabIndex={active ? 0 : -1}
            className={`fc-modes__tab${active ? " fc-modes__tab--active" : ""}`}
            onClick={() => onChange(m.id)}
          >
            {m.label}
          </button>
        );
      })}
    </div>
  );
}
