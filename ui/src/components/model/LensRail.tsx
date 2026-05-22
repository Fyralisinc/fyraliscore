// Left-side lens rail. Three sections: lenses (filter mode), show
// filters (per-band toggles), status filters (active/blocked/contested)
// and a search input at the bottom. Lenses are presentational here —
// the spec uses them to swap the workspace metaphor; for Wave 2 the
// active lens just emits a class hook so the page can highlight it.

import type {
  LensId,
  ShowFilters,
  StatusFilters,
} from "./types";
import { BAND_LABELS } from "./types";
import type { MapBand } from "@/api/map-types";

export interface LensRailProps {
  activeLens: LensId;
  onLensChange: (lens: LensId) => void;
  show: ShowFilters;
  onShowChange: (next: ShowFilters) => void;
  status: StatusFilters;
  onStatusChange: (next: StatusFilters) => void;
  search: string;
  onSearchChange: (v: string) => void;
}

const LENSES: Array<{ id: LensId; label: string }> = [
  { id: "company", label: "Company" },
  { id: "commitments", label: "Commitments" },
  { id: "decisions", label: "Decisions" },
  { id: "customers", label: "Customers" },
  { id: "teams", label: "Teams" },
  { id: "risks", label: "Risks" },
  { id: "owners", label: "Owners" },
  { id: "predictions", label: "Predictions" },
];

const BAND_TOGGLES: Array<{ id: MapBand; label: string }> = [
  { id: "goal", label: "Goals" },
  { id: "commitment", label: "Commitments" },
  { id: "decision", label: "Decisions" },
  { id: "risk", label: "Risks" },
  { id: "customer", label: "Customers" },
];

export function LensRail({
  activeLens,
  onLensChange,
  show,
  onShowChange,
  status,
  onStatusChange,
  search,
  onSearchChange,
}: LensRailProps) {
  return (
    <aside className="fy-model-lens-rail" aria-label="Model lenses and filters">
      <div className="fy-model-lens-rail__group">
        <div className="fy-model-lens-rail__heading">Lenses</div>
        {LENSES.map((lens) => (
          <button
            key={lens.id}
            type="button"
            className={`fy-model-lens-rail__lens${
              activeLens === lens.id ? " is-active" : ""
            }`}
            onClick={() => onLensChange(lens.id)}
            data-lens={lens.id}
            data-testid={`lens-${lens.id}`}
          >
            {lens.label}
          </button>
        ))}
      </div>

      <div className="fy-model-lens-rail__group">
        <div className="fy-model-lens-rail__heading">Show</div>
        {BAND_TOGGLES.map((b) => (
          <label
            key={b.id}
            className="fy-model-lens-rail__check"
            data-testid={`show-${b.id}`}
          >
            <input
              type="checkbox"
              checked={show[b.id]}
              onChange={(e) =>
                onShowChange({ ...show, [b.id]: e.target.checked })
              }
              aria-label={BAND_LABELS[b.id]}
            />
            <span>{b.label}</span>
          </label>
        ))}
      </div>

      <div className="fy-model-lens-rail__group">
        <div className="fy-model-lens-rail__heading">Status</div>
        {(
          [
            ["active", "Active"],
            ["blocked", "Blocked"],
            ["contested", "Contested"],
          ] as Array<[keyof StatusFilters, string]>
        ).map(([key, label]) => (
          <label
            key={key}
            className="fy-model-lens-rail__check"
            data-testid={`status-${key}`}
          >
            <input
              type="checkbox"
              checked={status[key]}
              onChange={(e) =>
                onStatusChange({ ...status, [key]: e.target.checked })
              }
              aria-label={label}
            />
            <span>{label}</span>
          </label>
        ))}
      </div>

      <div className="fy-model-lens-rail__group">
        <label className="fy-model-lens-rail__search">
          <span className="fy-model-lens-rail__heading">Search model</span>
          <input
            type="search"
            value={search}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Search field"
            aria-label="Search model"
            data-testid="model-search"
          />
        </label>
      </div>
    </aside>
  );
}

export default LensRail;
