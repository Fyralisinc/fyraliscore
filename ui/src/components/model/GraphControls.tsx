// Bottom-center zoom controls. The handlers operate on a numeric zoom
// state owned by the parent (the SVG canvas scales by it). Lock and
// grid are placeholder toggles for now.

export interface GraphControlsProps {
  zoom: number;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onFit: () => void;
  locked: boolean;
  onToggleLock: () => void;
  showGrid: boolean;
  onToggleGrid: () => void;
}

export function GraphControls({
  zoom,
  onZoomIn,
  onZoomOut,
  onFit,
  locked,
  onToggleLock,
  showGrid,
  onToggleGrid,
}: GraphControlsProps) {
  return (
    <div
      className="fy-model-controls"
      role="group"
      aria-label="Graph zoom controls"
      data-testid="graph-controls"
    >
      <button
        type="button"
        className="fy-model-controls__btn"
        onClick={onZoomOut}
        aria-label="Zoom out"
        data-testid="zoom-out"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
          <path d="M3 8h10" />
        </svg>
      </button>
      <span className="fy-model-controls__zoom" data-testid="zoom-value">
        {Math.round(zoom * 100)}%
      </span>
      <button
        type="button"
        className="fy-model-controls__btn"
        onClick={onZoomIn}
        aria-label="Zoom in"
        data-testid="zoom-in"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
          <path d="M3 8h10M8 3v10" />
        </svg>
      </button>
      <button
        type="button"
        className="fy-model-controls__btn"
        onClick={onFit}
        aria-label="Fit to viewport"
        data-testid="zoom-fit"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
          <path d="M2 6V2h4M14 6V2h-4M2 10v4h4M14 10v4h-4" />
        </svg>
      </button>
      <button
        type="button"
        className={`fy-model-controls__btn${locked ? " is-on" : ""}`}
        onClick={onToggleLock}
        aria-label="Lock layout"
        aria-pressed={locked}
        data-testid="zoom-lock"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
          <rect x="3.5" y="7" width="9" height="6" rx="1" />
          <path d="M5 7V5a3 3 0 016 0v2" />
        </svg>
      </button>
      <button
        type="button"
        className={`fy-model-controls__btn${showGrid ? " is-on" : ""}`}
        onClick={onToggleGrid}
        aria-label="Show grid"
        aria-pressed={showGrid}
        data-testid="zoom-grid"
      >
        <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
          <rect x="2.5" y="2.5" width="11" height="11" />
          <path d="M6 2.5v11M10 2.5v11M2.5 6h11M2.5 10h11" />
        </svg>
      </button>
    </div>
  );
}

export default GraphControls;
