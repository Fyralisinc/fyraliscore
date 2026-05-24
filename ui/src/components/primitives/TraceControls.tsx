export interface TraceControlsProps {
  onTraceBack?: () => void;
  onTraceForward?: () => void;
  onOpenInModel?: () => void;
  onViewFullChain?: () => void;
}

export function TraceControls({
  onTraceBack,
  onTraceForward,
  onOpenInModel,
  onViewFullChain,
}: TraceControlsProps) {
  return (
    <div className="fy-trace-controls" role="group" aria-label="Trace controls">
      <button
        type="button"
        className="fy-trace-controls__btn"
        onClick={onTraceBack}
      >
        Trace back
      </button>
      <button
        type="button"
        className="fy-trace-controls__btn"
        onClick={onTraceForward}
      >
        Trace forward
      </button>
      <button
        type="button"
        className="fy-trace-controls__btn"
        onClick={onOpenInModel}
      >
        Open in Model
      </button>
      <button
        type="button"
        className="fy-trace-controls__btn"
        onClick={onViewFullChain}
      >
        View full chain
      </button>
    </div>
  );
}

export default TraceControls;
