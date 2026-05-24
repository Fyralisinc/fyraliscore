import type { ContextGap } from "@/api/trust-types";

interface Props {
  gaps: ContextGap[];
  onAddContext?: () => void;
  onConnectSource?: () => void;
  onAskOwner?: () => void;
  compact?: boolean;
}

// Context Gap component (spec §10). Warm-attention style, not warning.
// Suppresses itself entirely when no gaps exist — humility is signal,
// not chrome.
export function ContextGapList({
  gaps,
  onAddContext,
  onConnectSource,
  onAskOwner,
  compact,
}: Props) {
  if (gaps.length === 0) return null;

  return (
    <div className="fx-gaps" role="region" aria-label="What Fyralis may be missing">
      <div className="fx-gaps__label">What Fyralis may be missing</div>
      {gaps.map((g) => (
        <div key={g.id} className="fx-gaps__item">
          {g.text}
        </div>
      ))}
      {!compact ? (
        <div className="fx-gaps__actions">
          {onAddContext ? (
            <button type="button" className="fx-btn fx-btn--sm" onClick={onAddContext}>
              Add context
            </button>
          ) : null}
          {onConnectSource ? (
            <button type="button" className="fx-btn fx-btn--sm fx-btn--ghost" onClick={onConnectSource}>
              Connect source
            </button>
          ) : null}
          {onAskOwner ? (
            <button type="button" className="fx-btn fx-btn--sm fx-btn--ghost" onClick={onAskOwner}>
              Ask owner
            </button>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
