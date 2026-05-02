import { useEffect } from "react";

type Props = { onClose: () => void };

const ROWS: Array<{ keys: string[]; label: string }> = [
  { keys: ["J", "K"], label: "Next / previous card" },
  { keys: ["Enter"], label: "Toggle expansion" },
  { keys: ["A"], label: "Act on focused card" },
  { keys: ["H"], label: "Hold focused card" },
  { keys: ["R"], label: "Route focused card" },
  { keys: ["S"], label: "Snooze focused card" },
  { keys: ["D"], label: "Dismiss focused card" },
  { keys: ["/"], label: "Focus the Ask field" },
  { keys: ["1", "2", "3"], label: "Filter All / Operational / Strategic" },
  { keys: ["?"], label: "Show this overlay" },
  { keys: ["Esc"], label: "Close overlay or blur input" },
];

// Per spec §5.7 — modal overlay; click outside or Esc closes.
export function ShortcutsOverlay({ onClose }: Props) {
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      if (e.key === "Escape") onClose();
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  return (
    <div
      className="kbd-overlay"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-label="Keyboard shortcuts"
    >
      <div className="kbd-card" onClick={(e) => e.stopPropagation()}>
        <div className="kbd-title">Keyboard shortcuts</div>
        <div className="kbd-subtitle">
          Built for the morning where you've got 90 seconds.
        </div>
        <div className="kbd-grid">
          {ROWS.map((r, i) => (
            <div className="kbd-row" key={i}>
              <span>{r.label}</span>
              <span className="keys">
                {r.keys.map((k) => (
                  <kbd key={k}>{k}</kbd>
                ))}
              </span>
            </div>
          ))}
        </div>
        <div className="kbd-close">Esc to close</div>
      </div>
    </div>
  );
}
