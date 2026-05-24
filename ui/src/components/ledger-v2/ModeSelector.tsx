import type { LedgerMode } from "@/api/ledger-v2-types";

interface Props {
  mode: LedgerMode;
  onChange: (mode: LedgerMode) => void;
}

const MODES: { id: LedgerMode; label: string }[] = [
  { id: "timeline", label: "Timeline" },
  { id: "resolutions", label: "Resolutions" },
  { id: "accuracy", label: "Accuracy" },
  { id: "audit", label: "Audit" },
];

// Underline-style tabs sitting above the workspace. Matches the
// screenshot: moss underline + label on the active tab; muted text on
// the rest.
export function ModeSelector({ mode, onChange }: Props) {
  return (
    <div className="lg-tabs" role="tablist" aria-label="Ledger mode">
      {MODES.map((m) => (
        <button
          key={m.id}
          role="tab"
          aria-selected={mode === m.id}
          className={`lg-tab${mode === m.id ? " lg-tab--active" : ""}`}
          onClick={() => onChange(m.id)}
          type="button"
        >
          {m.label}
        </button>
      ))}
    </div>
  );
}
