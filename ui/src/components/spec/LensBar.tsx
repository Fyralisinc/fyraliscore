import type { ModelLens } from "@/api/operating-thread-types";

interface Props {
  active: ModelLens;
  onChange: (lens: ModelLens) => void;
}

const LENSES: Array<{ id: ModelLens; label: string; question: string }> = [
  { id: "company",     label: "Company",     question: "What is the current operating shape?" },
  { id: "commitments", label: "Commitments", question: "What have we promised, and what threatens those promises?" },
  { id: "decisions",   label: "Decisions",   question: "Which unresolved choices are blocking the company?" },
  { id: "customers",   label: "Customers",   question: "Which customers are affected by internal structure?" },
  { id: "teams",       label: "Teams",       question: "Where is organizational load creating risk?" },
  { id: "risks",       label: "Risks",       question: "What can materially hurt the company, and why?" },
  { id: "owners",      label: "Owners",      question: "Who owns what; where is accountability unclear?" },
  { id: "predictions", label: "Predictions", question: "What does Fyralis think may happen next?" },
];

// 8-lens bar — spec §12.6. Lenses reframe the same model; they are
// not separate pages.
export function LensBar({ active, onChange }: Props) {
  return (
    <div className="fx-row" style={{ gap: 12, alignItems: "center" }}>
      <div className="fx-lensbar" role="tablist" aria-label="Model lens">
        {LENSES.map((l) => (
          <button
            key={l.id}
            type="button"
            role="tab"
            aria-selected={active === l.id}
            className={`fx-lensbar__btn${active === l.id ? " fx-lensbar__btn--active" : ""}`}
            onClick={() => onChange(l.id)}
          >
            {l.label}
          </button>
        ))}
      </div>
      <span className="fx-muted" style={{ fontSize: 13 }}>
        {LENSES.find((l) => l.id === active)?.question}
      </span>
    </div>
  );
}
