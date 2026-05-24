import type { LedgerChainCard } from "@/api/ledger-v2-types";
import { ChainCard } from "./ChainCard";

interface Props {
  chains: LedgerChainCard[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}

interface Bucket {
  label: string;
  description: string;
  match: (c: LedgerChainCard) => boolean;
  empty: string;
}

// Resolutions mode — outcome-oriented view of the same chain set.
const BUCKETS: Bucket[] = [
  {
    label: "Resolved",
    description: "Chains that have closed with an outcome.",
    match: (c) => c.status === "resolved" || c.status === "resolved_true",
    empty: "Nothing closed in this window yet.",
  },
  {
    label: "Monitoring",
    description: "Resolution accepted; watching for sustained signal.",
    match: (c) => c.status === "monitoring",
    empty: "Nothing currently in monitoring.",
  },
  {
    label: "Still open",
    description: "Active chains awaiting acceptance, owner, or evidence.",
    match: (c) => c.status === "open",
    empty: "Nothing open.",
  },
  {
    label: "Corrected / Contested",
    description: "Chains where a claim was corrected or pushed back on.",
    match: (c) =>
      c.status === "corrected" ||
      c.status === "contested" ||
      c.status === "resolved_false",
    empty: "No corrections or contestations in this window.",
  },
];

export function ResolutionsMode({ chains, selectedId, onSelect }: Props) {
  return (
    <section className="lg-resolutions" aria-label="Resolutions">
      {BUCKETS.map((b) => {
        const matches = chains.filter(b.match);
        return (
          <div className="lg-resolutions__bucket" key={b.label}>
            <header className="lg-resolutions__head">
              <h3 className="lg-resolutions__title">{b.label}</h3>
              <span className="lg-resolutions__count">{matches.length}</span>
              <span className="lg-resolutions__desc">{b.description}</span>
            </header>
            {matches.length === 0 ? (
              <p className="lg-empty">{b.empty}</p>
            ) : (
              <ul className="lg-resolutions__list">
                {matches.map((c) => (
                  <li key={c.id}>
                    <ChainCard
                      chain={c}
                      selected={c.id === selectedId}
                      onSelect={onSelect}
                    />
                  </li>
                ))}
              </ul>
            )}
          </div>
        );
      })}
    </section>
  );
}
