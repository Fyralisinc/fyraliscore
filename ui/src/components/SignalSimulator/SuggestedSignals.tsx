import type { SuggestedSignalItem } from "@/api/demo-client";

type Props = {
  items: SuggestedSignalItem[];
  onPick: (item: SuggestedSignalItem) => void;
};

export function SuggestedSignals({ items, onPick }: Props) {
  if (!items || items.length === 0) return null;
  return (
    <div className="sim-suggested">
      <div className="sim-suggested-label">Suggested signals</div>
      <div className="sim-suggested-list">
        {items.map((it, i) => (
          <button
            key={i}
            type="button"
            className="sim-suggested-item"
            onClick={() => onPick(it)}
          >
            {it.label}
          </button>
        ))}
      </div>
    </div>
  );
}
