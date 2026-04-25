import type { QueryChip } from "@/api/types";
import { Icon } from "./Icon";

type Props = {
  queries: QueryChip[];
  onAsk: (query: string) => void;
};

const TAG_COPY: Record<string, string> = {
  urgent: "urgent",
  relevant: "relevant",
  "2min": "2 min",
  evergreen: "evergreen",
};

// Six pre-loaded queries in a 2-column grid. Hot chips reference today's
// live situation; evergreen chips are standing patterns. Design doc §10.3.
export function QueryGrid({ queries, onAsk }: Props) {
  return (
    <section className="queries" data-testid="query-grid">
      {queries.map((q) => (
        <button
          key={q.id}
          type="button"
          className={q.hot ? "q hot" : "q"}
          onClick={() => onAsk(q.label)}
        >
          <span className="q-glyph">
            <Icon name={q.icon} />
          </span>
          {q.label}
          {q.tag ? <span className="q-tag">{TAG_COPY[q.tag] ?? q.tag}</span> : null}
        </button>
      ))}
    </section>
  );
}
