import type { Greeting as GreetingT } from "@/api/types";
import { formatKathmanduMetaLine, formatStaleness } from "@/lib/time";

type Props = { greeting: GreetingT };

// Meta-line in mono + one paragraph of backend-rendered prose. The
// body_html is trusted HTML from the rendering service (voice_rules.py
// validates). Design doc §10.2.
export function Greeting({ greeting }: Props) {
  const meta = formatKathmanduMetaLine(greeting.meta.recomputed_at);
  const freshness = formatStaleness(greeting.staleness_seconds);
  return (
    <section className="greeting" data-testid="greeting">
      <div className="greeting-meta">
        <span>{meta}</span>
        <span className="sep">·</span>
        <span className="fresh">{freshness}</span>
        <span className="sep">·</span>
        <span>
          watching <b>{greeting.meta.signals_watched_count.toLocaleString()}</b>{" "}
          signals since Friday
        </span>
      </div>
      <p
        className="greeting-body"
        // body_html is authored by the rendering service; voice_rules
        // validate. Safe per CONTRACTS §5.
        dangerouslySetInnerHTML={{ __html: greeting.body_html }}
      />
    </section>
  );
}
