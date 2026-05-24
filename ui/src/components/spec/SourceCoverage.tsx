import type { SourceCoverage } from "@/api/trust-types";

interface Props {
  items: SourceCoverage[];
}

const STATUS_LABEL: Record<SourceCoverage["status"], string> = {
  connected: "connected",
  limited: "limited",
  not_connected: "not connected",
  stale: "stale",
};

// Source coverage component — spec §15.6. Use text labels alongside dots
// since color is never the only signal (accessibility).
export function SourceCoverageList({ items }: Props) {
  return (
    <div className="fx-sources" aria-label="Source coverage">
      {items.map((s) => (
        <div key={s.source} className="fx-sources__item">
          <span
            className={`fx-sources__dot fx-sources__dot--${s.status}`}
            aria-hidden="true"
          />
          <span>{s.label ?? s.source}</span>
          <span className="fx-muted" style={{ marginLeft: "auto" }}>
            {STATUS_LABEL[s.status]}
          </span>
        </div>
      ))}
    </div>
  );
}
