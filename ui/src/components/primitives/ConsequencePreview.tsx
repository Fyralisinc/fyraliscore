export type ConsequenceVerb =
  | "create"
  | "update"
  | "archive"
  | "notify"
  | "reeval";

export interface ConsequenceEntry {
  verb: ConsequenceVerb;
  text: string;
}

export interface ConsequencePreviewProps {
  entries: ConsequenceEntry[];
  title?: string;
}

const verbLabel: Record<ConsequenceVerb, string> = {
  create: "Creates",
  update: "Updates",
  archive: "Archives",
  notify: "Notifies",
  reeval: "Re-evaluates",
};

export function ConsequencePreview({
  entries,
  title = "If accepted",
}: ConsequencePreviewProps) {
  return (
    <div className="fy-consequence">
      <div className="fy-consequence__title">{title}</div>
      <ul className="fy-consequence__list">
        {entries.map((entry, idx) => (
          <li key={idx} className="fy-consequence__item">
            <span
              className={`fy-consequence__verb fy-consequence__verb--${entry.verb}`}
            >
              {verbLabel[entry.verb]}
            </span>
            <span>{entry.text}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

export default ConsequencePreview;
