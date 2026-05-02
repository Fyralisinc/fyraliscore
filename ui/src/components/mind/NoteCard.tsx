import type { Note } from "./types";

// Spec Part 7 — Note card grammar.
type Props = {
  note: Note;
  justCreated?: boolean;
  showParseChip?: boolean;
  onPromoteToLoop: () => void;
  onPromoteToReminder: () => void;
  onRemove: () => void;
  onChangeCategory?: (target: "loop" | "reminder") => void;
};

export function NoteCard({
  note,
  justCreated,
  showParseChip,
  onPromoteToLoop,
  onPromoteToReminder,
  onRemove,
  onChangeCategory,
}: Props) {
  return (
    <article
      className={"item note" + (justCreated ? " just-created" : "")}
      data-id={note.id}
      data-state={note.state}
      role="article"
      aria-label={`Note: ${note.headline}`}
    >
      <header className="item-header">
        <span className="item-type">NOTE</span>
        <span className="item-meta">{formatDate(note.created)}</span>
      </header>

      <div className="item-body">
        <p className="item-headline">{note.headline}</p>
        {showParseChip ? (
          <div className="parse-chip">
            <span className="parse-chip-text">↑ Parsed as a Note.</span>
            <div className="parse-chip-actions">
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("loop")}
              >
                Change to Loop
              </button>
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("reminder")}
              >
                Change to Reminder
              </button>
            </div>
          </div>
        ) : null}
      </div>

      {note.source ? (
        <div className="item-source">
          <span className="source-label">Source</span>
          <span className="source-value">{note.source}</span>
        </div>
      ) : null}

      {note.substrate_stance ? (
        <div className="item-substrate-context">
          <p className="substrate-evidence">↑ {note.substrate_stance}</p>
        </div>
      ) : null}

      <footer className="item-footer">
        <button type="button" className="item-action" onClick={onRemove}>
          Remove
        </button>
        <div className="item-actions-right">
          <button type="button" className="item-action" onClick={onPromoteToReminder}>
            Promote to Reminder
          </button>
          <button type="button" className="item-action primary" onClick={onPromoteToLoop}>
            Promote to Loop
          </button>
        </div>
      </footer>
    </article>
  );
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString("en-US", { month: "short", day: "numeric" }).toLowerCase();
}
