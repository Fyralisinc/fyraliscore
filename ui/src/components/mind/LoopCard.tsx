import { useState } from "react";
import { ageDays, isAging } from "@/hooks/useMind";
import type { Loop } from "./types";

// Spec Part 6 — Loop card grammar.
type Props = {
  loop: Loop;
  justCreated?: boolean;
  showParseChip?: boolean;
  onResolve: () => void;
  onSendToToday: () => void;
  onPromoteToDecision?: () => void;
  onAddNote: (text: string) => void;
  onChangeCategory?: (target: "note" | "reminder") => void;
  onSeeOriginalToday?: () => void;
};

export function LoopCard({
  loop,
  justCreated,
  showParseChip,
  onResolve,
  onSendToToday,
  onPromoteToDecision,
  onAddNote,
  onChangeCategory,
  onSeeOriginalToday,
}: Props) {
  const [adding, setAdding] = useState(false);
  const [draft, setDraft] = useState("");
  const [resolving, setResolving] = useState(false);

  const aging = isAging(loop);
  const heldDays = ageDays(loop.created);
  const typeLabel = loop.from_today
    ? "LOOP · from Today"
    : `LOOP · ${loop.kind}`;
  const primary =
    loop.kind === "action"
      ? "Mark done"
      : loop.kind === "question"
        ? "Promote to Decision"
        : "Mark resolved";

  function handlePrimary() {
    if (loop.kind === "question" && onPromoteToDecision) {
      onPromoteToDecision();
      return;
    }
    setResolving(true);
    window.setTimeout(() => onResolve(), 480);
  }

  function submitNote() {
    const t = draft.trim();
    if (!t) {
      setAdding(false);
      return;
    }
    onAddNote(t);
    setDraft("");
    setAdding(false);
  }

  return (
    <article
      className={
        "item loop" +
        (justCreated ? " just-created" : "") +
        (resolving ? " resolving" : "") +
        (loop.from_today ? " from-today" : "")
      }
      data-id={loop.id}
      data-kind={loop.kind}
      data-state={loop.state}
      role="article"
      aria-label={`Loop: ${loop.headline}`}
    >
      <header className="item-header">
        <span className="item-type">{typeLabel}</span>
        <span className="item-meta">
          held {heldDays} {heldDays === 1 ? "day" : "days"}
          {aging ? <span className="aging-marker"> · AGING</span> : null}
        </span>
      </header>

      <div className="item-body">
        <p className="item-headline">{loop.headline}</p>

        {showParseChip ? (
          <div className="parse-chip">
            <span className="parse-chip-text">
              ↑ Parsed as a Loop
              {loop.kind ? ` (${loop.kind})` : ""}
              {loop.person ? ` with ${loop.person} as the person` : ""}.
            </span>
            <div className="parse-chip-actions">
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("note")}
              >
                Change to Note
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

      {loop.substrate_evidence || loop.substrate_stance ? (
        <div className="item-substrate-context">
          {loop.substrate_evidence ? (
            <p className="substrate-evidence">↑ {loop.substrate_evidence}</p>
          ) : null}
          {loop.substrate_stance ? (
            <p className="substrate-stance">↑ {loop.substrate_stance}</p>
          ) : null}
        </div>
      ) : null}

      {loop.user_notes.length > 0 ? (
        <div className="item-user-notes">
          {loop.user_notes.map((n, i) => (
            <p className="user-note" key={i}>
              <span className="note-date">{formatNoteDate(n.date)}</span>
              {n.text}
            </p>
          ))}
        </div>
      ) : null}

      {adding ? (
        <div className="item-add-note">
          <textarea
            placeholder="What are you thinking?"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            autoFocus
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                submitNote();
              } else if (e.key === "Escape") {
                setAdding(false);
                setDraft("");
              }
            }}
          />
          <div className="add-note-actions">
            <button
              type="button"
              className="item-action"
              onClick={() => {
                setAdding(false);
                setDraft("");
              }}
            >
              Cancel
            </button>
            <button
              type="button"
              className="item-action primary"
              onClick={submitNote}
            >
              Add note
            </button>
          </div>
        </div>
      ) : null}

      <footer className="item-footer">
        <button
          type="button"
          className="item-action"
          onClick={() => setAdding((v) => !v)}
        >
          Add note
        </button>
        <div className="item-actions-right">
          {loop.from_today && onSeeOriginalToday ? (
            <button
              type="button"
              className="item-action"
              onClick={onSeeOriginalToday}
            >
              See original Today card
            </button>
          ) : null}
          <button
            type="button"
            className="item-action"
            onClick={onSendToToday}
          >
            Send to Today
          </button>
          <button
            type="button"
            className="item-action primary"
            onClick={handlePrimary}
          >
            {primary}
          </button>
        </div>
      </footer>
    </article>
  );
}

function formatNoteDate(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString("en-US", { month: "short", day: "numeric" }).toLowerCase();
}
