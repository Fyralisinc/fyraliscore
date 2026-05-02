import { useState } from "react";
import type { Loop, LoopKind, Note, Reminder } from "./types";

// Spec Part 11.5 — Promote modal for Note → Loop / Note → Reminder.
type Target = "loop" | "reminder";

type Props = {
  note: Note;
  target: Target;
  onCancel: () => void;
  onConfirm: (extras: {
    loop_kind?: LoopKind;
    person?: string | null;
    thinking?: string;
    trigger_type?: Reminder["trigger_type"];
    remind_at?: string;
    condition?: string;
  }) => void;
};

export function PromoteModal({ note, target, onCancel, onConfirm }: Props) {
  const [loopKind, setLoopKind] = useState<Loop["kind"]>("action");
  const [person, setPerson] = useState("");
  const [thinking, setThinking] = useState("");
  const [triggerType, setTriggerType] =
    useState<Reminder["trigger_type"]>("time");
  const [remindAt, setRemindAt] = useState("");
  const [condition, setCondition] = useState("");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (target === "loop") {
      onConfirm({
        loop_kind: loopKind,
        person: person.trim() || null,
        thinking: thinking.trim() || undefined,
      });
    } else {
      const remind = remindAt ? new Date(remindAt).toISOString() : undefined;
      onConfirm({
        trigger_type: triggerType,
        remind_at: triggerType === "time" ? remind : undefined,
        condition: triggerType === "condition" ? condition.trim() : undefined,
      });
    }
  }

  return (
    <div
      className="promote-modal-backdrop"
      role="dialog"
      aria-modal="true"
      onClick={(e) => {
        if (e.target === e.currentTarget) onCancel();
      }}
    >
      <form className="promote-modal" onSubmit={handleSubmit}>
        <h3>{target === "loop" ? "Promote this note to a Loop" : "Promote this note to a Reminder"}</h3>

        <div className="promote-content">
          <p className="promote-original">"{note.headline}"</p>

          {target === "loop" ? (
            <>
              <label>
                What kind of loop is this?
                <select
                  value={loopKind}
                  onChange={(e) => setLoopKind(e.target.value as Loop["kind"])}
                >
                  <option value="action">Action (something to do)</option>
                  <option value="concern">Concern (something to watch)</option>
                  <option value="question">Question (something to decide)</option>
                </select>
              </label>
              <label>
                Optional: who is this about?
                <input
                  type="text"
                  placeholder="e.g., David, board"
                  value={person}
                  onChange={(e) => setPerson(e.target.value)}
                />
              </label>
              <label>
                Optional: what are you thinking about it?
                <textarea
                  rows={3}
                  value={thinking}
                  onChange={(e) => setThinking(e.target.value)}
                />
              </label>
            </>
          ) : (
            <>
              <label>
                Trigger type
                <select
                  value={triggerType}
                  onChange={(e) =>
                    setTriggerType(e.target.value as Reminder["trigger_type"])
                  }
                >
                  <option value="time">Time (specific moment)</option>
                  <option value="condition">Watching (substrate-detected activity)</option>
                </select>
              </label>
              {triggerType === "time" ? (
                <label>
                  When should I bring it back?
                  <input
                    type="datetime-local"
                    value={remindAt}
                    onChange={(e) => setRemindAt(e.target.value)}
                  />
                </label>
              ) : (
                <label>
                  What should I watch for?
                  <input
                    type="text"
                    placeholder="e.g., Acme renewal-related activity"
                    value={condition}
                    onChange={(e) => setCondition(e.target.value)}
                  />
                </label>
              )}
            </>
          )}
        </div>

        <div className="promote-actions">
          <button
            type="button"
            className="item-action"
            onClick={onCancel}
          >
            Cancel
          </button>
          <button type="submit" className="item-action primary">
            {target === "loop" ? "Promote to Loop" : "Promote to Reminder"}
          </button>
        </div>
      </form>
    </div>
  );
}
