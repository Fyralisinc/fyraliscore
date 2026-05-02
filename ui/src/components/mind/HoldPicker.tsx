import { useState } from "react";

// Spec Part 12.3 — Shift+H picker on Today: choose category to send to My Mind.
type Choice = "loop" | "note" | "reminder";

type Props = {
  headline: string;
  onCancel: () => void;
  onConfirm: (
    choice: Choice,
    extras?: { remind_at?: string; condition?: string }
  ) => void;
};

export function HoldPicker({ headline, onCancel, onConfirm }: Props) {
  const [choice, setChoice] = useState<Choice>("loop");
  const [remindAt, setRemindAt] = useState("");
  const [condition, setCondition] = useState("");
  const [reminderType, setReminderType] = useState<"time" | "condition">("time");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (choice === "reminder") {
      const remind = remindAt ? new Date(remindAt).toISOString() : undefined;
      onConfirm(choice, {
        remind_at: reminderType === "time" ? remind : undefined,
        condition: reminderType === "condition" ? condition.trim() : undefined,
      });
    } else {
      onConfirm(choice);
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
      <form className="promote-modal hold-picker" onSubmit={handleSubmit}>
        <h3>Send to My Mind as…</h3>

        <p className="promote-original">"{headline}"</p>

        <div className="hold-picker-options">
          <label className="hold-picker-row">
            <input
              type="radio"
              name="hold-choice"
              checked={choice === "loop"}
              onChange={() => setChoice("loop")}
            />
            <span className="hold-picker-label">
              <strong>Loop</strong>
              <span className="hold-picker-help">
                Active item I'll come back to
              </span>
            </span>
          </label>
          <label className="hold-picker-row">
            <input
              type="radio"
              name="hold-choice"
              checked={choice === "note"}
              onChange={() => setChoice("note")}
            />
            <span className="hold-picker-label">
              <strong>Note</strong>
              <span className="hold-picker-help">
                Capture for later reference
              </span>
            </span>
          </label>
          <label className="hold-picker-row">
            <input
              type="radio"
              name="hold-choice"
              checked={choice === "reminder"}
              onChange={() => setChoice("reminder")}
            />
            <span className="hold-picker-label">
              <strong>Reminder</strong>
              <span className="hold-picker-help">
                When should I bring it back?
              </span>
            </span>
          </label>
        </div>

        {choice === "reminder" ? (
          <div className="hold-picker-reminder-detail">
            <label>
              Trigger
              <select
                value={reminderType}
                onChange={(e) =>
                  setReminderType(e.target.value as "time" | "condition")
                }
              >
                <option value="time">At a specific time</option>
                <option value="condition">When relevant activity</option>
              </select>
            </label>
            {reminderType === "time" ? (
              <label>
                When
                <input
                  type="datetime-local"
                  value={remindAt}
                  onChange={(e) => setRemindAt(e.target.value)}
                />
              </label>
            ) : (
              <label>
                Watch for
                <input
                  type="text"
                  placeholder="e.g., Acme renewal-related activity"
                  value={condition}
                  onChange={(e) => setCondition(e.target.value)}
                />
              </label>
            )}
          </div>
        ) : null}

        <div className="promote-actions">
          <button type="button" className="item-action" onClick={onCancel}>
            Cancel
          </button>
          <button type="submit" className="item-action primary">
            Add
          </button>
        </div>
      </form>
    </div>
  );
}
