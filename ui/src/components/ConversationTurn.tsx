import { useState } from "react";
import type { AskResponse } from "@/api/types";

type Props = {
  turn: AskResponse;
  onFollowUp: () => void;
  onSave: () => Promise<void> | void;
  onDone: () => void;
};

// One conversation-turn card. Query echo + backend response_html + three
// verbs per design doc §11. Done animates collapse via .dismissing class.
export function ConversationTurn({ turn, onFollowUp, onSave, onDone }: Props) {
  const [dismissing, setDismissing] = useState(false);
  const [saved, setSaved] = useState(false);

  const handleDone = () => {
    setDismissing(true);
    // Let the CSS transition run before unmounting in the parent.
    window.setTimeout(onDone, 300);
  };
  const handleSave = async () => {
    await onSave();
    setSaved(true);
  };

  return (
    <div
      className={dismissing ? "turn dismissing" : "turn"}
      data-testid="turn"
      data-turn-id={turn.turn_id}
    >
      <div className="tq">{turn.query_echo}</div>
      <div
        className="ta"
        dangerouslySetInnerHTML={{ __html: turn.response_html }}
      />
      <div className="turn-verbs">
        {turn.verbs.map((v) => {
          if (v.id === "followup") {
            return (
              <button
                key={v.id}
                type="button"
                className="verb"
                onClick={onFollowUp}
              >
                {v.label}
              </button>
            );
          }
          if (v.id === "save") {
            return (
              <button
                key={v.id}
                type="button"
                className="verb"
                onClick={handleSave}
                style={saved ? { color: "var(--cool)" } : undefined}
              >
                {saved ? "✓ Saved" : v.label}
              </button>
            );
          }
          return (
            <button
              key={v.id}
              type="button"
              className="verb"
              onClick={handleDone}
            >
              {v.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
