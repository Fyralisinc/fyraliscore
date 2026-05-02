import { useState } from "react";
import type { AskResponse } from "@/api/types";

type Props = {
  turn: AskResponse;
  onFollowUp: () => void;
  onSave: () => Promise<boolean>;
  onDone: () => Promise<void>;
};

// Lightweight conversation turn for ask responses surfaced beneath the
// ask zone. Reuses the existing /view/ceo/ask + turn-action endpoints.
export function Conversation({ turn, onFollowUp, onSave, onDone }: Props) {
  const [dismissing, setDismissing] = useState(false);

  function done() {
    setDismissing(true);
    void onDone();
  }
  return (
    <div className={"turn" + (dismissing ? " dismissing" : "")}>
      <div className="turn-q">{turn.query_echo}</div>
      <div
        className="turn-a"
        dangerouslySetInnerHTML={{ __html: turn.response_html }}
      />
      <div className="turn-verbs">
        {turn.verbs.map((v) => (
          <button
            key={v.id}
            className="turn-verb"
            onClick={() => {
              if (v.id === "followup") onFollowUp();
              else if (v.id === "save") void onSave();
              else if (v.id === "done") done();
            }}
            type="button"
          >
            {v.label}
          </button>
        ))}
      </div>
    </div>
  );
}
