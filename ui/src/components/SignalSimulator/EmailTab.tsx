import { useEffect, useState } from "react";
import type { SuggestedSignalItem } from "@/api/demo-client";
import { SuggestedSignals } from "./SuggestedSignals";
import { SendStatusLine } from "./SlackTab";
import type { SendFn, SendStatus } from "./types";

type Props = {
  suggestions: SuggestedSignalItem[];
  send: SendFn;
  status: SendStatus;
};

export function EmailTab({ suggestions, send, status }: Props) {
  const [from, setFrom] = useState("");
  const [to, setTo] = useState("");
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");

  useEffect(() => {
    if (status.kind === "sent") setBody("");
  }, [status]);

  function pick(item: SuggestedSignalItem) {
    const p = item.payload as Record<string, unknown>;
    if (typeof p.from === "string") setFrom(p.from);
    if (typeof p.to === "string") setTo(p.to);
    if (typeof p.subject === "string") setSubject(p.subject);
    if (typeof p.body === "string") setBody(p.body);
  }

  function onSend() {
    void send("email:message", { from, to, subject, body });
  }

  return (
    <div className="sim-form">
      <label className="sim-label">
        From
        <input
          className="sim-input"
          type="text"
          value={from}
          onChange={(e) => setFrom(e.target.value)}
        />
      </label>
      <label className="sim-label">
        To
        <input
          className="sim-input"
          type="text"
          value={to}
          onChange={(e) => setTo(e.target.value)}
        />
      </label>
      <label className="sim-label">
        Subject
        <input
          className="sim-input"
          type="text"
          value={subject}
          onChange={(e) => setSubject(e.target.value)}
        />
      </label>
      <label className="sim-label">
        Body
        <textarea
          className="sim-textarea"
          rows={5}
          value={body}
          onChange={(e) => setBody(e.target.value)}
        />
      </label>
      <div className="sim-send-bar">
        <button
          type="button"
          className="sim-send"
          onClick={onSend}
          disabled={status.kind === "sending"}
        >
          {status.kind === "sending" ? "Sending…" : "Send"}
        </button>
        <SendStatusLine status={status} />
      </div>
      <SuggestedSignals items={suggestions} onPick={pick} />
    </div>
  );
}
