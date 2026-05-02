import { useState } from "react";
import type { SuggestedSignalItem } from "@/api/demo-client";
import { SuggestedSignals } from "./SuggestedSignals";
import { SendStatusLine } from "./SlackTab";
import type { SendFn, SendStatus } from "./types";

type Props = {
  suggestions: SuggestedSignalItem[];
  send: SendFn;
  status: SendStatus;
};

export function CalendarTab({ suggestions, send, status }: Props) {
  const [title, setTitle] = useState("");
  const [attendees, setAttendees] = useState("");
  const [minutesAgo, setMinutesAgo] = useState(60);

  function pick(item: SuggestedSignalItem) {
    const p = item.payload as Record<string, unknown>;
    if (typeof p.title === "string") setTitle(p.title);
    if (Array.isArray(p.attendees)) setAttendees(p.attendees.join("\n"));
    else if (typeof p.attendees === "string") setAttendees(p.attendees);
    if (typeof p.minutes_ago === "number") setMinutesAgo(p.minutes_ago);
  }

  function onSend() {
    const list = attendees
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean);
    void send("calendar:event", {
      title,
      attendees: list,
      minutes_ago: minutesAgo,
    });
  }

  return (
    <div className="sim-form">
      <label className="sim-label">
        Title
        <input
          className="sim-input"
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
        />
      </label>
      <label className="sim-label">
        Attendees (one per line)
        <textarea
          className="sim-textarea"
          rows={4}
          value={attendees}
          onChange={(e) => setAttendees(e.target.value)}
        />
      </label>
      <label className="sim-label">
        Minutes ago
        <input
          className="sim-input"
          type="number"
          value={minutesAgo}
          min={0}
          onChange={(e) => setMinutesAgo(Number(e.target.value) || 0)}
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
