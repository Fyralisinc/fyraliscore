import { useState } from "react";
import type { SuggestedSignalItem } from "@/api/demo-client";
import { SuggestedSignals } from "./SuggestedSignals";
import { SendStatusLine } from "./SlackTab";
import type { SendFn, SendStatus } from "./types";

const EVENT_TYPES = ["pr_opened", "pr_merged", "commit", "issue_comment"] as const;
type EventType = (typeof EVENT_TYPES)[number];

type Props = {
  suggestions: SuggestedSignalItem[];
  send: SendFn;
  status: SendStatus;
};

export function GitHubTab({ suggestions, send, status }: Props) {
  const [repo, setRepo] = useState("");
  const [eventType, setEventType] = useState<EventType>("pr_opened");
  const [author, setAuthor] = useState("");
  const [title, setTitle] = useState("");

  function pick(item: SuggestedSignalItem) {
    const p = item.payload as Record<string, unknown>;
    if (typeof p.repo === "string") setRepo(p.repo);
    if (
      typeof p.event_type === "string" &&
      (EVENT_TYPES as readonly string[]).includes(p.event_type)
    ) {
      setEventType(p.event_type as EventType);
    }
    if (typeof p.author === "string") setAuthor(p.author);
    if (typeof p.title === "string") setTitle(p.title);
  }

  function onSend() {
    void send("github:event", {
      repo,
      event_type: eventType,
      author,
      title,
    });
  }

  return (
    <div className="sim-form">
      <label className="sim-label">
        Repo
        <input
          className="sim-input"
          type="text"
          value={repo}
          onChange={(e) => setRepo(e.target.value)}
          placeholder="org/service"
        />
      </label>
      <label className="sim-label">
        Event
        <select
          className="sim-input"
          value={eventType}
          onChange={(e) => setEventType(e.target.value as EventType)}
        >
          {EVENT_TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </label>
      <label className="sim-label">
        Author
        <input
          className="sim-input"
          type="text"
          value={author}
          onChange={(e) => setAuthor(e.target.value)}
        />
      </label>
      <label className="sim-label">
        Title
        <input
          className="sim-input"
          type="text"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
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
