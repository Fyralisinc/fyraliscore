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

export function CustomTab({ suggestions, send, status }: Props) {
  const [channel, setChannel] = useState("");
  const [json, setJson] = useState("{\n  \n}");
  const [parseError, setParseError] = useState<string | null>(null);

  function pick(item: SuggestedSignalItem) {
    if (typeof item.channel === "string") setChannel(item.channel);
    setJson(JSON.stringify(item.payload, null, 2));
    setParseError(null);
  }

  function onSend() {
    setParseError(null);
    let parsed: unknown;
    try {
      parsed = JSON.parse(json);
    } catch (err) {
      setParseError(err instanceof Error ? err.message : "invalid JSON");
      return;
    }
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed)
    ) {
      setParseError("payload must be a JSON object");
      return;
    }
    void send(channel, parsed as Record<string, unknown>);
  }

  return (
    <div className="sim-form">
      <label className="sim-label">
        Channel
        <input
          className="sim-input"
          type="text"
          value={channel}
          onChange={(e) => setChannel(e.target.value)}
          placeholder="slack:message"
        />
      </label>
      <label className="sim-label">
        Payload (JSON)
        <textarea
          className="sim-textarea sim-mono"
          rows={10}
          value={json}
          onChange={(e) => setJson(e.target.value)}
        />
      </label>
      {parseError ? (
        <div className="sim-status warn">{parseError}</div>
      ) : null}
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
