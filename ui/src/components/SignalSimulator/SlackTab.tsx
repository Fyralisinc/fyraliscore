import { useEffect, useMemo, useState } from "react";
import type { SuggestedSignalItem } from "@/api/demo-client";
import { SuggestedSignals } from "./SuggestedSignals";
import type { SendFn, SendStatus } from "./types";

type Props = {
  suggestions: SuggestedSignalItem[];
  send: SendFn;
  status: SendStatus;
};

// Special sentinel used by the dropdowns to switch into a free-text
// fallback. Picked over an empty string so it survives stripping.
const CUSTOM = "__custom__";

const FALLBACK_CHANNELS = ["#general", "#sales", "#eng", "#founder-private"];
const FALLBACK_AUTHORS = ["Founder — Jules Park", "Eng — Sarah Chen", "AE — Diego Rivera"];

function uniq(values: (string | undefined)[]): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const v of values) {
    if (!v) continue;
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

export function SlackTab({ suggestions, send, status }: Props) {
  // Build dropdown options from the company's suggested-signal catalog
  // so the operator picks from values that actually exist in the demo
  // data. Fall back to a small canned list when suggestions haven't
  // loaded yet (the simulator is usable before /suggested resolves).
  const channelOptions = useMemo(() => {
    const fromSuggestions = uniq(suggestions.map((s) => s.channel_name));
    return fromSuggestions.length > 0 ? fromSuggestions : FALLBACK_CHANNELS;
  }, [suggestions]);
  const authorOptions = useMemo(() => {
    const fromSuggestions = uniq(suggestions.map((s) => s.author_label));
    return fromSuggestions.length > 0 ? fromSuggestions : FALLBACK_AUTHORS;
  }, [suggestions]);

  const [channel, setChannel] = useState<string>(channelOptions[0] ?? "");
  const [author, setAuthor] = useState<string>(authorOptions[0] ?? "");
  const [channelCustom, setChannelCustom] = useState("");
  const [authorCustom, setAuthorCustom] = useState("");
  const [message, setMessage] = useState("");

  // Keep the selects pinned to a real option whenever the option list
  // changes (e.g. once /suggested resolves and the fallbacks swap out).
  useEffect(() => {
    if (channel === CUSTOM) return;
    if (!channelOptions.includes(channel)) setChannel(channelOptions[0] ?? "");
  }, [channelOptions, channel]);
  useEffect(() => {
    if (author === CUSTOM) return;
    if (!authorOptions.includes(author)) setAuthor(authorOptions[0] ?? "");
  }, [authorOptions, author]);

  useEffect(() => {
    if (status.kind === "sent") setMessage("");
  }, [status]);

  function pick(item: SuggestedSignalItem) {
    // Suggested signals carry top-level keys (channel_name, author_label,
    // text). Snap the dropdowns to the matching option when present;
    // otherwise drop into the custom slot so the value still appears.
    if (item.channel_name) {
      if (channelOptions.includes(item.channel_name)) {
        setChannel(item.channel_name);
      } else {
        setChannel(CUSTOM);
        setChannelCustom(item.channel_name);
      }
    }
    if (item.author_label) {
      if (authorOptions.includes(item.author_label)) {
        setAuthor(item.author_label);
      } else {
        setAuthor(CUSTOM);
        setAuthorCustom(item.author_label);
      }
    }
    if (item.text) setMessage(item.text);
  }

  function onSend() {
    const ch = channel === CUSTOM ? channelCustom.trim() : channel;
    const au = author === CUSTOM ? authorCustom.trim() : author;
    void send("slack:message", { channel: ch, author: au, message });
  }

  return (
    <div className="sim-form">
      <label className="sim-label">
        Channel
        <select
          className="sim-input"
          value={channel}
          onChange={(e) => setChannel(e.target.value)}
        >
          {channelOptions.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
          <option value={CUSTOM}>Other…</option>
        </select>
        {channel === CUSTOM ? (
          <input
            className="sim-input"
            type="text"
            value={channelCustom}
            onChange={(e) => setChannelCustom(e.target.value)}
            placeholder="#custom-channel"
          />
        ) : null}
      </label>
      <label className="sim-label">
        Author
        <select
          className="sim-input"
          value={author}
          onChange={(e) => setAuthor(e.target.value)}
        >
          {authorOptions.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
          <option value={CUSTOM}>Other…</option>
        </select>
        {author === CUSTOM ? (
          <input
            className="sim-input"
            type="text"
            value={authorCustom}
            onChange={(e) => setAuthorCustom(e.target.value)}
            placeholder="Role — Name"
          />
        ) : null}
      </label>
      <label className="sim-label">
        Message
        <textarea
          className="sim-textarea"
          rows={4}
          value={message}
          onChange={(e) => setMessage(e.target.value)}
          placeholder="Linear just asked about SSO too…"
        />
      </label>
      <SendBar onSend={onSend} status={status} />
      <SuggestedSignals items={suggestions} onPick={pick} />
    </div>
  );
}

function SendBar({
  onSend,
  status,
}: {
  onSend: () => void;
  status: SendStatus;
}) {
  return (
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
  );
}

export function SendStatusLine({ status }: { status: SendStatus }) {
  if (status.kind === "idle") return null;
  if (status.kind === "sending") return <span className="sim-status">…</span>;
  if (status.kind === "error")
    return <span className="sim-status warn">{status.message}</span>;
  return (
    <span className="sim-status ok">
      sent {status.deduped ? "(deduped)" : ""}
    </span>
  );
}
