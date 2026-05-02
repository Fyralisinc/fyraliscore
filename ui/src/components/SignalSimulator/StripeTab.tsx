import { useState } from "react";
import type { SuggestedSignalItem } from "@/api/demo-client";
import { SuggestedSignals } from "./SuggestedSignals";
import { SendStatusLine } from "./SlackTab";
import type { SendFn, SendStatus } from "./types";

const EVENT_TYPES = [
  "payment",
  "payment_failed",
  "subscription_updated",
  "subscription_canceled",
] as const;
type EventType = (typeof EVENT_TYPES)[number];

type Props = {
  suggestions: SuggestedSignalItem[];
  send: SendFn;
  status: SendStatus;
};

export function StripeTab({ suggestions, send, status }: Props) {
  const [eventType, setEventType] = useState<EventType>("payment");
  const [customer, setCustomer] = useState("");
  const [amount, setAmount] = useState(0);

  function pick(item: SuggestedSignalItem) {
    const p = item.payload as Record<string, unknown>;
    if (
      typeof p.event_type === "string" &&
      (EVENT_TYPES as readonly string[]).includes(p.event_type)
    ) {
      setEventType(p.event_type as EventType);
    }
    if (typeof p.customer === "string") setCustomer(p.customer);
    if (typeof p.amount_usd === "number") setAmount(p.amount_usd);
  }

  function onSend() {
    void send("stripe:event", {
      event_type: eventType,
      customer,
      amount_usd: amount,
    });
  }

  return (
    <div className="sim-form">
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
        Customer
        <input
          className="sim-input"
          type="text"
          value={customer}
          onChange={(e) => setCustomer(e.target.value)}
        />
      </label>
      <label className="sim-label">
        Amount (USD)
        <input
          className="sim-input"
          type="number"
          value={amount}
          min={0}
          onChange={(e) => setAmount(Number(e.target.value) || 0)}
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
