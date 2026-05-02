import { useState } from "react";
import type { Reminder } from "./types";

// Spec Part 8 — Reminder card grammar (time + watching variants).
type Props = {
  reminder: Reminder;
  justCreated?: boolean;
  showParseChip?: boolean;
  onMarkDone: () => void;
  onSnooze: (until: Date) => void;
  onStopWatching: () => void;
  onSeeInHistory?: () => void;
  onSendToToday?: () => void;
  onChangeCategory?: (target: "loop" | "note") => void;
};

export function ReminderCard({
  reminder,
  justCreated,
  showParseChip,
  onMarkDone,
  onSnooze,
  onStopWatching,
  onSeeInHistory,
  onSendToToday,
  onChangeCategory,
}: Props) {
  const [snoozeOpen, setSnoozeOpen] = useState(false);

  const isWatching = reminder.trigger_type === "condition";
  const fired = reminder.state === "fired";

  if (isWatching) {
    return (
      <WatchingCard
        reminder={reminder}
        justCreated={!!justCreated}
        showParseChip={!!showParseChip}
        onStopWatching={onStopWatching}
        onSeeInHistory={onSeeInHistory}
        onSendToToday={onSendToToday}
        onChangeCategory={onChangeCategory}
      />
    );
  }

  const dueText = reminder.remind_at
    ? formatDue(reminder.remind_at)
    : "no time set";

  return (
    <article
      className={
        "item reminder" + (justCreated ? " just-created" : "") + (fired ? " is-fired" : "")
      }
      data-id={reminder.id}
      data-trigger="time"
      data-state={reminder.state}
      role="article"
      aria-label={`Reminder: ${reminder.headline}`}
    >
      <header className={"item-header" + (fired ? " fired" : "")}>
        <span className="item-type">REMINDER</span>
        {fired ? (
          <span className="item-status fired">DUE</span>
        ) : (
          <span className="item-meta">due {dueText}</span>
        )}
      </header>

      <div className="item-body">
        <p className="item-headline">{reminder.headline}</p>
        <p className="reminder-context">
          Set on {formatDate(reminder.created)}
          {fired ? ". Due now." : "."}
        </p>
        {showParseChip ? (
          <div className="parse-chip">
            <span className="parse-chip-text">
              ↑ Parsed as a Reminder
              {reminder.remind_at ? ` due ${dueText}` : ""}.
            </span>
            <div className="parse-chip-actions">
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("loop")}
              >
                Change to Loop
              </button>
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("note")}
              >
                Change to Note
              </button>
            </div>
          </div>
        ) : null}
      </div>

      <footer className="item-footer">
        <button type="button" className="item-action" onClick={onStopWatching}>
          Remove
        </button>
        <div className="item-actions-right">
          <div className="snooze-wrap">
            <button
              type="button"
              className="item-action"
              onClick={() => setSnoozeOpen((v) => !v)}
            >
              Snooze
            </button>
            {snoozeOpen ? (
              <div className="snooze-menu" role="menu">
                <button
                  type="button"
                  onClick={() => {
                    onSnooze(plusHours(1));
                    setSnoozeOpen(false);
                  }}
                >
                  1 hour
                </button>
                <button
                  type="button"
                  onClick={() => {
                    onSnooze(tomorrowAt(8));
                    setSnoozeOpen(false);
                  }}
                >
                  Tomorrow
                </button>
                <button
                  type="button"
                  onClick={() => {
                    onSnooze(plusDays(7));
                    setSnoozeOpen(false);
                  }}
                >
                  Next week
                </button>
              </div>
            ) : null}
          </div>
          <button
            type="button"
            className={"item-action" + (fired ? " primary" : "")}
            onClick={onMarkDone}
          >
            {fired ? "Done" : "Mark done"}
          </button>
        </div>
      </footer>
    </article>
  );
}

function WatchingCard({
  reminder,
  justCreated,
  showParseChip,
  onStopWatching,
  onSeeInHistory,
  onSendToToday,
  onChangeCategory,
}: {
  reminder: Reminder;
  justCreated: boolean;
  showParseChip: boolean;
  onStopWatching: () => void;
  onSeeInHistory?: () => void;
  onSendToToday?: () => void;
  onChangeCategory?: (target: "loop" | "note") => void;
}) {
  const signals = reminder.signals ?? [];
  const setOn = formatDate(reminder.created);

  return (
    <article
      className={"item reminder watching" + (justCreated ? " just-created" : "")}
      data-id={reminder.id}
      data-trigger="condition"
      data-state={reminder.state}
      role="article"
      aria-label={`Watching: ${reminder.headline}`}
    >
      <header className="item-header">
        <span className="item-type">WATCHING</span>
        <span className="item-meta">
          {signals.length === 0
            ? "no signals yet"
            : `${signals.length} signals since set`}
        </span>
      </header>

      <div className="item-body">
        <p className="item-headline">{reminder.headline}</p>
        {signals.length === 0 ? (
          <p className="reminder-context">
            Set {setOn}. Substrate has detected no relevant signals since.
          </p>
        ) : null}
        {showParseChip ? (
          <div className="parse-chip">
            <span className="parse-chip-text">↑ Parsed as a Watching reminder.</span>
            <div className="parse-chip-actions">
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("loop")}
              >
                Change to Loop
              </button>
              <button
                type="button"
                className="item-action"
                onClick={() => onChangeCategory?.("note")}
              >
                Change to Note
              </button>
            </div>
          </div>
        ) : null}
      </div>

      {signals.length > 0 ? (
        <div className="item-substrate-context">
          <p className="substrate-evidence">
            ↑ {signals.length} relevant {signals.length === 1 ? "signal" : "signals"} since you set this {setOn}:
          </p>
          <ul className="watching-signals">
            {signals.map((s, i) => (
              <li key={i}>
                <span className="signal-date">{formatDate(s.date)}</span>
                {s.description}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      <footer className="item-footer">
        {signals.length > 0 && onSeeInHistory ? (
          <button type="button" className="item-action" onClick={onSeeInHistory}>
            See in History
          </button>
        ) : (
          <span />
        )}
        <div className="item-actions-right">
          {onSendToToday ? (
            <button type="button" className="item-action" onClick={onSendToToday}>
              Send to Today
            </button>
          ) : null}
          <button type="button" className="item-action" onClick={onStopWatching}>
            Stop watching
          </button>
        </div>
      </footer>
    </article>
  );
}

function formatDate(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString("en-US", { month: "short", day: "numeric" }).toLowerCase();
}

function formatDue(iso: string): string {
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  const now = new Date();
  const diff = d.getTime() - now.getTime();
  const days = Math.floor(diff / 86_400_000);
  if (days < -1) return `${Math.abs(days)} days ago`;
  if (days === -1) return "yesterday";
  if (days === 0) return "today";
  if (days === 1) return "tomorrow";
  if (days < 7) {
    return d.toLocaleString("en-US", { weekday: "long" });
  }
  return d.toLocaleString("en-US", { month: "short", day: "numeric" });
}

function plusHours(h: number): Date {
  const d = new Date();
  d.setHours(d.getHours() + h);
  return d;
}

function plusDays(n: number): Date {
  const d = new Date();
  d.setDate(d.getDate() + n);
  return d;
}

function tomorrowAt(hour: number): Date {
  const d = new Date();
  d.setDate(d.getDate() + 1);
  d.setHours(hour, 0, 0, 0);
  return d;
}
