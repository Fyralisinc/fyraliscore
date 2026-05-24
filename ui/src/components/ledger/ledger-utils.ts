// Date / time helpers used across the Ledger surface. Pure functions —
// no React, no DOM.

import type { LedgerEvent } from "@/api/history-types";

const MONTHS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

export function formatTime(iso: string): string {
  const d = new Date(iso);
  let h = d.getUTCHours();
  const m = d.getUTCMinutes();
  const am = h < 12;
  if (h === 0) h = 12;
  else if (h > 12) h -= 12;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")} ${am ? "AM" : "PM"}`;
}

export function formatDateKey(iso: string): string {
  const d = new Date(iso);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}

export function formatDateHeader(
  iso: string,
  todayKey?: string,
  yesterdayKey?: string
): string {
  const d = new Date(iso);
  const key = formatDateKey(iso);
  const month = MONTHS[d.getUTCMonth()];
  const day = d.getUTCDate();
  if (todayKey && key === todayKey) return `Today · ${month} ${day}`;
  if (yesterdayKey && key === yesterdayKey) return `Yesterday · ${month} ${day}`;
  return `${month} ${day}`;
}

export function formatLongDate(iso: string): string {
  const d = new Date(iso);
  const month = MONTHS[d.getUTCMonth()];
  return `${month} ${d.getUTCDate()}, ${d.getUTCFullYear()}`;
}

export function formatLongDateAtTime(iso: string, todayKey?: string): string {
  const key = formatDateKey(iso);
  const time = formatTime(iso);
  if (todayKey && key === todayKey) return `Today at ${time}`;
  return `${formatLongDate(iso)} at ${time}`;
}

export type EventGroup = { dateKey: string; iso: string; events: LedgerEvent[] };

export function groupByDay(events: LedgerEvent[]): EventGroup[] {
  const map = new Map<string, EventGroup>();
  for (const e of events) {
    const key = formatDateKey(e.timestamp);
    const g = map.get(key);
    if (g) g.events.push(e);
    else map.set(key, { dateKey: key, iso: e.timestamp, events: [e] });
  }
  // Newest first
  return [...map.values()].sort((a, b) =>
    a.dateKey < b.dateKey ? 1 : a.dateKey > b.dateKey ? -1 : 0
  );
}

export function todayKey(now?: Date): string {
  const d = now ?? new Date();
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}

export function yesterdayKey(now?: Date): string {
  const d = new Date((now ?? new Date()).getTime());
  d.setUTCDate(d.getUTCDate() - 1);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
}
