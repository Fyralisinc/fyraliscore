// Kathmandu-local time conversion per CONTRACTS.md §5. The backend ships
// ISO-8601 UTC on the wire; the UI localizes at display time.
//
// Using Intl rather than a heavy date lib keeps the bundle small.

const TZ = "Asia/Kathmandu";

const METALINE_FMT = new Intl.DateTimeFormat("en-GB", {
  timeZone: TZ,
  weekday: "short",
  day: "2-digit",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const CLOCK_FMT = new Intl.DateTimeFormat("en-GB", {
  timeZone: TZ,
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

export function formatKathmanduMetaLine(iso: string): string {
  try {
    const d = new Date(iso);
    return METALINE_FMT.format(d).replace(",", " ·");
  } catch {
    return iso;
  }
}

export function formatKathmanduClock(iso: string): string {
  try {
    return CLOCK_FMT.format(new Date(iso));
  } catch {
    return iso;
  }
}

export function formatStaleness(seconds: number): string {
  if (seconds < 90) return `recomputed ${Math.max(1, Math.round(seconds))}s ago`;
  const m = Math.round(seconds / 60);
  if (m < 60) return `recomputed ${m}m ago`;
  const h = Math.round(m / 60);
  return `recomputed ${h}h ago`;
}
