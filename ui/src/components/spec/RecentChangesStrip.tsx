import { useFyralisStore } from "@/lib/store";

interface Props {
  onSelectThread?: (id: string) => void;
}

// Recent model changes strip — spec §12.11. Sits beneath the thread
// board (or in the inspector) so Model feels alive without flashing.
export function RecentChangesStrip({ onSelectThread }: Props) {
  const recent = useFyralisStore((s) => s.recentChanges);
  if (recent.length === 0) return null;
  return (
    <aside className="fx-recent" aria-label="Recent model changes">
      <div className="fx-recent__label">Recent model changes</div>
      {recent.slice(0, 5).map((r) => (
        <div
          key={r.id}
          className="fx-recent__item"
          onClick={() => r.threadId && onSelectThread?.(r.threadId)}
          role={r.threadId ? "button" : undefined}
          tabIndex={r.threadId ? 0 : -1}
        >
          <span className="fx-recent__item-time">{formatHM(r.occurredAt)}</span>
          <span className="fx-recent__item-body">{r.summary}</span>
        </div>
      ))}
    </aside>
  );
}

function formatHM(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
  } catch {
    return iso;
  }
}
