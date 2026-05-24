import { useMemo } from "react";
import type { LedgerChainCard } from "@/api/ledger-v2-types";
import { ChainCard } from "./ChainCard";

interface Props {
  chains: LedgerChainCard[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  now?: Date;
}

// Memory River — chronological list of chain cards, grouped by date
// bucket (Today, Yesterday, exact dates further back). The river is
// the primary surface in Timeline mode.
export function MemoryRiver({ chains, selectedId, onSelect, now }: Props) {
  const reference = now ?? new Date();
  const groups = useMemo(() => groupChains(chains, reference), [chains, reference]);

  return (
    <section className="lg-river" aria-label="Memory river">
      {groups.map((g) => (
        <div className="lg-river__group" key={g.key}>
          <div className="lg-river__group-label">{g.label}</div>
          <ul className="lg-river__list">
            {g.chains.map((c) => (
              <li key={c.id}>
                <ChainCard
                  chain={c}
                  selected={c.id === selectedId}
                  onSelect={onSelect}
                  now={reference}
                />
              </li>
            ))}
          </ul>
        </div>
      ))}
      <a className="lg-link lg-river__more" href="#older">
        View older chains →
      </a>
    </section>
  );
}

interface Group {
  key: string;
  label: string;
  chains: LedgerChainCard[];
}

function groupChains(chains: LedgerChainCard[], ref: Date): Group[] {
  const sorted = [...chains].sort((a, b) => bucketTime(b) - bucketTime(a));
  const byKey: Record<string, Group> = {};
  const order: string[] = [];

  for (const c of sorted) {
    const ts = new Date(bucketTime(c));
    const { key, label } = bucketKey(ts, ref);
    if (!byKey[key]) {
      byKey[key] = { key, label, chains: [] };
      order.push(key);
    }
    byKey[key].chains.push(c);
  }
  return order.map((k) => byKey[k]);
}

function bucketTime(c: LedgerChainCard): number {
  const ts = c.resolvedAt ?? c.startedAt;
  return new Date(ts).getTime();
}

function bucketKey(d: Date, ref: Date): { key: string; label: string } {
  const day = startOfDay(d);
  const today = startOfDay(ref);
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  if (day.getTime() === today.getTime())
    return { key: "today", label: "Today" };
  if (day.getTime() === yesterday.getTime())
    return { key: "yesterday", label: "Yesterday" };
  const label = d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
  return { key: `d-${day.toISOString()}`, label };
}

function startOfDay(d: Date): Date {
  const x = new Date(d);
  x.setHours(0, 0, 0, 0);
  return x;
}
