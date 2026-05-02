import { useState } from "react";
import type { RoutedCoda as RoutedCodaModel } from "@/api/today-types";

type Props = { coda: RoutedCodaModel };

// Per spec §4.8 — sits below the feed before the ask zone. Collapsible.
// "The other items I'm tracking — none need you."
export function RoutedCoda({ coda }: Props) {
  const [open, setOpen] = useState(false);
  return (
    <button
      className={"routed-coda" + (open ? " expanded" : "")}
      onClick={() => setOpen((v) => !v)}
      type="button"
    >
      <div className="routed-coda-head">
        <div className="routed-coda-text">
          The other <em>{coda.total} items</em> I'm tracking — none need you.
          {" "}I've routed them.
        </div>
        <div className="routed-coda-cta">
          See routing <span aria-hidden="true">{open ? "↑" : "↓"}</span>
        </div>
      </div>
      <div className="routed-coda-detail">
        <div className="routed-coda-detail-inner">
          {coda.rows.map((r, i) => (
            <div className="routed-row" key={i}>
              <strong>{r.recipient}</strong>{" "}
              <span className="count">· {r.count} items ·</span>{" "}
              {r.items}
            </div>
          ))}
        </div>
      </div>
    </button>
  );
}
