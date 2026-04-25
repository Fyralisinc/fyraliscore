import type { CloseLine as CloseLineT } from "@/api/types";

type Props = { closeLine: CloseLineT };

// Single horizontal strip below the cards — the release sentence and
// three mono metadata items. Design doc §10.5.
export function CloseLine({ closeLine }: Props) {
  const { signal_count, external_moves, calibration_pct } = closeLine.metadata;
  // The body is already a single sentence; we keep the "You can go."
  // phrase bold by splitting on its fixed anchor — a tiny concession to
  // presentation that matches the prototype without client-side
  // composition.
  const body = closeLine.body;
  const marker = "You can go.";
  let prefix = body;
  let boldTail = "";
  const idx = body.indexOf(marker);
  if (idx >= 0) {
    prefix = body.slice(0, idx).trimEnd() + " ";
    boldTail = body.slice(idx);
  }
  return (
    <div className="close-line" data-testid="close-line">
      <span className="c-left">
        {prefix}
        {boldTail ? <b>{boldTail}</b> : null}
      </span>
      <span className="c-right">
        <span>{signal_count.toLocaleString()} signals</span>
        <span>{external_moves} external moves</span>
        <span className="calib">{calibration_pct}% calibrated</span>
      </span>
    </div>
  );
}
