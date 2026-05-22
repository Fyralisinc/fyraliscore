// Visual contract for a node tile inside the layered graph. Rendered
// as a foreignObject child of the SVG so styling can use the same CSS
// system as the rest of the app — easier than cytoscape labels, and
// keyboard/focus behaviour comes for free.

import type { MapBand, MapNode } from "@/api/map-types";
import type { NodeMetaV2 } from "@/api/map-mock-v2";
import { BAND_TYPE_LABELS } from "./types";

export interface NodeTileProps {
  node: MapNode;
  meta?: NodeMetaV2;
  selected: boolean;
  dimmed: boolean;
  onClick: (id: string) => void;
}

function formatArr(usd: number | null): string | null {
  if (usd === null) return null;
  if (usd >= 1_000_000) return `$${(usd / 1_000_000).toFixed(2)}M ARR`;
  if (usd >= 1_000) return `$${Math.round(usd / 1_000)}K ARR`;
  return `$${usd} ARR`;
}

function tileModifiers(node: MapNode, meta?: NodeMetaV2): string {
  const m: string[] = [`fy-node-tile--${node.band ?? "unknown"}`];
  if (meta?.critical) m.push("fy-node-tile--critical");
  if (meta?.awaiting_confirmation) m.push("fy-node-tile--awaiting");
  if (node.status === "Unassigned" || meta?.status_label === "Unassigned")
    m.push("fy-node-tile--unassigned");
  if (node.health === "contested") m.push("fy-node-tile--contested");
  return m.join(" ");
}

function metaLine(node: MapNode, meta?: NodeMetaV2): string | null {
  if (!meta) return null;
  if (node.band === "customer" && meta.arr !== null) {
    return formatArr(meta.arr);
  }
  if (meta.status_label) return meta.status_label;
  if (meta.critical) return "Critical risk";
  if (meta.awaiting_confirmation) return "Open question";
  if (node.band === "goal") return "Primary company goal";
  if (meta.owner) return `Owner: ${meta.owner}`;
  return null;
}

export function NodeTile({
  node,
  meta,
  selected,
  dimmed,
  onClick,
}: NodeTileProps) {
  const cls = [
    "fy-node-tile",
    tileModifiers(node, meta),
    selected ? "is-selected" : "",
    dimmed ? "is-dimmed" : "",
  ]
    .filter(Boolean)
    .join(" ");
  const sub = metaLine(node, meta);
  const typeLabel = BAND_TYPE_LABELS[node.band ?? ("goal" as MapBand)];

  return (
    <button
      type="button"
      className={cls}
      onClick={() => onClick(node.id)}
      data-node-id={node.id}
      data-band={node.band}
      data-selected={selected ? "true" : "false"}
      aria-pressed={selected}
      data-testid={`node-${node.id}`}
    >
      <span className="fy-node-tile__type">{typeLabel}</span>
      <span className="fy-node-tile__title">{node.natural}</span>
      {sub ? <span className="fy-node-tile__sub">{sub}</span> : null}
      {meta?.critical ? (
        <span className="fy-node-tile__badge fy-node-tile__badge--critical">
          CRITICAL
        </span>
      ) : null}
    </button>
  );
}

export default NodeTile;
