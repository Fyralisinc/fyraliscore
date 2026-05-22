// Bottom-right line-style legend. Three styles per spec §4.4:
// supports (solid Moss Cipher), depends-on (dashed Stone Veil), and
// blocks/inhibits (solid Deep Garnet).

export function GraphLegend() {
  return (
    <div
      className="fy-model-legend"
      role="group"
      aria-label="Edge legend"
      data-testid="graph-legend"
    >
      <LegendRow color="var(--color-moss-cipher)" dash={null} label="Supports" />
      <LegendRow color="var(--color-weathered-sage)" dash="4 3" label="Depends on" />
      <LegendRow color="var(--color-deep-garnet)" dash={null} label="Blocks / inhibits" />
    </div>
  );
}

function LegendRow({
  color,
  dash,
  label,
}: {
  color: string;
  dash: string | null;
  label: string;
}) {
  return (
    <div className="fy-model-legend__row">
      <svg width="32" height="6" viewBox="0 0 32 6" aria-hidden="true">
        <line
          x1="0"
          y1="3"
          x2="32"
          y2="3"
          stroke={color}
          strokeWidth="1.6"
          strokeDasharray={dash ?? undefined}
        />
      </svg>
      <span>{label}</span>
    </div>
  );
}

export default GraphLegend;
