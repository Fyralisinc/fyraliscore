import type { ReactNode } from "react";

export interface SummaryStripCell {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  icon?: ReactNode;
}

export interface SummaryStripProps {
  cells: SummaryStripCell[];
  className?: string;
}

export function SummaryStrip({ cells, className }: SummaryStripProps) {
  const classes = ["fy-summary-strip", className].filter(Boolean).join(" ");
  return (
    <div className={classes} role="group" aria-label="Summary">
      {cells.map((cell, idx) => (
        <div className="fy-summary-cell" key={idx}>
          <div className="fy-summary-cell__label">
            {cell.icon ? <span aria-hidden="true">{cell.icon}</span> : null}
            <span>{cell.label}</span>
          </div>
          <div className="fy-summary-cell__value">{cell.value}</div>
          {cell.sub ? (
            <div className="fy-summary-cell__sub">{cell.sub}</div>
          ) : null}
        </div>
      ))}
    </div>
  );
}

export default SummaryStrip;
