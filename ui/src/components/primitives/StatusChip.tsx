import type { ReactNode } from "react";

export type StatusChipVariant =
  | "trust"
  | "authority"
  | "review"
  | "critical"
  | "evidence"
  | "forecast"
  | "neutral";

export interface StatusChipProps {
  variant?: StatusChipVariant;
  icon?: ReactNode;
  children: ReactNode;
  className?: string;
}

export function StatusChip({
  variant = "neutral",
  icon,
  children,
  className,
}: StatusChipProps) {
  const classes = ["fy-status-chip", `fy-status-chip--${variant}`, className]
    .filter(Boolean)
    .join(" ");
  return (
    <span className={classes}>
      {icon ? <span aria-hidden="true">{icon}</span> : null}
      <span>{children}</span>
    </span>
  );
}

export default StatusChip;
