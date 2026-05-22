import type { OperatingThreadStatus } from "@/api/operating-thread-types";

const STATUS_LABEL: Record<OperatingThreadStatus, string> = {
  healthy: "Healthy",
  watch: "Watch",
  under_pressure: "Under Pressure",
  needs_review: "Needs Review",
  critical: "Critical",
  stale: "Stale",
  contested: "Contested",
  monitoring: "Monitoring",
  resolved: "Resolved",
};

interface Props {
  status: OperatingThreadStatus;
  className?: string;
}

export function StatusPill({ status, className }: Props) {
  const cls = `fx-pill fx-pill--${status.replace("_", "-")}${className ? ` ${className}` : ""}`;
  return <span className={cls}>{STATUS_LABEL[status]}</span>;
}

export function statusRailClass(status: OperatingThreadStatus): string {
  return `fx-rail fx-rail--${status.replace("_", "-")}`;
}
