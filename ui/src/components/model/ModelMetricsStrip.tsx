// Six-chip metric strip across the top of the Model page (spec §4.1).
// Thin-icon visual treatment, neutral chip surface, semantic accent for
// At-risk ARR only — the rest are calm system counters.

export interface ModelMetricsStripProps {
  activeNodes: number;
  changedToday: number;
  contested: number;
  awaitingConfirmation: number;
  blockedCommitments: number;
  atRiskArrUsd: number;
}

function formatArr(usd: number): string {
  if (usd >= 1_000_000) return `$${(usd / 1_000_000).toFixed(2)}M`;
  if (usd >= 1_000) return `$${Math.round(usd / 1_000)}K`;
  return `$${usd}`;
}

interface ChipProps {
  icon: React.ReactNode;
  label: string;
  value: string;
  variant?: "critical" | "review" | "neutral";
  testId: string;
}

function Chip({ icon, label, value, variant = "neutral", testId }: ChipProps) {
  return (
    <div
      className={`fy-model-metric fy-model-metric--${variant}`}
      data-testid={testId}
    >
      <span className="fy-model-metric__icon" aria-hidden="true">
        {icon}
      </span>
      <span className="fy-model-metric__label">{label}</span>
      <span className="fy-model-metric__value">{value}</span>
    </div>
  );
}

export function ModelMetricsStrip(props: ModelMetricsStripProps) {
  return (
    <div className="fy-model-metrics" role="group" aria-label="Model metrics">
      <Chip
        testId="metric-active-nodes"
        icon={<IconNodes />}
        label="Active Nodes"
        value={String(props.activeNodes)}
      />
      <Chip
        testId="metric-changed-today"
        icon={<IconChange />}
        label="Changed today"
        value={String(props.changedToday)}
      />
      <Chip
        testId="metric-contested"
        icon={<IconContested />}
        label="Contested"
        value={String(props.contested)}
        variant="review"
      />
      <Chip
        testId="metric-awaiting"
        icon={<IconAwaiting />}
        label="Awaiting confirmation"
        value={String(props.awaitingConfirmation)}
      />
      <Chip
        testId="metric-blocked"
        icon={<IconBlocked />}
        label="Blocked commitments"
        value={String(props.blockedCommitments)}
      />
      <Chip
        testId="metric-arr-at-risk"
        icon={<IconArr />}
        label="At-risk ARR"
        value={formatArr(props.atRiskArrUsd)}
        variant="critical"
      />
    </div>
  );
}

function IconNodes() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.2">
      <circle cx="4" cy="4" r="1.6" />
      <circle cx="12" cy="4" r="1.6" />
      <circle cx="8" cy="12" r="1.6" />
      <path d="M5.4 4.7 10.6 4.7M4.6 5.5 7.3 10.5M11.4 5.5 8.7 10.5" />
    </svg>
  );
}
function IconChange() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.2">
      <path d="M3 6.5C4 4.5 6 3.5 8 3.5c2.5 0 4.5 1.5 5 4M13 9.5C12 11.5 10 12.5 8 12.5c-2.5 0-4.5-1.5-5-4" />
      <path d="M13 3.5v3h-3M3 12.5v-3h3" />
    </svg>
  );
}
function IconContested() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.2">
      <circle cx="8" cy="8" r="5.5" />
      <path d="M5 5l6 6M11 5l-6 6" />
    </svg>
  );
}
function IconAwaiting() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.2">
      <circle cx="8" cy="8" r="5.5" />
      <path d="M8 5v3l2 1.5" />
    </svg>
  );
}
function IconBlocked() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.2">
      <circle cx="8" cy="8" r="5.5" />
      <path d="M4 4l8 8" />
    </svg>
  );
}
function IconArr() {
  return (
    <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.2">
      <path d="M8 2v12M5 5h4a1.5 1.5 0 010 3H6a1.5 1.5 0 000 3h5" />
    </svg>
  );
}

export default ModelMetricsStrip;
