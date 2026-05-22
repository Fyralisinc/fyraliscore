export type ConfidenceTier = "low" | "moderate" | "high";

export interface ConfidenceProps {
  value: number;
  basis: string;
  tier?: ConfidenceTier;
  showNumber?: boolean;
}

function tierFromValue(value: number): ConfidenceTier {
  if (value >= 0.8) return "high";
  if (value >= 0.6) return "moderate";
  return "low";
}

const tierLabel: Record<ConfidenceTier, string> = {
  low: "Low",
  moderate: "Moderate",
  high: "High",
};

export function Confidence({
  value,
  basis,
  tier,
  showNumber = true,
}: ConfidenceProps) {
  const resolvedTier = tier ?? tierFromValue(value);
  const percent = Math.round(value * 100);
  return (
    <div className="fy-confidence" role="group" aria-label="Confidence">
      {showNumber ? (
        <span className="fy-confidence__value">{percent}%</span>
      ) : null}
      <span className="fy-confidence__tier">{tierLabel[resolvedTier]}</span>
      <span className="fy-confidence__basis">{basis}</span>
    </div>
  );
}

export default Confidence;
