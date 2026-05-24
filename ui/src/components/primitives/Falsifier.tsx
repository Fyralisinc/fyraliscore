export interface FalsifierProps {
  condition: string;
  label?: string;
}

export function Falsifier({
  condition,
  label = "Falsification condition",
}: FalsifierProps) {
  return (
    <div className="fy-falsifier" role="note" aria-label={label}>
      <svg
        className="fy-falsifier__icon"
        viewBox="0 0 18 18"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.4"
        aria-hidden="true"
      >
        <circle cx="9" cy="9" r="7" />
        <path d="M9 5v4.5" />
        <circle cx="9" cy="12.4" r="0.6" fill="currentColor" stroke="none" />
      </svg>
      <div className="fy-falsifier__body">
        <span className="fy-falsifier__label">{label}</span>
        <span className="fy-falsifier__text">{condition}</span>
      </div>
    </div>
  );
}

export default Falsifier;
