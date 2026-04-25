// Icon set for query-grid chips. Name space matches CONTRACTS.md §4:
//   why, draft, timeline, pattern, brief, dependency,
//   calendar, person, customer, calibration, question, observation
//
// Each glyph is a 16x16 line-art SVG aligned with the visual register of
// /company-os.html (no fills except where the prototype uses them).
// Unknown icon names fall back to `question` so a future contract bump
// never blanks the grid.

type Props = { name: string; className?: string };

const paths: Record<string, JSX.Element> = {
  why: (
    <>
      <path
        d="M8 2v9M8 14v-1"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
      />
      <circle cx="8" cy="8" r="6.25" stroke="currentColor" strokeWidth="1.2" />
    </>
  ),
  brief: (
    <path
      d="M3 4h10M3 8h10M3 12h6"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
    />
  ),
  draft: (
    <path
      d="M2.5 4l5.5 4 5.5-4M2.5 4v7.5a1 1 0 001 1h9a1 1 0 001-1V4M2.5 4h11"
      stroke="currentColor"
      strokeWidth="1.2"
      strokeLinejoin="round"
    />
  ),
  calibration: (
    <>
      <circle cx="8" cy="8" r="6.25" stroke="currentColor" strokeWidth="1.2" />
      <path
        d="M5.5 8l2 2 3.5-4"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </>
  ),
  pattern: (
    <path d="M8 2v12M2 8h12" stroke="currentColor" strokeWidth="1.2" />
  ),
  observation: (
    <>
      <circle cx="8" cy="8" r="6.25" stroke="currentColor" strokeWidth="1.2" />
      <circle cx="8" cy="8" r="2" fill="currentColor" />
    </>
  ),
  timeline: (
    <>
      <path d="M2 8h12" stroke="currentColor" strokeWidth="1.2" />
      <circle cx="4" cy="8" r="1.3" fill="currentColor" />
      <circle cx="8" cy="8" r="1.3" fill="currentColor" />
      <circle cx="12" cy="8" r="1.3" fill="currentColor" />
    </>
  ),
  dependency: (
    <path
      d="M3 5h5a3 3 0 013 3v3M11 8l2.5 2.5M11 8l-2.5 2.5"
      stroke="currentColor"
      strokeWidth="1.2"
      strokeLinecap="round"
      strokeLinejoin="round"
      fill="none"
    />
  ),
  calendar: (
    <>
      <rect
        x="2.5"
        y="3.5"
        width="11"
        height="10"
        rx="1"
        stroke="currentColor"
        strokeWidth="1.2"
        fill="none"
      />
      <path
        d="M2.5 6.5h11M5 2.5v2M11 2.5v2"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinecap="round"
      />
    </>
  ),
  person: (
    <>
      <circle cx="8" cy="6" r="2.5" stroke="currentColor" strokeWidth="1.2" />
      <path
        d="M3 13.5c.8-2 2.7-3 5-3s4.2 1 5 3"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinecap="round"
        fill="none"
      />
    </>
  ),
  customer: (
    <>
      <path
        d="M3 6h10l-1 7H4z"
        stroke="currentColor"
        strokeWidth="1.2"
        fill="none"
      />
      <path
        d="M6 6V4a2 2 0 014 0v2"
        stroke="currentColor"
        strokeWidth="1.2"
        fill="none"
      />
    </>
  ),
  question: (
    <>
      <circle cx="8" cy="8" r="6.25" stroke="currentColor" strokeWidth="1.2" />
      <path
        d="M6 6.5a2 2 0 014 0c0 1.5-2 1.5-2 3M8 11.5v.01"
        stroke="currentColor"
        strokeWidth="1.2"
        strokeLinecap="round"
        fill="none"
      />
    </>
  ),
};

export function Icon({ name, className }: Props) {
  const body = paths[name] ?? paths.question;
  return (
    <svg
      viewBox="0 0 16 16"
      fill="none"
      className={className}
      aria-hidden="true"
    >
      {body}
    </svg>
  );
}
