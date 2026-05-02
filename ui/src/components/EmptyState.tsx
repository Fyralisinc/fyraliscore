type Props = {
  headline?: string;
  body?: string;
};

// Per spec §4.7 — inbox-zero is a real moment of completion.
export function EmptyState({
  headline = "You're at zero.",
  body = "Nothing else needs your attention today. I'll surface again if anything material changes.",
}: Props) {
  return (
    <div className="empty-state visible" role="status">
      <div className="empty-state-icon" aria-hidden="true">
        <svg width="22" height="22" viewBox="0 0 22 22" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M5 11l4 4 8-8" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>
      <div className="empty-state-headline">{headline}</div>
      <div className="empty-state-body">{body}</div>
    </div>
  );
}
