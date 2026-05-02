type Props = {
  query: string;
};

// Skeleton placeholder rendered between Ask submission and the LLM
// response landing. The substrate ask call is multi-second; without a
// visible loading state the user thinks nothing happened (the bug we
// just fixed at the API layer would have stayed invisible at the UX
// layer too). Three pulsing bars echo the eventual prose layout so the
// answer doesn't visually jump when it arrives.
export function ThinkingTurn({ query }: Props) {
  return (
    <div className="turn thinking" data-testid="thinking-turn">
      <div className="turn-q">{query}</div>
      <div className="turn-a">
        <div className="thinking-status" aria-live="polite">
          <span className="thinking-dot" />
          <span className="thinking-dot" />
          <span className="thinking-dot" />
          <span className="thinking-label">Thinking through the substrate…</span>
        </div>
        <div className="skeleton-line skeleton-line-long" />
        <div className="skeleton-line skeleton-line-mid" />
        <div className="skeleton-line skeleton-line-short" />
      </div>
    </div>
  );
}
