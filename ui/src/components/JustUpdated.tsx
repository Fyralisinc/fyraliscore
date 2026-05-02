import { useEffect, useState } from "react";

type Props = {
  text_html: string;
  onDismiss: () => void;
};

// Per spec §3.4 — appears when something has changed since last visit.
// Auto-dismisses after 14s with a fade. User can manually close.
export function JustUpdated({ text_html, onDismiss }: Props) {
  const [dismissing, setDismissing] = useState(false);

  useEffect(() => {
    const fadeTimer = window.setTimeout(() => setDismissing(true), 13_000);
    const removeTimer = window.setTimeout(() => onDismiss(), 14_000);
    return () => {
      window.clearTimeout(fadeTimer);
      window.clearTimeout(removeTimer);
    };
  }, [onDismiss]);

  return (
    <div
      className={"just-updated" + (dismissing ? " dismissing" : "")}
      role="status"
      aria-live="polite"
    >
      <span dangerouslySetInnerHTML={{ __html: text_html }} />
      <span
        className="x"
        onClick={() => {
          setDismissing(true);
          window.setTimeout(onDismiss, 200);
        }}
        role="button"
        aria-label="Dismiss notification"
      >
        ×
      </span>
    </div>
  );
}
