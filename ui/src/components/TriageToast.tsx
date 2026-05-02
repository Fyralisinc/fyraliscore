import { useEffect, useState } from "react";
import type { TriageToast as TriageToastModel } from "@/hooks/useToday";

type Props = {
  toast: TriageToastModel | null;
  onDismiss: () => void;
};

// Brief bottom-center confirmation that fires after every triage. Tells
// the user "Reaffirmed — <card headline>" so the click doesn't feel
// silent. Auto-fades; user can also click X to clear.
export function TriageToast({ toast, onDismiss }: Props) {
  const [phase, setPhase] = useState<"in" | "out">("in");

  useEffect(() => {
    if (!toast) return;
    setPhase("in");
    const fade = window.setTimeout(() => setPhase("out"), 4000);
    return () => window.clearTimeout(fade);
  }, [toast?.id]);

  if (!toast) return null;

  return (
    <div
      className={"triage-toast " + phase + " kind-" + toast.kind}
      role="status"
      aria-live="polite"
      data-testid="triage-toast"
    >
      <div className="triage-toast-headline">{toast.headline}</div>
      {toast.detail ? (
        <div className="triage-toast-detail">{toast.detail}</div>
      ) : null}
      <button
        className="triage-toast-close"
        type="button"
        onClick={onDismiss}
        aria-label="Dismiss notification"
      >
        ×
      </button>
    </div>
  );
}
