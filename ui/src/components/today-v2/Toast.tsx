// Tiny success/error toast for the Today page. Spec §17.3 — no
// celebratory animation, just a green/red confirmation strip.

interface Props {
  text: string;
  kind?: "success" | "error" | "info";
  onDismiss?: () => void;
}

export function Toast({ text, kind = "info", onDismiss }: Props) {
  return (
    <div
      className={`tdv2-toast tdv2-toast--${kind}`}
      role="status"
      data-testid="today-toast"
      onClick={onDismiss}
    >
      {text}
    </div>
  );
}
