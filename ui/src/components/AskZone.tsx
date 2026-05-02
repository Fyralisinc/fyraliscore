import { forwardRef, useState } from "react";

type Props = {
  suggestions: string[];
  onAsk: (query: string) => void;
  sending?: boolean;
};

// Per spec §4.9 — the escape hatch. Always visible at the bottom of the
// feed. When the recommendation feed doesn't have what the user needs,
// they ask directly. Suggestions chips below.
export const AskZone = forwardRef<HTMLInputElement, Props>(function AskZone(
  { suggestions, onAsk, sending },
  ref
) {
  const [value, setValue] = useState("");

  function submit(text: string) {
    const trimmed = text.trim();
    if (!trimmed || sending) return;
    onAsk(trimmed);
    setValue("");
  }

  return (
    <section className="ask-zone" aria-label="Ask anything">
      <div className="ask-zone-label">Ask anything</div>
      <input
        ref={ref}
        className="ask"
        type="text"
        placeholder="What did we decide about pricing in Q4? Tell me about Northwind…"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter") submit(value);
        }}
        disabled={sending}
      />
      <div className="suggestions">
        {suggestions.map((s) => (
          <button
            key={s}
            className="suggestion"
            type="button"
            onClick={() => submit(s)}
          >
            {s}
          </button>
        ))}
      </div>
    </section>
  );
});
