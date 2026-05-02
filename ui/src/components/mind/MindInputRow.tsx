import { forwardRef, useState } from "react";

// Spec Part 5 — input row. Substrate-as-parser model. Press N to focus.
type Props = {
  onSubmit: (text: string) => void;
  onOpenFilter: () => void;
  onSearchChange: (text: string) => void;
  searchValue: string;
  filterActive?: boolean;
};

export const MindInputRow = forwardRef<HTMLInputElement, Props>(function MindInputRow(
  { onSubmit, onOpenFilter, onSearchChange, searchValue, filterActive },
  ref
) {
  const [text, setText] = useState("");
  const [parsing, setParsing] = useState(false);

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = text.trim();
    if (!trimmed || parsing) return;
    setParsing(true);
    // Simulate the 200-400ms substrate parsing latency described in spec §5.2.
    window.setTimeout(() => {
      onSubmit(trimmed);
      setText("");
      setParsing(false);
      // Refocus so the user can keep adding rapidly.
      const node = (ref as React.MutableRefObject<HTMLInputElement | null>)?.current;
      node?.focus();
    }, 240);
  }

  return (
    <form className="input-row mind-input-row" onSubmit={handleSubmit} aria-label="Add to My Mind">
      <input
        ref={ref}
        type="text"
        className={"mind-input" + (parsing ? " parsing" : "")}
        placeholder={parsing ? "Parsing…" : "What's on your mind?"}
        autoComplete="off"
        spellCheck={true}
        value={text}
        onChange={(e) => setText(e.target.value)}
        disabled={parsing}
        aria-label="Capture a thought"
      />
      <button
        type="button"
        className={"control-toggle filter-toggle" + (filterActive ? " active" : "")}
        onClick={onOpenFilter}
      >
        Filter ▾
      </button>
      <input
        type="search"
        className="search-input mind-search"
        placeholder="Search…"
        value={searchValue}
        onChange={(e) => onSearchChange(e.target.value)}
        aria-label="Search items"
      />
    </form>
  );
});
