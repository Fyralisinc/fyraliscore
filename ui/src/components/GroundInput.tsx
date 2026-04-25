import { forwardRef, useEffect, useRef, useState } from "react";

type Props = {
  onSubmit: (query: string) => void;
};

// Always-visible bottom input. `/` focuses from anywhere, ↵ submits,
// value is cleared on submit. Design doc §10.6 / §11. The `forwardRef`
// API lets App pin the DOM node so Follow-up verbs can refocus it.
export const GroundInput = forwardRef<HTMLInputElement, Props>(
  function GroundInput({ onSubmit }, externalRef) {
    const localRef = useRef<HTMLInputElement | null>(null);
    const [value, setValue] = useState("");

    // Register both refs on mount so both the parent and internal state
    // can reach the node without collisions.
    useEffect(() => {
      if (!externalRef) return;
      if (typeof externalRef === "function") externalRef(localRef.current);
      else externalRef.current = localRef.current;
    }, [externalRef]);

    return (
      <footer className="ground">
        <div className="ground-inner">
          <span className="ground-mark">?</span>
          <input
            ref={localRef}
            id="ground-input"
            className="ground-input"
            type="text"
            placeholder="Ask anything else"
            autoComplete="off"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && value.trim()) {
                e.preventDefault();
                onSubmit(value.trim());
                setValue("");
              }
            }}
          />
          <span className="ground-kbd">
            <kbd>/</kbd>
            <kbd>↵</kbd>
          </span>
        </div>
      </footer>
    );
  }
);
