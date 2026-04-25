import { useMemo } from "react";

export function JsonView({ value }: { value: unknown }) {
  const text = useMemo(() => {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }, [value]);
  return <pre className="json-box">{text}</pre>;
}
