// Arrow marker definitions shared across all canvas surfaces.
//
// Mounted once at the page level so any canvas component (OverviewMap,
// RelationshipCorridor, TracePath, NodeNeighborhood) can reference the
// `fm-arrow-${colorToken}` id from a path's `markerEnd`.
//
// `markerUnits="userSpaceOnUse"` decouples marker size from stroke
// width so low-strength edges still show a legible arrow head.

export function ArrowDefs() {
  const tokens = [
    "moss",
    "lapis",
    "iris",
    "garnet",
    "teal",
    "ochre",
    "blue",
    "gold",
    "coral",
    "sage",
  ] as const;
  return (
    <svg width="0" height="0" style={{ position: "absolute" }} aria-hidden="true">
      <defs>
        {tokens.map((t) => (
          <marker
            key={t}
            id={`fm-arrow-${t}`}
            viewBox="0 0 12 12"
            refX="10"
            refY="6"
            markerWidth="11"
            markerHeight="11"
            markerUnits="userSpaceOnUse"
            orient="auto-start-reverse"
          >
            <path d="M0,1 L11,6 L0,11 z" className={`fm-arrow fm-arrow--${t}`} />
          </marker>
        ))}
      </defs>
    </svg>
  );
}
