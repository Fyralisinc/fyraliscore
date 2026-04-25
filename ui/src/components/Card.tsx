import type { Card as CardT } from "@/api/types";

type Props = {
  card: CardT;
  active: boolean;
  onToggle: () => void;
};

// Three card species share the same container; the `kind` drives
// class-level styling (.obs / .dec / .q-card). Design doc §10.4.
// body_html carries inline spans (.serif-hot, .hl, .n) produced by the
// rendering service.
export function Card({ card, active, onToggle }: Props) {
  const kindClass =
    card.kind === "decision"
      ? "dec"
      : card.kind === "question"
      ? "q-card"
      : "obs";
  const className = ["card", kindClass, active ? "active" : ""]
    .filter(Boolean)
    .join(" ");
  return (
    <button
      type="button"
      className={className}
      data-testid={`card-${card.kind}`}
      data-card-id={card.id}
      aria-expanded={active}
      onClick={onToggle}
    >
      <div className="card-row">
        <span className={`card-tag ${card.tag_color}`}>
          <span className="tag-mark" />
          {card.tag_label}
        </span>
        <span className="card-meta">{card.meta}</span>
      </div>
      {card.kind === "decision" ? (
        // Decision card body contains its own two-col layout (dec-text +
        // dec-chips) — treat the full body_html as the card-content region.
        <div
          className="card-body"
          dangerouslySetInnerHTML={{ __html: card.body_html }}
        />
      ) : card.kind === "question" ? (
        <div dangerouslySetInnerHTML={{ __html: card.body_html }} />
      ) : (
        <p
          className="card-body"
          dangerouslySetInnerHTML={{ __html: card.body_html }}
        />
      )}
    </button>
  );
}
