import type { Card as CardT, CardVerb } from "@/api/types";

type Props = {
  card: CardT;
  open: boolean;
  onVerb: (verb: CardVerb) => void;
  onClose: () => void;
};

// Inline drawer beneath a card. Renders reasoning_html + any evidence
// blocks as trace ledgers + the card's verbs. Design doc §10.4 +
// /company-os.html lines 928-950 / 968-995 / 1010-1030.
export function CardExpanded({ card, open, onVerb, onClose }: Props) {
  const headTone = card.tag_color === "hot" ? "h hot" : "h";
  const headLabel =
    card.kind === "observation"
      ? "Reasoning · evidence · model · resource"
      : card.kind === "decision"
      ? "Two paths · drafts ready"
      : "Why I'm asking";
  return (
    <div
      className={open ? "expanded open" : "expanded"}
      data-testid={`expanded-${card.id}`}
      aria-hidden={!open}
    >
      <div className="inner">
        <div className="inner-head">
          <span className={headTone}>{headLabel}</span>
          <button
            type="button"
            className="esc"
            onClick={(e) => {
              e.stopPropagation();
              onClose();
            }}
            aria-label="close"
          >
            esc
          </button>
        </div>
        <div
          dangerouslySetInnerHTML={{ __html: card.expanded.reasoning_html }}
        />
        {card.expanded.evidence.map((ev, i) => (
          <div className="trace" key={i}>
            <div className="t-head">
              <span>{ev.label}</span>
              <span />
            </div>
            <div dangerouslySetInnerHTML={{ __html: ev.body_html }} />
          </div>
        ))}
        {card.expanded.verbs.length > 0 ? (
          <div className="verbs">
            {card.expanded.verbs.map((v) => (
              <button
                key={v.id}
                type="button"
                className={v.primary ? "verb primary" : "verb"}
                onClick={(e) => {
                  e.stopPropagation();
                  onVerb(v);
                }}
              >
                {v.label}
              </button>
            ))}
          </div>
        ) : null}
      </div>
    </div>
  );
}
