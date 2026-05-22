import type { ConsequenceOp } from "@/api/spec-delta-types";

interface Props {
  ops: ConsequenceOp[];
  title?: string;
}

const OP_LABEL: Record<ConsequenceOp["operation"], string> = {
  create: "Create",
  update: "Update",
  archive: "Archive",
  notify: "Notify",
  reevaluate: "Re-evaluate",
};

// Consequence preview — spec §15.10. Grouped by operation, shown
// alongside the Decision Delta detail.
export function ConsequencePreviewView({ ops, title = "If accepted" }: Props) {
  if (ops.length === 0) return null;
  return (
    <div className="fx-stack" style={{ gap: 6 }}>
      <div className="fx-inspector__section-label">{title}</div>
      <div className="fx-consequence">
        {ops.map((op, i) => (
          <div key={i} className="fx-consequence__item">
            <span className={`fx-consequence__op fx-consequence__op--${op.operation}`}>
              {OP_LABEL[op.operation]}
            </span>
            <span>{op.label}{op.target ? ` (${op.target.label})` : ""}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
