import type { LedgerEvent } from "@/api/history-types";
import { RightInspector } from "@/components/primitives";
import { typeMeta } from "./event-taxonomy";
import { formatLongDateAtTime, todayKey } from "./ledger-utils";
import { EvidenceMiniGrid } from "./EvidenceMiniGrid";
import { MiniTimeline } from "./MiniTimeline";

export interface LedgerInspectorProps {
  event: LedgerEvent;
  onClose: () => void;
  onBack?: () => void;
  onForward?: () => void;
  canBack?: boolean;
  canForward?: boolean;
  // anchor "now" for the "Today at HH:MM AM" label so fixture-driven
  // tests render deterministically.
  now?: Date;
}

export function LedgerInspector({
  event,
  onClose,
  onBack,
  onForward,
  canBack = false,
  canForward = false,
  now,
}: LedgerInspectorProps) {
  const meta = typeMeta(event.type);
  const tKey = now
    ? todayKey(now)
    : (() => {
        const d = new Date(event.timestamp);
        return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, "0")}-${String(d.getUTCDate()).padStart(2, "0")}`;
      })();

  const actorLabel =
    event.actor.kind === "person"
      ? event.actor.role
        ? `${event.actor.name} / ${event.actor.role}`
        : event.actor.name
      : event.actor.name;

  const classification = (
    <span
      className={
        "fy-ledger__inspector-class fy-ledger__inspector-class--" +
        meta.className
      }
      data-testid="ledger-inspector-class"
    >
      {meta.label}
    </span>
  );

  const body = (
    <div
      className="fy-ledger__inspector-body"
      data-testid="ledger-inspector-body"
    >
      <p
        className="fy-ledger__inspector-time"
        data-testid="ledger-inspector-time"
      >
        {formatLongDateAtTime(event.timestamp, tKey)}
      </p>
      {event.body ? (
        <p className="fy-ledger__inspector-text">{event.body}</p>
      ) : (
        <p className="fy-ledger__inspector-text">{event.summary}</p>
      )}

      <dl className="fy-ledger__inspector-def">
        <div>
          <dt>Actor</dt>
          <dd>{actorLabel}</dd>
        </div>
        {event.target ? (
          <div>
            <dt>Target</dt>
            <dd>{event.target}</dd>
          </div>
        ) : null}
        {event.detail_type ? (
          <div>
            <dt>Type</dt>
            <dd>{event.detail_type}</dd>
          </div>
        ) : null}
        {event.scope && event.scope.length > 0 ? (
          <div>
            <dt>Scope</dt>
            <dd>{event.scope.join(", ")}</dd>
          </div>
        ) : null}
        {event.related_nodes && event.related_nodes.length > 0 ? (
          <div>
            <dt>Related nodes</dt>
            <dd>
              {event.related_nodes.map((node, idx) => (
                <span key={node.id}>
                  {idx > 0 ? ", " : null}
                  <a
                    className="fy-ledger__inspector-link"
                    href={node.href ?? "#"}
                  >
                    {node.label}
                  </a>
                </span>
              ))}
            </dd>
          </div>
        ) : null}
      </dl>

      {event.changes && event.changes.length > 0 ? (
        <section className="fy-ledger__inspector-section">
          <h3 className="fy-ledger__inspector-section-title">
            Changes triggered
          </h3>
          <ul className="fy-ledger__inspector-changes">
            {event.changes.map((change, idx) => (
              <li key={idx}>{change.text}</li>
            ))}
          </ul>
        </section>
      ) : null}

      {event.evidence && event.evidence.length > 0 ? (
        <section className="fy-ledger__inspector-section">
          <h3 className="fy-ledger__inspector-section-title">
            Evidence at time of action
          </h3>
          <EvidenceMiniGrid items={event.evidence} />
        </section>
      ) : null}

      {event.mini_timeline && event.mini_timeline.length > 0 ? (
        <section className="fy-ledger__inspector-section">
          <h3 className="fy-ledger__inspector-section-title">Timeline</h3>
          <MiniTimeline steps={event.mini_timeline} />
        </section>
      ) : null}

      <section className="fy-ledger__inspector-section">
        <h3 className="fy-ledger__inspector-section-title">Related</h3>
        <div className="fy-ledger__inspector-related">
          <a className="fy-ledger__inspector-link" href="#" data-testid="ledger-link-view-in-model">
            View in model →
          </a>
          <a className="fy-ledger__inspector-link" href="#" data-testid="ledger-link-view-full-chain">
            View full chain →
          </a>
        </div>
      </section>
    </div>
  );

  return (
    <RightInspector
      title={
        <span data-testid="ledger-inspector-title">{event.title}</span>
      }
      classification={classification}
      body={body}
      onBack={onBack}
      onForward={onForward}
      onClose={onClose}
      canBack={canBack}
      canForward={canForward}
    />
  );
}
