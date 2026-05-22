// Right-side inspector for the selected node. Wraps the
// shared RightInspector primitive with model-specific body
// content: classification label, definition list, supports
// list, depends-on list, and footer buttons.

import { useMemo } from "react";
import type { MapNode } from "@/api/map-types";
import type { NodeMetaV2 } from "@/api/map-mock-v2";
import type { TraceStep } from "@/api/model-trace-types";
import { RightInspector, StatusChip } from "@/components/primitives";

export interface NodeInspectorProps {
  node: MapNode;
  meta?: NodeMetaV2;
  supports: TraceStep[];
  dependsOn: TraceStep[];
  onClose: () => void;
  onTraceBack: () => void;
  onTraceForward: () => void;
  onContest: () => void;
  onCreateDelta: () => void;
  tracing: "back" | "forward" | null;
}

function relTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "—";
  const secs = Math.max(0, (Date.now() - t) / 1000);
  if (secs < 60) return `${Math.round(secs)}s ago`;
  if (secs < 3600) return `${Math.round(secs / 60)} minute${secs >= 120 ? "s" : ""} ago`;
  if (secs < 86_400) return `${Math.round(secs / 3600)}h ago`;
  return `${Math.round(secs / 86_400)}d ago`;
}

export function NodeInspector({
  node,
  meta,
  supports,
  dependsOn,
  onClose,
  onTraceBack,
  onTraceForward,
  onContest,
  onCreateDelta,
  tracing,
}: NodeInspectorProps) {
  const classification = useMemo(() => {
    if (meta?.critical) {
      return (
        <span className="fy-inspector__crit" data-testid="inspector-critical">
          CRITICAL RISK
        </span>
      );
    }
    if (node.band === "decision") {
      return <StatusChip variant="authority">DECISION</StatusChip>;
    }
    if (node.band === "commitment") {
      return <StatusChip variant="trust">COMMITMENT</StatusChip>;
    }
    if (node.band === "customer") {
      return <StatusChip variant="evidence">CUSTOMER</StatusChip>;
    }
    if (node.band === "goal") {
      return <StatusChip variant="trust">GOAL</StatusChip>;
    }
    if (node.band === "risk") {
      return <StatusChip variant="review">RISK</StatusChip>;
    }
    return null;
  }, [meta?.critical, node.band]);

  const isContested = node.health === "contested";
  const isAwaiting = meta?.awaiting_confirmation;

  return (
    <div className="fy-model-inspector" data-testid="node-inspector">
      <div className="fy-model-inspector__header-row">
        <span className="fy-model-inspector__eyebrow">Selected Node</span>
      </div>
      <RightInspector
        title={node.natural}
        classification={classification}
        onClose={onClose}
        canBack={false}
        canForward={false}
        body={
          <div className="fy-model-inspector__body">
            <dl className="fy-model-inspector__dl">
              <div>
                <dt>Status</dt>
                <dd>
                  <span
                    className={`fy-model-inspector__dot fy-model-inspector__dot--${
                      isAwaiting
                        ? "awaiting"
                        : isContested
                          ? "contested"
                          : "active"
                    }`}
                  />
                  {meta?.status_label ??
                    (isAwaiting
                      ? "Awaiting confirmation"
                      : isContested
                        ? "Contested"
                        : "Active")}
                </dd>
              </div>
              <div>
                <dt>Confidence</dt>
                <dd>
                  {node.confidence.toFixed(2)}{" "}
                  <span className="fy-model-inspector__muted">calibrated</span>
                </dd>
              </div>
              <div>
                <dt>Authority</dt>
                <dd>{meta?.owner ?? "Fyralis inference"}</dd>
              </div>
              <div>
                <dt>Subject</dt>
                <dd>
                  {node.band === "risk" && meta?.critical
                    ? "Beacon · Northvale · Conduit · Salesforce sync"
                    : node.natural}
                </dd>
              </div>
              <div>
                <dt>Last confirmed</dt>
                <dd>{relTime(meta?.last_confirmed_at ?? null)}</dd>
              </div>
            </dl>

            <section className="fy-model-inspector__section">
              <h3>Falsifies if</h3>
              <p>
                Two anchor renewals confirm intent to renew with the
                current Salesforce sync state, or the sync incident rate
                drops to zero for 30 days.
              </p>
            </section>

            <section
              className="fy-model-inspector__section"
              data-testid="inspector-supports"
            >
              <h3>Supports</h3>
              {supports.length === 0 ? (
                <p className="fy-model-inspector__muted">No support links surfaced yet.</p>
              ) : (
                <ul className="fy-model-inspector__list">
                  {supports.map((s) => (
                    <li key={s.id}>
                      <span className="fy-model-inspector__type">{s.kind}</span>
                      <span>{s.summary}</span>
                    </li>
                  ))}
                </ul>
              )}
            </section>

            <section
              className="fy-model-inspector__section"
              data-testid="inspector-depends-on"
            >
              <h3>Depends on</h3>
              {dependsOn.length === 0 ? (
                <p className="fy-model-inspector__muted">No dependencies surfaced yet.</p>
              ) : (
                <ul className="fy-model-inspector__list">
                  {dependsOn.map((s) => (
                    <li key={s.id}>
                      <span>{s.summary}</span>
                    </li>
                  ))}
                </ul>
              )}
            </section>
          </div>
        }
        footerActions={
          <div className="fy-model-inspector__actions">
            <button
              type="button"
              className={`fy-btn fy-btn--secondary${tracing === "back" ? " is-active" : ""}`}
              onClick={onTraceBack}
              data-testid="trace-back"
              aria-pressed={tracing === "back"}
            >
              ← Trace back
            </button>
            <button
              type="button"
              className={`fy-btn fy-btn--secondary${tracing === "forward" ? " is-active" : ""}`}
              onClick={onTraceForward}
              data-testid="trace-forward"
              aria-pressed={tracing === "forward"}
            >
              Trace forward →
            </button>
            <button
              type="button"
              className="fy-btn fy-btn--secondary"
              onClick={onContest}
              data-testid="contest-claim"
            >
              Contest claim
            </button>
            <button
              type="button"
              className="fy-btn fy-btn--critical"
              onClick={onCreateDelta}
              data-testid="create-delta"
            >
              Create Decision Delta
            </button>
          </div>
        }
      />
    </div>
  );
}

export default NodeInspector;
