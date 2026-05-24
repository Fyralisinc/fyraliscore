import type { LedgerChainDetail } from "@/api/ledger-v2-types";
import { StatusChip } from "./StatusChip";
import { EventTimeline } from "./EventTimeline";
import { BeforeAfter } from "./BeforeAfter";
import { EvidenceAtTime } from "./EvidenceAtTime";
import { OutcomeImpactBlock } from "./OutcomeImpactBlock";
import { ForecastAccuracyBlock } from "./ForecastAccuracyBlock";
import { RelatedContextBlock } from "./RelatedContext";

interface Props {
  detail: LedgerChainDetail;
  onOpenAccuracy?: () => void;
  onOpenEvent?: (id: string) => void;
}

// The Chain Inspector — the main interpretive surface in Timeline
// mode. Layout: head + summary on top, then a two-column grid:
//   left:  Event timeline
//   right: Before/After, Evidence, Outcome & Impact, Accuracy, Related
export function ChainInspector({ detail, onOpenAccuracy, onOpenEvent }: Props) {
  return (
    <section className="lg-inspector" aria-labelledby="lg-inspector-title">
      <button
        type="button"
        className="lg-inspector__menu"
        aria-label="More chain actions"
      >
        <svg width="14" height="14" viewBox="0 0 14 14" aria-hidden="true">
          <circle cx="3" cy="7" r="1.1" fill="currentColor" />
          <circle cx="7" cy="7" r="1.1" fill="currentColor" />
          <circle cx="11" cy="7" r="1.1" fill="currentColor" />
        </svg>
      </button>
      <header className="lg-inspector__head">
        <div className="lg-micro-label">Selected chain</div>
        <div className="lg-inspector__title-row">
          <h2 id="lg-inspector-title" className="lg-inspector__title">
            {detail.title}
          </h2>
          <StatusChip status={detail.status} />
        </div>
        <div className="lg-inspector__sub">
          <span>Started {shortDate(detail.startedAt)}</span>
          {detail.resolvedAt ? (
            <>
              <span className="lg-dot">·</span>
              <span>Resolved {shortDate(detail.resolvedAt)}</span>
            </>
          ) : null}
          <span className="lg-dot">·</span>
          <span>
            {detail.eventCount} {detail.eventCount === 1 ? "event" : "events"}
          </span>
        </div>
        <p className="lg-inspector__summary">{detail.summary}</p>
      </header>

      <div className="lg-inspector__grid">
        <div className="lg-inspector__col-left">
          <div className="lg-micro-label">Timeline</div>
          <EventTimeline events={detail.events} onOpenEvent={onOpenEvent} />
          <a className="lg-link lg-inspector__more" href="#full-event-log">
            View full event log →
          </a>
        </div>
        <div className="lg-inspector__col-right">
          <BeforeAfter before={detail.beforeState} after={detail.afterState} />
          <div className="lg-inspector__pair">
            <EvidenceAtTime evidence={detail.evidenceAtTime} />
            <OutcomeImpactBlock outcome={detail.outcome} />
          </div>
          <div className="lg-inspector__pair">
            <ForecastAccuracyBlock
              result={detail.accuracy}
              onOpenAccuracy={onOpenAccuracy}
            />
            <RelatedContextBlock related={detail.relatedContext} />
          </div>
        </div>
      </div>
    </section>
  );
}

function shortDate(iso: string): string {
  return new Date(iso).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}
