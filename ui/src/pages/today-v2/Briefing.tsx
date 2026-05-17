// Today page — Briefing Mode. Default landing surface.
//
// Layout is a single vertical column: header → summary strip → primary
// judgment card → other-judgment accordion → handled-without-you panel.
// Cards expand in place rather than navigating away, so the user's
// scroll position and mental anchor never reset.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";

import { useTodayPage } from "@/hooks/useTodayPage";
import { getDeltaEvidence } from "@/api/today-page-client";

import { BriefingHeader } from "@/components/today-v2/BriefingHeader";
import { SummaryStrip } from "@/components/today-v2/SummaryStrip";
import { PrimaryJudgmentCard } from "@/components/today-v2/PrimaryJudgmentCard";
import { OtherJudgmentList } from "@/components/today-v2/OtherJudgmentList";
import { HandledWithoutYouPanel } from "@/components/today-v2/HandledWithoutYouPanel";
import { DelegationSheet } from "@/components/today-v2/DelegationSheet";
import { CorrectionSheet } from "@/components/today-v2/CorrectionSheet";
import { EvidenceDrawer } from "@/components/today-v2/EvidenceDrawer";
import { Toast } from "@/components/today-v2/Toast";

import type {
  CorrectionBody,
  DecisionDelta,
  DelegateBody,
  EvidenceResponse,
  HandledWithoutYouSummary,
} from "@/api/today-page-types";

import "@/pages/today-v2/styles.css";

type ToastKind = "success" | "error" | "info";
type ToastState = { kind: ToastKind; text: string; id: number };

export default function TodayBriefing() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { data, loading, error, applyChange, delegate, correct, refetch } =
    useTodayPage();

  const [applyingId, setApplyingId] = useState<string | null>(null);
  const [delegateTarget, setDelegateTarget] = useState<DecisionDelta | null>(null);
  const [correctionTarget, setCorrectionTarget] = useState<DecisionDelta | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);

  // Inline expansion. Tracks which "other item" is open. The primary
  // judgment card has its own toggle (primaryExpanded) since the user's
  // mental model is "primary is always partly open, click for more".
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [primaryExpanded, setPrimaryExpanded] = useState(false);

  // Lazy-loaded evidence keyed by deltaId. Cleared on refetch.
  const [evidenceCache, setEvidenceCache] = useState<
    Record<string, EvidenceResponse>
  >({});
  const [evidenceDelta, setEvidenceDelta] = useState<DecisionDelta | null>(null);
  const [evidenceLoading, setEvidenceLoading] = useState(false);

  // Ordered queue for "Reviewing N of M" — primary first, others after.
  const orderedQueue = useMemo<DecisionDelta[]>(() => {
    if (!data) return [];
    const list: DecisionDelta[] = [];
    if (data.primaryJudgment) list.push(data.primaryJudgment);
    list.push(...data.otherChanges);
    return list;
  }, [data]);

  const positionOf = useCallback(
    (id: string): { index: number; total: number } | null => {
      const idx = orderedQueue.findIndex((d) => d.id === id);
      if (idx < 0) return null;
      return { index: idx, total: orderedQueue.length };
    },
    [orderedQueue],
  );

  // Deep-link support: ?expand=<deltaId> auto-opens that card.
  useEffect(() => {
    const target = searchParams.get("expand");
    if (!target || !data) return;
    if (data.primaryJudgment?.id === target) {
      setPrimaryExpanded(true);
    } else if (data.otherChanges.some((d) => d.id === target)) {
      setExpandedId(target);
    }
    // Clean the param so re-renders don't keep re-opening on toggle.
    const next = new URLSearchParams(searchParams);
    next.delete("expand");
    setSearchParams(next, { replace: true });
  }, [data, searchParams, setSearchParams]);

  // Keyboard model. Esc collapses the open accordion (or closes the
  // top-most sheet); Enter expands/collapses the primary card.
  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const tag = (document.activeElement as HTMLElement | null)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA") return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === "Escape") {
        if (evidenceDelta) {
          setEvidenceDelta(null);
          e.preventDefault();
        } else if (delegateTarget) {
          setDelegateTarget(null);
          e.preventDefault();
        } else if (correctionTarget) {
          setCorrectionTarget(null);
          e.preventDefault();
        } else if (expandedId) {
          setExpandedId(null);
          e.preventDefault();
        } else if (primaryExpanded) {
          setPrimaryExpanded(false);
          e.preventDefault();
        }
        return;
      }
      if (
        e.key === "Enter" &&
        data?.primaryJudgment &&
        !delegateTarget &&
        !correctionTarget &&
        !evidenceDelta
      ) {
        setPrimaryExpanded((v) => !v);
        e.preventDefault();
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [
    data,
    delegateTarget,
    correctionTarget,
    evidenceDelta,
    expandedId,
    primaryExpanded,
  ]);

  const showToast = useCallback((kind: ToastKind, text: string) => {
    setToast({ kind, text, id: Date.now() });
  }, []);

  useEffect(() => {
    if (!toast) return;
    const id = window.setTimeout(() => setToast(null), 4500);
    return () => window.clearTimeout(id);
  }, [toast]);

  const handleToggleOther = useCallback((id: string) => {
    setExpandedId((current) => {
      if (current === id) return null;
      // Smoothly bring the newly-expanded card into the viewport so the
      // user can read it without manual scrolling. The browser keeps
      // surrounding cards visible above/below.
      window.setTimeout(() => {
        const el = document.querySelector(
          `[data-testid="other-card-${id}"]`,
        ) as HTMLElement | null;
        el?.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }, 0);
      return id;
    });
  }, []);

  const openEvidence = useCallback(
    async (delta: DecisionDelta) => {
      setEvidenceDelta(delta);
      if (evidenceCache[delta.id]) return;
      setEvidenceLoading(true);
      try {
        const ev = await getDeltaEvidence(delta.id);
        setEvidenceCache((prev) => ({ ...prev, [delta.id]: ev }));
      } catch {
        showToast("error", "Could not load evidence right now.");
        setEvidenceDelta(null);
      } finally {
        setEvidenceLoading(false);
      }
    },
    [evidenceCache, showToast],
  );

  const handleAccept = useCallback(
    async (id: string) => {
      setApplyingId(id);
      try {
        const result = await applyChange(id);
        if (result?.status === "applied") {
          showToast("success", result.resultMessage);
          // Collapse the row that was just accepted — it's gone from
          // the actionable set.
          setExpandedId((current) => (current === id ? null : current));
        } else if (result?.status === "requires_refresh") {
          showToast("error", result.resultMessage);
          await refetch();
        }
      } catch {
        showToast("error", "Could not apply change. Please try again.");
      } finally {
        setApplyingId(null);
      }
    },
    [applyChange, refetch, showToast],
  );

  const handleDelegate = useCallback(
    async (body: DelegateBody) => {
      if (!delegateTarget) return;
      try {
        const result = await delegate(delegateTarget.id, body);
        if (result?.status === "delegated") {
          showToast("success", result.resultMessage);
        } else if (result?.status === "requires_refresh") {
          showToast("error", result.resultMessage);
          await refetch();
        }
      } catch {
        showToast("error", "Could not delegate change. Please try again.");
      } finally {
        setDelegateTarget(null);
      }
    },
    [delegate, delegateTarget, refetch, showToast],
  );

  const handleCorrection = useCallback(
    async (body: CorrectionBody) => {
      if (!correctionTarget) return;
      try {
        const result = await correct(correctionTarget.id, body);
        if (result?.status === "correction_submitted") {
          showToast("success", result.resultMessage);
        } else if (result?.status === "requires_refresh") {
          showToast("error", result.resultMessage);
          await refetch();
        }
      } catch {
        showToast("error", "Could not submit correction. Please try again.");
      } finally {
        setCorrectionTarget(null);
      }
    },
    [correct, correctionTarget, refetch, showToast],
  );

  return (
    <>
      <AppShell
        sidebar={<Sidebar activeRoute="today" />}
        main={
          <div className="tdv2-page" data-testid="today-page">
            {loading && !data ? (
              <LoadingSkeleton />
            ) : error ? (
              <ErrorState />
            ) : data ? (
              <>
                <BriefingHeader
                  summary={data.summary}
                  generatedAt={data.generatedAt}
                />
                <SummaryStrip summary={data.summary} />
                {data.primaryJudgment || data.otherChanges.length > 0 ? (
                  <div className="tdv2-body tdv2-body--stack">
                    {data.primaryJudgment ? (
                      <PrimaryJudgmentCard
                        delta={data.primaryJudgment}
                        applying={applyingId === data.primaryJudgment.id}
                        expanded={primaryExpanded}
                        position={positionOf(data.primaryJudgment.id) ?? undefined}
                        onToggleExpand={() => setPrimaryExpanded((v) => !v)}
                        onOpenEvidence={() =>
                          void openEvidence(data.primaryJudgment!)
                        }
                        onAccept={() =>
                          handleAccept(data.primaryJudgment!.id)
                        }
                        onDelegate={() =>
                          setDelegateTarget(data.primaryJudgment!)
                        }
                        onCorrect={() =>
                          setCorrectionTarget(data.primaryJudgment!)
                        }
                      />
                    ) : null}
                    <OtherJudgmentList
                      items={data.otherChanges}
                      expandedId={expandedId}
                      applyingId={applyingId}
                      positionOf={positionOf}
                      onToggle={handleToggleOther}
                      onAccept={handleAccept}
                      onDelegate={(d) => setDelegateTarget(d)}
                      onCorrect={(d) => setCorrectionTarget(d)}
                      onOpenEvidence={(d) => void openEvidence(d)}
                    />
                    <HandledWithoutYouPanel summary={data.handledWithoutYou} />
                  </div>
                ) : (
                  <AllClearState summary={data.handledWithoutYou} />
                )}
              </>
            ) : null}
          </div>
        }
      />
      {delegateTarget ? (
        <DelegationSheet
          delta={delegateTarget}
          onCancel={() => setDelegateTarget(null)}
          onSubmit={handleDelegate}
        />
      ) : null}
      {correctionTarget ? (
        <CorrectionSheet
          onCancel={() => setCorrectionTarget(null)}
          onSubmit={handleCorrection}
        />
      ) : null}
      {evidenceDelta && evidenceCache[evidenceDelta.id] ? (
        <EvidenceDrawer
          data={evidenceCache[evidenceDelta.id]}
          deltaTitle={evidenceDelta.title}
          onClose={() => setEvidenceDelta(null)}
        />
      ) : null}
      {evidenceDelta && evidenceLoading && !evidenceCache[evidenceDelta.id] ? (
        <EvidenceLoadingBackdrop />
      ) : null}
      {toast ? (
        <Toast
          text={toast.text}
          kind={toast.kind}
          onDismiss={() => setToast(null)}
        />
      ) : null}
    </>
  );
}

function LoadingSkeleton() {
  return (
    <div className="tdv2-skeleton" data-testid="today-skeleton">
      <div className="tdv2-skeleton-block tdv2-skeleton-block--summary" />
      <div className="tdv2-skeleton-block tdv2-skeleton-block--primary" />
      <div className="tdv2-skeleton-block tdv2-skeleton-block--side" />
    </div>
  );
}

function ErrorState() {
  return (
    <div className="tdv2-error" data-testid="today-error">
      We couldn't load Today right now. Try again in a moment.
    </div>
  );
}

function EvidenceLoadingBackdrop() {
  return (
    <div className="tdv2-drawer-backdrop" data-testid="evidence-loading">
      <div className="tdv2-drawer" style={{ padding: "var(--space-6)" }}>
        <p style={{ margin: 0, color: "var(--text-muted)", fontSize: 13 }}>
          Loading evidence…
        </p>
      </div>
    </div>
  );
}

function AllClearState({ summary }: { summary: HandledWithoutYouSummary }) {
  return (
    <div className="tdv2-empty" data-testid="today-all-clear">
      <h2 className="tdv2-empty__title">Nothing needs your judgment right now.</h2>
      <p className="tdv2-empty__body">
        Fyralis processed {summary.signalsAbsorbed} signals since your last review.
        {summary.modelUpdatesApplied > 0
          ? ` ${summary.modelUpdatesApplied} model updates were absorbed automatically.`
          : ""}
        {summary.itemsUnderMonitoring > 0
          ? ` ${summary.itemsUnderMonitoring} items are being monitored.`
          : ""}
      </p>
      <div className="tdv2-empty__actions">
        <a className="tdv2-btn tdv2-btn--primary" href="/model">
          Open Model
        </a>
        <a className="tdv2-btn tdv2-btn--secondary" href="/ledger">
          View Ledger
        </a>
      </div>
    </div>
  );
}
