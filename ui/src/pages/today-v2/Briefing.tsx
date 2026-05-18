// Today page — single combined layout.
//
//   ┌────────────────────────────────────────────────────────────┐
//   │ Sidebar │ Today header (briefing line + Ask Fyralis search) │
//   │         │ Fyralis Brief (synthesis · what changed · handled) │
//   │         │ ┌──────────────┬─────────────────────────────────┐ │
//   │         │ │ Review queue │ Focused review sheet            │ │
//   │         │ │ rail         │   (selected proposed change)    │ │
//   │         │ └──────────────┴─────────────────────────────────┘ │
//   │         │ Action bar (Accept · Delegate · Request · Report) │
//   └────────────────────────────────────────────────────────────┘
//
// The page does not switch modes. A delta is always selected and its
// focused review sheet renders alongside the queue rail. Clicking
// another rail row swaps focus; URL stays in sync via ?review=<id>.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";

import { useTodayPage } from "@/hooks/useTodayPage";
import { getDeltaEvidence } from "@/api/today-page-client";

import { BriefingHeader } from "@/components/today-v2/BriefingHeader";
import {
  FyralisBrief,
  deriveWhatChanged,
} from "@/components/today-v2/FyralisBrief";
import { ReviewQueueRail } from "@/components/today-v2/ReviewQueueRail";
import { FocusedReviewCard } from "@/components/today-v2/FocusedReviewCard";
import { ReviewActionBar } from "@/components/today-v2/ReviewActionBar";
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

  // Lazy-loaded evidence keyed by deltaId.
  const [evidenceCache, setEvidenceCache] = useState<
    Record<string, EvidenceResponse>
  >({});
  const [evidenceDelta, setEvidenceDelta] = useState<DecisionDelta | null>(null);
  const [evidenceLoading, setEvidenceLoading] = useState(false);

  const orderedQueue = useMemo<DecisionDelta[]>(() => {
    if (!data) return [];
    const list: DecisionDelta[] = [];
    if (data.primaryJudgment) list.push(data.primaryJudgment);
    list.push(...data.otherChanges);
    return list;
  }, [data]);

  // Selected delta is driven by ?review=<id>; if absent or stale, fall
  // back to the primary judgment so the focused sheet is never empty.
  const reviewParam = searchParams.get("review");
  const selectedId = useMemo(() => {
    if (reviewParam && orderedQueue.some((d) => d.id === reviewParam)) {
      return reviewParam;
    }
    return orderedQueue[0]?.id ?? null;
  }, [reviewParam, orderedQueue]);
  const selectedDelta = useMemo(
    () => orderedQueue.find((d) => d.id === selectedId) ?? null,
    [orderedQueue, selectedId],
  );

  const setSelected = useCallback(
    (id: string, opts?: { replace?: boolean }) => {
      const next = new URLSearchParams(searchParams);
      next.set("review", id);
      setSearchParams(next, { replace: opts?.replace ?? false });
      window.setTimeout(() => {
        const heading = document.querySelector<HTMLElement>(
          `#focused-${id} .tdv2-review__title`,
        );
        heading?.focus?.();
      }, 0);
    },
    [searchParams, setSearchParams],
  );

  const positionOf = useCallback(
    (id: string): { index: number; total: number } | null => {
      const idx = orderedQueue.findIndex((d) => d.id === id);
      if (idx < 0) return null;
      return { index: idx, total: orderedQueue.length };
    },
    [orderedQueue],
  );

  // Keyboard shortcuts — J/K (and Arrow keys) cycle through the queue;
  // Esc closes the topmost sheet/drawer. Don't fire when typing in an
  // input/textarea or when a sheet is open.
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
        }
        return;
      }

      if (!selectedDelta) return;
      if (delegateTarget || correctionTarget || evidenceDelta) return;
      const idx = orderedQueue.findIndex((d) => d.id === selectedDelta.id);
      if (e.key === "j" || e.key === "ArrowDown") {
        if (idx + 1 < orderedQueue.length) {
          setSelected(orderedQueue[idx + 1].id, { replace: true });
          e.preventDefault();
        }
      } else if (e.key === "k" || e.key === "ArrowUp") {
        if (idx - 1 >= 0) {
          setSelected(orderedQueue[idx - 1].id, { replace: true });
          e.preventDefault();
        }
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [
    correctionTarget,
    delegateTarget,
    evidenceDelta,
    orderedQueue,
    selectedDelta,
    setSelected,
  ]);

  const showToast = useCallback((kind: ToastKind, text: string) => {
    setToast({ kind, text, id: Date.now() });
  }, []);

  useEffect(() => {
    if (!toast) return;
    const id = window.setTimeout(() => setToast(null), 4500);
    return () => window.clearTimeout(id);
  }, [toast]);

  const evidenceAbortRef = useRef<AbortController | null>(null);
  useEffect(() => {
    if (evidenceDelta === null) {
      evidenceAbortRef.current?.abort();
      evidenceAbortRef.current = null;
    }
  }, [evidenceDelta]);
  useEffect(() => () => evidenceAbortRef.current?.abort(), []);

  const openEvidence = useCallback(
    async (delta: DecisionDelta) => {
      setEvidenceDelta(delta);
      if (evidenceCache[delta.id]) return;
      evidenceAbortRef.current?.abort();
      const controller = new AbortController();
      evidenceAbortRef.current = controller;
      setEvidenceLoading(true);
      try {
        const ev = await getDeltaEvidence(delta.id, controller.signal);
        if (controller.signal.aborted) return;
        setEvidenceCache((prev) => ({ ...prev, [delta.id]: ev }));
      } catch (err) {
        if (controller.signal.aborted) return;
        if ((err as { name?: string })?.name === "AbortError") return;
        showToast("error", "Could not load evidence right now.");
        setEvidenceDelta(null);
      } finally {
        if (!controller.signal.aborted) setEvidenceLoading(false);
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
          // Advance to the next item in the queue so review momentum continues.
          const idx = orderedQueue.findIndex((d) => d.id === id);
          if (idx >= 0 && idx + 1 < orderedQueue.length) {
            setSelected(orderedQueue[idx + 1].id, { replace: true });
          }
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
    [applyChange, orderedQueue, refetch, setSelected, showToast],
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

  const whatChanged = useMemo(
    () => (data ? deriveWhatChanged(orderedQueue) : []),
    [data, orderedQueue],
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
            ) : data && selectedDelta ? (
              <>
                <BriefingHeader
                  summary={data.summary}
                  generatedAt={data.generatedAt}
                />
                <FyralisBrief
                  synthesis={
                    data.handledWithoutYou.reassuranceCopy ||
                    "Customer reliability and pricing ownership are the only areas requiring your attention."
                  }
                  whatChanged={whatChanged}
                  handled={data.handledWithoutYou}
                />
                <div className="tdv2-split" data-testid="review-mode">
                  <ReviewQueueRail
                    items={orderedQueue}
                    selectedId={selectedDelta.id}
                    handled={data.handledWithoutYou}
                    onSelect={(id) => setSelected(id)}
                  />
                  <div className="tdv2-split__sheet">
                    <FocusedReviewCard
                      delta={selectedDelta}
                      applying={applyingId === selectedDelta.id}
                      position={positionOf(selectedDelta.id)}
                      onOpenEvidence={() => void openEvidence(selectedDelta)}
                    />
                  </div>
                </div>
                <ReviewActionBar
                  delta={selectedDelta}
                  applying={applyingId === selectedDelta.id}
                  onAccept={() => handleAccept(selectedDelta.id)}
                  onDelegate={() => setDelegateTarget(selectedDelta)}
                  onRequestChanges={() => setCorrectionTarget(selectedDelta)}
                  onCorrect={() => setCorrectionTarget(selectedDelta)}
                />
              </>
            ) : data ? (
              <AllClearState summary={data.handledWithoutYou} />
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
