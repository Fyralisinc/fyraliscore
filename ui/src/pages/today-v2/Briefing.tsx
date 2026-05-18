// Today page — Briefing Mode. Default landing surface.
//
// Layout: header → vertical queue of judgment cards.
// One card at a time enters Focused Review state in place; the rest
// remain visible above and below as compact rows. No navigation, no
// modal, no separate inspector (spec §4.2).

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";

import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";

import { useTodayPage } from "@/hooks/useTodayPage";
import { getDeltaEvidence } from "@/api/today-page-client";

import { BriefingHeader } from "@/components/today-v2/BriefingHeader";
import { FocusedReviewCard } from "@/components/today-v2/FocusedReviewCard";
import { CompactCard } from "@/components/today-v2/CompactCard";
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

  // Lazy-loaded evidence keyed by deltaId. Cleared on refetch.
  const [evidenceCache, setEvidenceCache] = useState<
    Record<string, EvidenceResponse>
  >({});
  const [evidenceDelta, setEvidenceDelta] = useState<DecisionDelta | null>(null);
  const [evidenceLoading, setEvidenceLoading] = useState(false);

  // Ordered queue: primary first, then others.
  const orderedQueue = useMemo<DecisionDelta[]>(() => {
    if (!data) return [];
    const list: DecisionDelta[] = [];
    if (data.primaryJudgment) list.push(data.primaryJudgment);
    list.push(...data.otherChanges);
    return list;
  }, [data]);

  // Which card is in Focused Review state. Defaults to the primary
  // judgment so the user lands directly on the most urgent case.
  const [expandedId, setExpandedId] = useState<string | null>(null);
  useEffect(() => {
    if (expandedId && orderedQueue.some((d) => d.id === expandedId)) return;
    setExpandedId(orderedQueue[0]?.id ?? null);
  }, [orderedQueue, expandedId]);

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
    if (orderedQueue.some((d) => d.id === target)) {
      setExpandedId(target);
    }
    const next = new URLSearchParams(searchParams);
    next.delete("expand");
    setSearchParams(next, { replace: true });
  }, [data, orderedQueue, searchParams, setSearchParams]);

  // Keyboard model. Esc collapses the open card or closes the top-most
  // sheet. Letter shortcuts hit the visible action bar.
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
        }
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [
    delegateTarget,
    correctionTarget,
    evidenceDelta,
    expandedId,
  ]);

  const showToast = useCallback((kind: ToastKind, text: string) => {
    setToast({ kind, text, id: Date.now() });
  }, []);

  useEffect(() => {
    if (!toast) return;
    const id = window.setTimeout(() => setToast(null), 4500);
    return () => window.clearTimeout(id);
  }, [toast]);

  const handleOpen = useCallback((id: string) => {
    setExpandedId(id);
    window.setTimeout(() => {
      const el = document.getElementById(`focused-${id}`);
      el?.scrollIntoView({ behavior: "smooth", block: "start" });
    }, 0);
  }, []);

  const handleCollapse = useCallback((id: string) => {
    setExpandedId((current) => (current === id ? null : current));
  }, []);

  // Abort the in-flight evidence fetch when the user opens a different
  // delta, closes the drawer, or unmounts the page, so a slow response
  // can't overwrite the cache or clear the loading flag for a newer
  // request.
  const evidenceAbortRef = useRef<AbortController | null>(null);
  useEffect(() => {
    if (evidenceDelta === null) {
      evidenceAbortRef.current?.abort();
      evidenceAbortRef.current = null;
    }
  }, [evidenceDelta]);
  useEffect(
    () => () => {
      evidenceAbortRef.current?.abort();
    },
    [],
  );

  const openEvidence = useCallback(
    async (delta: DecisionDelta) => {
      setEvidenceDelta(delta);
      // Always read from a ref-style snapshot of cache via setState below;
      // a direct read of `evidenceCache` here would be stale for callbacks
      // queued before the latest setState commits.
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
        if (!controller.signal.aborted) {
          setEvidenceLoading(false);
        }
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
                {orderedQueue.length > 0 ? (
                  <div className="tdv2-stream" data-testid="today-stream">
                    {orderedQueue.map((d) =>
                      d.id === expandedId ? (
                        <FocusedReviewCard
                          key={d.id}
                          delta={d}
                          applying={applyingId === d.id}
                          position={positionOf(d.id)}
                          onCollapse={() => handleCollapse(d.id)}
                          onAccept={() => handleAccept(d.id)}
                          onDelegate={() => setDelegateTarget(d)}
                          onCorrect={() => setCorrectionTarget(d)}
                          onOpenEvidence={() => void openEvidence(d)}
                        />
                      ) : (
                        <CompactCard
                          key={d.id}
                          delta={d}
                          onOpen={() => handleOpen(d.id)}
                        />
                      ),
                    )}
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
