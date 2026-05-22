// Model page (v2) — entry point.
//
// Renders the AppShell + Model header + relationship-mode bar + the
// state-specific canvas. The state machine is local: one
// active focus state at a time, with a back stack so Esc / back
// returns to the previous view. Browser back also pops the stack.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";

import {
  Breadcrumb,
  ModelHeader,
  RelationshipModeBar,
} from "./components/primitives";
import { CategorySheet } from "./components/CategorySheet";
import { SearchOverlay } from "./components/SearchOverlay";
import { FullDetailSheet } from "./components/FullDetailSheet";
import { ArrowDefs } from "./canvas/ArrowDefs";
import { NodeNeighborhood } from "./canvas/NodeNeighborhood";
import { OverviewMap } from "./canvas/OverviewMap";
import { RelationshipCorridor } from "./canvas/RelationshipCorridor";
import { TracePath } from "./canvas/TracePath";
import {
  loadCategoryFocus,
  loadItemDetail,
  loadOverview,
  loadRelationshipFocus,
  loadTrace,
} from "./data/load";
import type {
  CategoryFocus,
  CategoryId,
  ItemDetail,
  ModelOverview,
  ModelPageState,
  RelationshipFocus,
  RelationshipMode,
  Trace,
} from "./types";

type Phase = "idle" | "loading" | "error";

export default function ModelPage() {
  const [mode, setMode] = useState<RelationshipMode>("impact");
  const [state, setState] = useState<ModelPageState>({ type: "overview" });
  const [backStack, setBackStack] = useState<ModelPageState[]>([]);
  const [search, setSearch] = useState("");

  const [overview, setOverview] = useState<ModelOverview | null>(null);
  const [overviewPhase, setOverviewPhase] = useState<Phase>("loading");
  const [categoryFocus, setCategoryFocus] = useState<CategoryFocus | null>(null);
  const [relationshipFocus, setRelationshipFocus] = useState<RelationshipFocus | null>(null);
  const [itemDetail, setItemDetail] = useState<ItemDetail | null>(null);
  const [trace, setTrace] = useState<Trace | null>(null);
  const [stateError, setStateError] = useState<string | null>(null);
  const [fullDetailOpen, setFullDetailOpen] = useState(false);
  // Category focus is shown as a right-side drawer (overlay) rather
  // than as a main-canvas state, so the overview lattice stays visible
  // behind a blurred backdrop while the user inspects one category.
  const [openCategoryId, setOpenCategoryId] = useState<CategoryId | null>(null);

  const transitionRef = useRef(0);

  // Load overview (and re-load on mode change).
  useEffect(() => {
    const id = ++transitionRef.current;
    const ctrl = new AbortController();
    setOverviewPhase("loading");
    setStateError(null);
    loadOverview(mode, ctrl.signal)
      .then((o) => {
        if (id !== transitionRef.current) return;
        setOverview(o);
        setOverviewPhase("idle");
      })
      .catch((err: unknown) => {
        if ((err as Error)?.name === "AbortError") return;
        setStateError("Failed to load overview.");
        setOverviewPhase("error");
      });
    return () => ctrl.abort();
  }, [mode]);

  // Close the Full Detail sheet whenever we leave NodeZoom — the
  // sheet is anchored to the current item and should not survive a
  // state transition.
  useEffect(() => {
    if (state.type !== "nodeZoom") setFullDetailOpen(false);
  }, [state]);

  // Load category focus when the right-side sheet opens; clear when
  // it closes. The fetch is keyed on (openCategoryId, mode) so a mode
  // switch while the sheet is open refreshes the relationship list.
  useEffect(() => {
    if (openCategoryId === null) {
      setCategoryFocus(null);
      return;
    }
    const ctrl = new AbortController();
    setCategoryFocus(null);
    loadCategoryFocus(openCategoryId, mode, ctrl.signal)
      .then(setCategoryFocus)
      .catch((err: unknown) => {
        if ((err as Error)?.name === "AbortError") return;
        setStateError("Failed to load category focus.");
      });
    return () => ctrl.abort();
  }, [openCategoryId, mode]);

  // Esc on the open category sheet just closes it (handled inside the
  // component too), so we don't need to participate in the back stack.
  // But pushing into nodeZoom / relationshipZoom from the sheet should
  // dismiss it — handled at the callback sites below.


  // Side fetches per state.
  useEffect(() => {
    const ctrl = new AbortController();
    setStateError(null);
    if (state.type === "relationshipZoom") {
      setRelationshipFocus(null);
      loadRelationshipFocus(state.bundleId, ctrl.signal)
        .then((rf) => setRelationshipFocus(rf))
        .catch((err: unknown) => {
          if ((err as Error)?.name === "AbortError") return;
          setStateError("Failed to load relationship.");
        });
    } else if (state.type === "nodeZoom") {
      setItemDetail(null);
      loadItemDetail(state.itemId, ctrl.signal)
        .then(setItemDetail)
        .catch((err: unknown) => {
          if ((err as Error)?.name === "AbortError") return;
          setStateError("Failed to load item.");
        });
    } else if (state.type === "traceView") {
      setTrace(null);
      loadTrace(state.itemId, state.direction, state.depth, ctrl.signal)
        .then(setTrace)
        .catch((err: unknown) => {
          if ((err as Error)?.name === "AbortError") return;
          setStateError("Failed to load trace.");
        });
    }
    return () => ctrl.abort();
  }, [state, mode]);

  // Esc-to-back. Also clears search. When the Full Detail sheet is
  // open, Esc is handled by the sheet itself — we skip here so a
  // single Esc closes the sheet without also popping the back stack.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        if (fullDetailOpen) return;
        e.preventDefault();
        goBack();
      } else if ((e.key === "k" || e.key === "K") && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        pushState({ type: "searchFocus", query: "" });
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // pushState / goBack are stable through useCallback below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fullDetailOpen]);

  const pushState = useCallback((next: ModelPageState) => {
    setBackStack((s) => [...s, state]);
    setState(next);
  }, [state]);

  const goBack = useCallback(() => {
    setBackStack((s) => {
      if (s.length === 0) {
        setState({ type: "overview" });
        return s;
      }
      const prev = s[s.length - 1];
      setState(prev);
      return s.slice(0, -1);
    });
  }, []);

  const goOverview = useCallback(() => {
    setBackStack([]);
    setState({ type: "overview" });
  }, []);

  const onCategoryClick = useCallback(
    (id: CategoryId) => setOpenCategoryId(id),
    [],
  );
  const onBundleClick = useCallback(
    (bundleId: string) => pushState({ type: "relationshipZoom", bundleId }),
    [pushState],
  );
  const onItemClick = useCallback(
    (itemId: string) => pushState({ type: "nodeZoom", itemId }),
    [pushState],
  );
  const onTraceCause = useCallback(
    (itemId: string) =>
      pushState({ type: "traceView", itemId, direction: "cause", depth: 4 }),
    [pushState],
  );
  const onTraceConsequence = useCallback(
    (itemId: string) =>
      pushState({ type: "traceView", itemId, direction: "consequence", depth: 4 }),
    [pushState],
  );

  // Breadcrumb is derived from backStack + current state.
  const breadcrumb = useMemo(() => {
    const trail: { id: string; label: string }[] = [{ id: "root", label: "Model" }];
    const stack = [...backStack, state];
    for (let i = 0; i < stack.length; i++) {
      const s = stack[i];
      if (s.type === "overview") continue;
      if (s.type === "relationshipZoom") {
        const parts = s.bundleId.split("__");
        trail.push({
          id: `bundle-${s.bundleId}`,
          label: `${parts[0]} ${parts[1]} ${parts[2]}`,
        });
      } else if (s.type === "nodeZoom") {
        const label =
          (i === stack.length - 1 ? itemDetail?.item.shortLabel : undefined) ?? "Item";
        trail.push({ id: `item-${s.itemId}`, label });
      } else if (s.type === "traceView") {
        trail.push({
          id: `trace-${s.itemId}`,
          label: s.direction === "cause" ? "Trace cause" : "Trace consequence",
        });
      } else if (s.type === "searchFocus") {
        trail.push({ id: "search", label: "Search" });
      }
    }
    return trail;
  }, [backStack, state, overview, itemDetail]);

  const onCrumbJump = useCallback(
    (idx: number) => {
      // Root = overview
      if (idx === 0) {
        goOverview();
        return;
      }
      // Reconstruct: the trail[idx] corresponds to stack[idx - 1].
      const stack = [...backStack, state];
      const targetIdx = idx - 1;
      if (targetIdx < 0 || targetIdx >= stack.length) return;
      const newStack = stack.slice(0, targetIdx);
      setBackStack(newStack);
      setState(stack[targetIdx]);
    },
    [backStack, state, goOverview],
  );

  // Render canvas for the current state.
  const canvas = (() => {
    if (state.type === "overview") {
      if (overviewPhase === "loading" && !overview) {
        return (
          <div className="fm-loading" data-testid="model-loading">
            <SkeletonOverview />
          </div>
        );
      }
      if (overviewPhase === "error" && !overview) {
        return (
          <div className="fm-error" role="alert" data-testid="model-error">
            <h3>Model could not load.</h3>
            <p>Fyralis could not retrieve the current company model.</p>
            <button type="button" onClick={() => setMode((m) => m)}>Retry</button>
          </div>
        );
      }
      if (!overview) return null;
      if (overview.summary.activeItemCount === 0) {
        return (
          <div className="fm-empty" data-testid="model-empty">
            <h3>Fyralis is building your company model.</h3>
            <p>
              Once Fyralis has enough interpreted company state, this page will
              show categories and relationships.
            </p>
          </div>
        );
      }
      return (
        <OverviewMap
          categories={overview.categories}
          bundles={overview.relationshipBundles}
          onCategoryClick={onCategoryClick}
          onBundleClick={onBundleClick}
        />
      );
    }

    if (state.type === "relationshipZoom") {
      if (!relationshipFocus) {
        return <div className="fm-loading">Loading relationship…</div>;
      }
      return (
        <RelationshipCorridor
          focus={relationshipFocus}
          onItemClick={onItemClick}
          onCategoryClick={onCategoryClick}
        />
      );
    }

    if (state.type === "nodeZoom") {
      if (!itemDetail) {
        return <div className="fm-loading">Loading item…</div>;
      }
      return (
        <NodeNeighborhood
          detail={itemDetail}
          onNeighborClick={onItemClick}
          onTraceCause={() => onTraceCause(itemDetail.item.id)}
          onTraceConsequence={() => onTraceConsequence(itemDetail.item.id)}
          onCreateDecisionDelta={() => {
            /* surface a toast at page level in a follow-up */
          }}
          onOpenFullDetail={() => setFullDetailOpen(true)}
        />
      );
    }

    if (state.type === "traceView") {
      if (!trace) {
        return <div className="fm-loading">Loading trace…</div>;
      }
      return (
        <TracePath
          trace={trace}
          depth={state.depth}
          onDepthChange={(d) =>
            setState({ ...state, depth: d })
          }
        />
      );
    }

    return null;
  })();

  const headerSummary = overview?.summary ?? {
    activeItemCount: 0,
    changedTodayCount: 0,
    blockedCount: 0,
    contestedCount: 0,
    lastUpdatedAt: new Date().toISOString(),
  };

  return (
    <>
      <AppShell
        sidebarMode="collapsed"
        sidebar={<Sidebar activeRoute="model" mode="collapsed" />}
        main={
          <div className="fm-page" data-testid="model-page">
            <ArrowDefs />
            <ModelHeader
              summary={headerSummary}
              searchValue={search}
              onSearchChange={setSearch}
              onSearchFocus={() => pushState({ type: "searchFocus", query: search })}
            />
            <div className="fm-controls">
              <RelationshipModeBar mode={mode} onChange={setMode} />
              <div className="fm-controls__spacer" />
              {state.type !== "overview" ? (
                <>
                  <Breadcrumb trail={breadcrumb} onJump={onCrumbJump} />
                  <button
                    type="button"
                    className="fm-back"
                    onClick={goBack}
                    data-testid="model-back"
                    aria-label="Back to previous view"
                  >
                    <span aria-hidden="true">←</span> Back
                  </button>
                </>
              ) : (
                <p className="fm-controls__hint">
                  Click a category to zoom · Click an arrow to inspect a relationship
                </p>
              )}
            </div>
            {stateError ? (
              <div className="fm-toast" role="status">
                {stateError}
              </div>
            ) : null}
            <div
              className="fm-stage"
              data-state={state.type}
              key={`stage-${state.type}-${"categoryId" in state ? state.categoryId : "itemId" in state ? state.itemId : "bundleId" in state ? state.bundleId : "root"}`}
            >
              {canvas}
            </div>
          </div>
        }
      />
      {openCategoryId && categoryFocus ? (
        <CategorySheet
          focus={categoryFocus}
          onClose={() => setOpenCategoryId(null)}
          onItemClick={(id) => {
            setOpenCategoryId(null);
            pushState({ type: "nodeZoom", itemId: id });
          }}
          onBundleClick={(id) => {
            setOpenCategoryId(null);
            pushState({ type: "relationshipZoom", bundleId: id });
          }}
          onRelatedCategoryClick={(id) => setOpenCategoryId(id)}
        />
      ) : null}
      {fullDetailOpen && itemDetail && state.type === "nodeZoom" ? (
        <FullDetailSheet
          detail={itemDetail}
          onClose={() => setFullDetailOpen(false)}
        />
      ) : null}
      {state.type === "searchFocus" && overview ? (
        <SearchOverlay
          query={search}
          onQueryChange={setSearch}
          onClose={goBack}
          categories={overview.categories}
          bundles={overview.relationshipBundles}
          items={overview.categories.flatMap((c) => c.topItems)}
          onCategoryPick={(id) => {
            setBackStack((s) => s.filter((x) => x.type !== "searchFocus"));
            setState({ type: "overview" });
            setOpenCategoryId(id);
          }}
          onBundlePick={(id) => {
            setBackStack((s) => s.filter((x) => x.type !== "searchFocus"));
            setState({ type: "relationshipZoom", bundleId: id });
          }}
          onItemPick={(id) => {
            setBackStack((s) => s.filter((x) => x.type !== "searchFocus"));
            setState({ type: "nodeZoom", itemId: id });
          }}
        />
      ) : null}
    </>
  );
}

// Lightweight skeleton — 8 placeholder cards arranged in the lattice
// positions, no animated graph theatre.
function SkeletonOverview() {
  return (
    <div className="fm-skeleton" aria-hidden="true">
      {Array.from({ length: 8 }).map((_, i) => (
        <div key={i} className="fm-skeleton__card" />
      ))}
    </div>
  );
}
