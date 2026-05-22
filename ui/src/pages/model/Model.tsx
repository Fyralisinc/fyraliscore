import { useCallback, useEffect, useMemo, useState } from "react";
import { AppShell } from "@/shell/AppShell";
import { Sidebar } from "@/shell/Sidebar";
import { LayeredGraph } from "@/components/model/LayeredGraph";
import { LensRail } from "@/components/model/LensRail";
import { ModelMetricsStrip } from "@/components/model/ModelMetricsStrip";
import { GraphLegend } from "@/components/model/GraphLegend";
import { GraphControls } from "@/components/model/GraphControls";
import { NodeInspector } from "@/components/model/NodeInspector";
import {
  DEFAULT_SHOW,
  DEFAULT_STATUS,
  type LensId,
  type ShowFilters,
  type StatusFilters,
  type ViewMode,
} from "@/components/model/types";
import { getMapSnapshot } from "@/api/map-client";
import {
  getDependsOn,
  getSupports,
  trace,
} from "@/api/model-trace-client";
import { MAP_SNAPSHOT_V2_FIXTURE, NODE_META_V2, MODEL_METRICS_V2 } from "@/api/map-mock-v2";
import type { MapEdge, MapNode, MapSnapshotResponse } from "@/api/map-types";
import type { NodeMetaV2 } from "@/api/map-mock-v2";
import type { TraceStep } from "@/api/model-trace-types";
import { ApiError } from "@/api/client";

// Fyralis — Model page (spec §4).
// Lays out the live company-state map as five stacked horizontal bands:
// goals, commitments, decisions, risks, customers. The renderer is SVG
// + foreignObject so positioning is deterministic (no force-directed
// jitter) and so node tiles get full DOM styling.

const TRACE_STEP_MS = 280;
const TOAST_MS = 2600;

export default function ModelPage() {
  const [snapshot, setSnapshot] = useState<MapSnapshotResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  // Metadata is keyed on node id; in real production it would come from
  // a sidecar response. For the fixture-driven Model page we
  // pre-compute NODE_META_V2 in the mock module.
  const [metaIndex, setMetaIndex] = useState<Record<string, NodeMetaV2>>(NODE_META_V2);

  const [activeLens, setActiveLens] = useState<LensId>("company");
  const [show, setShow] = useState<ShowFilters>(DEFAULT_SHOW);
  const [status, setStatus] = useState<StatusFilters>(DEFAULT_STATUS);
  const [search, setSearch] = useState("");
  const [view, setView] = useState<ViewMode>("map");

  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [supports, setSupports] = useState<TraceStep[]>([]);
  const [dependsOn, setDependsOn] = useState<TraceStep[]>([]);
  const [highlighted, setHighlighted] = useState<Set<string>>(new Set());
  const [tracing, setTracing] = useState<"back" | "forward" | null>(null);

  const [zoom, setZoom] = useState(1);
  const [locked, setLocked] = useState(false);
  const [showGrid, setShowGrid] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  // Band-focus lens. When set, the snapshot endpoint returns up to 30
  // nodes for that band while keeping other bands trimmed. Null =
  // overview (curated 2–4 per band + +N more cluster cards).
  const [lensBand, setLensBand] = useState<
    "goal" | "commitment" | "decision" | "risk" | "customer" | null
  >(null);

  // Initial snapshot fetch. Falls back to the local fixture if the
  // backend doesn't yet return banded nodes (graceful degradation:
  // server-driven bands when present, mock-driven bands otherwise).
  // Refetches when lensBand changes so the band-focused view loads.
  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    setLoading(true);
    getMapSnapshot({ lens: lensBand ?? undefined }, controller.signal)
      .then((resp) => {
        if (cancelled) return;
        // Server contract: every snapshot includes a `band` per node.
        // If the response is empty, surface the empty state honestly.
        // Otherwise, if the server returns nodes WITHOUT bands (legacy
        // tenant), stitch the fixture so we still render something.
        if (resp.nodes.length === 0) {
          setSnapshot(resp);
        } else {
          const hasBanded = resp.nodes.some((n) => Boolean(n.band));
          if (hasBanded) {
            setSnapshot(resp);
          } else {
            setSnapshot(MAP_SNAPSHOT_V2_FIXTURE);
            setMetaIndex(NODE_META_V2);
          }
        }
        setLoadError(null);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        if ((err as Error)?.name === "AbortError") return;
        if (err instanceof ApiError) {
          setLoadError(`Failed to load model (${err.status}).`);
        } else {
          setLoadError("Failed to load model.");
        }
        // Fall back to the fixture so the page still has substance to show
        // alongside the error band; the spec says "every empty/error
        // state should explain what's missing", not "blank screen".
        setSnapshot(MAP_SNAPSHOT_V2_FIXTURE);
        setMetaIndex(NODE_META_V2);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [lensBand]);

  // When a node is selected, fetch supports + depends-on for the
  // inspector. Both endpoints are tolerant of unknown ids (empty
  // arrays), so the inspector always has something to render.
  useEffect(() => {
    if (!selectedId) {
      setSupports([]);
      setDependsOn([]);
      return;
    }
    let cancelled = false;
    const controller = new AbortController();
    Promise.all([
      getSupports(selectedId, controller.signal).catch(() => ({
        node_id: selectedId,
        items: [] as TraceStep[],
      })),
      getDependsOn(selectedId, controller.signal).catch(() => ({
        node_id: selectedId,
        items: [] as TraceStep[],
      })),
    ]).then(([s, d]) => {
      if (cancelled) return;
      setSupports(s.items);
      setDependsOn(d.items);
    });
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [selectedId]);

  const nodes = snapshot?.nodes ?? [];
  const edges = snapshot?.edges ?? [];

  const filteredNodes = useMemo(() => {
    const q = search.trim().toLowerCase();
    return nodes.filter((n) => {
      if (!n.band) return false;
      if (!show[n.band]) return false;
      const meta = metaIndex[n.id];
      const isContested = n.health === "contested";
      const isBlocked = meta?.critical || n.status === "blocked";
      const isActive = !isContested && !isBlocked;
      if (isContested && !status.contested) return false;
      if (isBlocked && !status.blocked) return false;
      if (isActive && !status.active) return false;
      if (q && !n.natural.toLowerCase().includes(q)) return false;
      return true;
    });
  }, [nodes, show, status, search, metaIndex]);

  // Edges referencing hidden nodes get suppressed so the canvas doesn't
  // strand dangling arrows.
  const filteredEdges = useMemo(() => {
    const visible = new Set(filteredNodes.map((n) => n.id));
    return edges.filter(
      (e) => visible.has(e.source) && visible.has(e.target)
    );
  }, [filteredNodes, edges]);

  const onSelect = useCallback((id: string | null) => {
    setSelectedId(id);
    setHighlighted(new Set());
    setTracing(null);
  }, []);

  const animateTrace = useCallback(
    (direction: "back" | "forward") => {
      if (!selectedId) return;
      setTracing(direction);
      // Reveal edges in sequence by walking the trace chain. Each
      // pairwise step (n_i, n_{i+1}) maps to an edge key
      // `${source}__${target}__${kind}`; the SVG layer picks up the
      // `is-highlighted` class via the highlightedEdgeKeys prop.
      trace(selectedId, direction, { maxDepth: 4 })
        .catch(() => null)
        .then((chain) => {
          if (!chain) return;
          // Compose edge keys from the chain. For "back", traversal is
          // chain[i+1] → chain[i] (supports). For "forward",
          // chain[i] → chain[i+1] (contributes_to_resolution / blocks).
          const keys: string[] = [];
          for (let i = 0; i < chain.chain.length - 1; i++) {
            const a = chain.chain[i];
            const b = chain.chain[i + 1];
            if (direction === "back") {
              // try both kinds
              keys.push(`${b.id}__${a.id}__supports`);
              keys.push(`${b.id}__${a.id}__contributes_to_resolution`);
            } else {
              keys.push(`${a.id}__${b.id}__supports`);
              keys.push(`${a.id}__${b.id}__contributes_to_resolution`);
            }
          }
          // Also highlight any direct outgoing/incoming edges from the
          // seed if the chain isn't an exact graph match — this keeps
          // the user-visible behavior aligned with the spec: "Trace
          // back highlights upstream edges".
          for (const e of filteredEdges) {
            if (direction === "back" && e.target === selectedId) {
              keys.push(`${e.source}__${e.target}__${e.kind}`);
            }
            if (direction === "forward" && e.source === selectedId) {
              keys.push(`${e.source}__${e.target}__${e.kind}`);
            }
          }
          // Reveal step by step. State updates are coalesced; this is
          // why we accumulate into a fresh Set each tick.
          const acc = new Set<string>();
          let i = 0;
          const tick = () => {
            if (i >= keys.length) return;
            acc.add(keys[i]);
            setHighlighted(new Set(acc));
            i += 1;
            window.setTimeout(tick, TRACE_STEP_MS);
          };
          tick();
        });
    },
    [filteredEdges, selectedId]
  );

  const selectedNode: MapNode | null = useMemo(() => {
    if (!selectedId) return null;
    return nodes.find((n) => n.id === selectedId) ?? null;
  }, [selectedId, nodes]);

  const inspector = selectedNode ? (
    <NodeInspector
      node={selectedNode}
      meta={metaIndex[selectedNode.id]}
      supports={supports}
      dependsOn={dependsOn}
      onClose={() => onSelect(null)}
      onTraceBack={() => animateTrace("back")}
      onTraceForward={() => animateTrace("forward")}
      onContest={() => {
        setToast("Contest flow: coming soon");
        window.setTimeout(() => setToast(null), TOAST_MS);
      }}
      onCreateDelta={() => {
        setToast("Decision Delta: coming soon");
        window.setTimeout(() => setToast(null), TOAST_MS);
      }}
      tracing={tracing}
    />
  ) : null;

  return (
    <AppShell
      sidebar={<Sidebar activeRoute="model" />}
      main={
        <div className="fy-model-page" data-testid="model-page">
          <header className="fy-model-page__header">
            <div>
              <h1 className="fy-model-page__title">Model</h1>
              <p className="fy-model-page__subtitle">
                {lensBand
                  ? `Focused on ${lensBand}s — showing every active ${lensBand} in the model.`
                  : "The live structural representation of your company."}
              </p>
              {lensBand ? (
                <button
                  type="button"
                  className="fy-model-page__lens-exit"
                  onClick={() => setLensBand(null)}
                  data-testid="lens-exit"
                >
                  ← Back to overview
                </button>
              ) : null}
            </div>
            <div className="fy-model-page__header-right">
              <span className="fy-model-page__pulse" aria-hidden="true" />
              <span className="fy-model-page__live">LIVE</span>
              <span className="fy-model-page__time">
                {new Date().toLocaleString(undefined, {
                  weekday: "short",
                  hour: "numeric",
                  minute: "2-digit",
                })}
              </span>
              <button type="button" className="fy-btn fy-btn--primary">
                Ask Fyralis
              </button>
            </div>
          </header>

          <ModelMetricsStrip
            activeNodes={MODEL_METRICS_V2.active_nodes}
            changedToday={MODEL_METRICS_V2.changed_today}
            contested={MODEL_METRICS_V2.contested}
            awaitingConfirmation={MODEL_METRICS_V2.awaiting_confirmation}
            blockedCommitments={MODEL_METRICS_V2.blocked_commitments}
            atRiskArrUsd={MODEL_METRICS_V2.at_risk_arr_usd}
          />

          <div className="fy-model-page__viewswitch" role="tablist" aria-label="View">
            {(["map", "table", "timeline"] as const).map((v) => (
              <button
                key={v}
                type="button"
                className={`fy-model-page__viewbtn${view === v ? " is-active" : ""}`}
                role="tab"
                aria-selected={view === v}
                onClick={() => setView(v)}
                data-testid={`view-${v}`}
              >
                {v[0].toUpperCase()}
                {v.slice(1)}
              </button>
            ))}
          </div>

          {loadError ? (
            <div className="fy-model-page__error" role="alert" data-testid="model-error">
              {loadError} Showing last known state.
            </div>
          ) : null}

          <div className="fy-model-page__layout">
            <LensRail
              activeLens={activeLens}
              onLensChange={setActiveLens}
              show={show}
              onShowChange={setShow}
              status={status}
              onStatusChange={setStatus}
              search={search}
              onSearchChange={setSearch}
            />

            <div className="fy-model-page__canvas">
              {view === "map" ? (
                loading && !snapshot ? (
                  <div className="fy-model-page__loading">Loading model…</div>
                ) : filteredNodes.length === 0 ? (
                  <div
                    className="fy-model-page__empty"
                    data-testid="model-empty"
                  >
                    <h3>No active nodes</h3>
                    <p>
                      Either the model has no banded entries yet, or the
                      current filters hide everything. Try clearing
                      filters or connecting more sources.
                    </p>
                  </div>
                ) : (
                  <LayeredGraph
                    nodes={filteredNodes}
                    edges={filteredEdges}
                    meta={metaIndex}
                    selectedId={selectedId}
                    onSelect={onSelect}
                    highlightedEdgeKeys={highlighted}
                    zoom={zoom}
                    showGrid={showGrid}
                    bandTotals={snapshot?.band_totals}
                    onExpandBand={(band) => {
                      setLensBand(band);
                      setSelectedId(null);
                    }}
                  />
                )
              ) : (
                <div className="fy-model-page__placeholder" data-testid={`view-${view}-placeholder`}>
                  Coming soon
                </div>
              )}

              {view === "map" ? (
                <>
                  <div className="fy-model-page__controls-wrap">
                    <GraphControls
                      zoom={zoom}
                      onZoomIn={() => setZoom((z) => Math.min(2, +(z + 0.1).toFixed(2)))}
                      onZoomOut={() => setZoom((z) => Math.max(0.5, +(z - 0.1).toFixed(2)))}
                      onFit={() => setZoom(1)}
                      locked={locked}
                      onToggleLock={() => setLocked((v) => !v)}
                      showGrid={showGrid}
                      onToggleGrid={() => setShowGrid((v) => !v)}
                    />
                  </div>
                  <div className="fy-model-page__legend-wrap">
                    <GraphLegend />
                  </div>
                </>
              ) : null}
            </div>
          </div>

          {toast ? (
            <div className="fy-model-page__toast" role="status" data-testid="toast">
              {toast}
            </div>
          ) : null}
        </div>
      }
      inspector={inspector}
    />
  );
}
