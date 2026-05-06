import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { Sidebar } from "@/components/Sidebar";
import { ShortcutsOverlay } from "@/components/ShortcutsOverlay";
import { JustUpdated } from "@/components/JustUpdated";
import { MapControls } from "@/components/structure/MapControls";
import { CommitmentList } from "@/components/structure/CommitmentList";
import { RelationshipGraph } from "@/components/structure/RelationshipGraph";
import { useToday } from "@/hooks/useToday";
import {
  SAMPLE_COMMITMENTS,
  SAMPLE_CUSTOMERS,
  SAMPLE_DECISIONS,
  SAMPLE_GOALS,
  SAMPLE_GOAL_LEARNINGS,
  SAMPLE_OWNERS,
  SAMPLE_PEOPLE,
  SAMPLE_PEOPLE_INDEX,
  SAMPLE_RESOURCES,
} from "@/components/structure/sample-data";
import type {
  Commitment,
  CommitmentStatus,
  Filters,
  FocusTarget,
  GoalRef,
  PersonProfile,
} from "@/components/structure/types";
import {
  getStructureOverlay,
  getStructureRecent,
  getStructureResourceOverlay,
  getStructureResourcesAggregate,
  type StructureOverlayCommitment,
  type StructureOverlayCustomer,
  type StructureOverlayDecision,
  type StructureOverlayGoal,
  type StructureOverlayPerson,
  type StructureOverlayResource,
  type StructureOverlayResponse,
  type StructureResourceAggregate,
  type StructureResourceOverlayResponse,
} from "@/api/structure-client";
import { ResourceAggregateView } from "@/components/structure/ResourceAggregateView";

const DAY_MS = 24 * 60 * 60 * 1000;

// Combined overlay state — accepts both the focus-by-id payload (one
// commitment) and the recent-commitments payload (many) and merges
// them by entity id. Backend models these without UI fields like
// territory/activity, so we synthesize defaults here.
type OverlayState = {
  commitments: StructureOverlayCommitment[];
  goals: StructureOverlayGoal[];
  people: StructureOverlayPerson[];
  customers: StructureOverlayCustomer[];
  decisions: StructureOverlayDecision[];
  resources: StructureOverlayResource[];
};

function emptyOverlayState(): OverlayState {
  return {
    commitments: [], goals: [], people: [], customers: [],
    decisions: [], resources: [],
  };
}

function mergeOverlayBundle(
  state: OverlayState,
  bundle: {
    commitments: StructureOverlayCommitment[];
    goals: StructureOverlayGoal[];
    people: StructureOverlayPerson[];
    customers: StructureOverlayCustomer[];
    decisions?: StructureOverlayDecision[];
    resources?: StructureOverlayResource[];
  }
): OverlayState {
  const cIds = new Set(state.commitments.map((c) => c.id));
  const gIds = new Set(state.goals.map((g) => g.id));
  const pIds = new Set(state.people.map((p) => p.id));
  const customerIds = new Set(state.customers.map((c) => c.id));
  const decisionIds = new Set(state.decisions.map((d) => d.id));
  const resourceIds = new Set(state.resources.map((r) => r.id));
  return {
    commitments: [
      ...bundle.commitments.filter((c) => !cIds.has(c.id)),
      ...state.commitments,
    ],
    goals: [
      ...state.goals,
      ...bundle.goals.filter((g) => !gIds.has(g.id)),
    ],
    people: [
      ...state.people,
      ...bundle.people.filter((p) => !pIds.has(p.id)),
    ],
    customers: [
      ...state.customers,
      ...bundle.customers.filter((c) => !customerIds.has(c.id)),
    ],
    decisions: [
      ...state.decisions,
      ...(bundle.decisions ?? []).filter((d) => !decisionIds.has(d.id)),
    ],
    resources: [
      ...state.resources,
      ...(bundle.resources ?? []).filter((r) => !resourceIds.has(r.id)),
    ],
  };
}

function adaptOverlayCommitment(
  c: StructureOverlayCommitment,
  customerLabel: string | null,
  todayIso: string
): Commitment {
  const ownerId = c.owner ?? "unknown-owner";
  const ownerLabel = c.owner_display ?? "Owner";
  const territory = c.customer ? "customer-facing" : "strategic";
  return {
    id: c.id,
    label: c.label,
    territory,
    owner: ownerId,
    owner_display: ownerLabel,
    due_date: c.due_date ?? todayIso,
    created_date: todayIso,
    status: c.status,
    priority: c.priority,
    stakeholder: c.customer ? "customer" : "internal",
    stakeholder_label: c.customer_label ?? customerLabel ?? "Internal",
    customer: c.customer ?? undefined,
    traces_to: c.edges.constrained_by,
    related: [],
    edges: {
      contributes_to: c.edges.contributes_to,
      constrained_by: c.edges.constrained_by,
      consumes: c.edges.consumes,
      contributors: c.edges.contributors,
    },
    consumed_resources: (c.consumed_resources ?? []).map((r) => ({
      id: r.id,
      label: r.label,
      kind: r.kind,
      unit: r.unit ?? null,
      deployed_quantity: r.deployed_quantity ?? null,
    })),
    progress: undefined,
    substrate_insight: c.substrate_insight ?? undefined,
    activity: c.activity && c.activity.length > 0
      ? c.activity
      : [],
    learnings: (c.learnings ?? []).map((p) => ({
      id: p.id,
      statement: p.statement,
      strength: p.strength,
      evidence: p.evidence.map((e) => ({ when: e.when, text: e.text })),
    })),
  };
}

function adaptOverlayPerson(p: StructureOverlayPerson): PersonProfile {
  return {
    id: p.id,
    label: p.label,
    role: p.role,
    recent_observation: "Newly assigned via accepted recommendation.",
    calibration: 0.6,
    patterns: [],
  };
}

// Driftwood — Structure page. One view: relational (list rail + graph).
// The Lanes / Two-axis modes were removed; relational is the only mode.
export default function Structure() {
  const navigate = useNavigate();
  const now = useMemo(() => new Date(), []);
  const todayIso = useMemo(() => now.toISOString().slice(0, 10), [now]);
  const [searchParams, setSearchParams] = useSearchParams();
  const focusParam = searchParams.get("focus");
  const { today, dismissJustUpdated } = useToday();
  const [filters, setFilters] = useState<Filters>(() => ({
    entityKind: "all",
    time: "quarter",
    statuses: new Set<CommitmentStatus>([
      "on-track", "slipping", "at-risk", "blocked",
    ]),
    owner: null,
    customer: null,
  }));
  const [focus, setFocus] = useState<FocusTarget | null>(null);
  const [hoveredCommitmentId, setHoveredCommitmentId] = useState<string | null>(null);
  const [shortcutsOpen, setShortcutsOpen] = useState(false);
  const [listOpen, setListOpen] = useState(true);
  const [overlayState, setOverlayState] = useState<OverlayState>(() =>
    emptyOverlayState()
  );
  const [overlayError, setOverlayError] = useState<string | null>(null);
  // Aggregate resource portfolio (capacity + utilization). Fetched
  // once on mount; refreshed every 30s while the page is visible.
  const [resourceAggregate, setResourceAggregate] = useState<
    StructureResourceAggregate[]
  >([]);
  // Per-resource focus payload — only populated when focus.kind === "resource".
  const [resourceFocus, setResourceFocus] = useState<
    StructureResourceOverlayResponse | null
  >(null);
  // Tracks whether the initial /v1/structure/recent fetch has completed.
  // While in-flight we suppress the SAMPLE_* fallback graph so the user
  // doesn't see a placeholder count (e.g., ~47) flash before the real
  // tenant graph (e.g., 141 for Pelago) loads.
  const [initialFetchPending, setInitialFetchPending] = useState(true);

  // On mount, pull every active commitment for the tenant so the
  // graph reflects the loaded snapshot, not the SAMPLE_* placeholder
  // graph. After that, every 8s while visible, fetch the last 15
  // minutes' worth of changes so auto-accepted commitments surface
  // without a manual reload.
  useEffect(() => {
    let alive = true;
    let firstFetchDone = false;
    async function fetchStructure(initial: boolean) {
      if (!initial && document.hidden) return;
      try {
        // initial load: since_minutes=0 → all active commitments.
        // subsequent polls: 15-minute window for live changes.
        const res = await getStructureRecent(initial ? 0 : 15);
        if (!alive) return;
        if (!initial && res.commitments.length === 0) return;
        setOverlayState((prev) =>
          mergeOverlayBundle(prev, {
            commitments: res.commitments,
            goals: res.goals,
            people: res.people,
            customers: res.customers,
            decisions: res.decisions,
            resources: res.resources,
          })
        );
      } catch {
        // Surface only persistent failures; one-off transients shouldn't
        // be loud.
      } finally {
        firstFetchDone = true;
        if (initial && alive) setInitialFetchPending(false);
      }
    }
    async function fetchAggregate() {
      try {
        const res = await getStructureResourcesAggregate();
        if (!alive) return;
        setResourceAggregate(res.resources);
      } catch {
        // The aggregate endpoint can be missing in older backends or
        // for tenants without capacity resources — fail silently.
      }
    }
    void fetchStructure(true);
    void fetchAggregate();
    const id = window.setInterval(() => {
      if (firstFetchDone) void fetchStructure(false);
    }, 8000);
    const aggId = window.setInterval(() => {
      if (!document.hidden) void fetchAggregate();
    }, 30000);
    return () => {
      alive = false;
      window.clearInterval(id);
      window.clearInterval(aggId);
    };
  }, []);

  // When focus is a resource, fetch its overlay (consumers + owners)
  // so the focus view can render edges to consuming commitments.
  useEffect(() => {
    if (focus?.kind !== "resource") {
      setResourceFocus(null);
      return;
    }
    const ctrl = new AbortController();
    let alive = true;
    (async () => {
      try {
        const res = await getStructureResourceOverlay(focus.id, ctrl.signal);
        if (!alive) return;
        setResourceFocus(res);
      } catch (err) {
        if (!alive) return;
        if (err instanceof Error && err.name === "AbortError") return;
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, [focus?.kind, focus?.id]);

  // When ?focus=<id> points to a commitment that's not yet in the
  // overlay state or the sample data, fetch the single-commitment
  // overlay and merge it in. Always advance the focus state so the
  // graph centers on the targeted commitment.
  useEffect(() => {
    if (!focusParam) {
      setOverlayError(null);
      return;
    }
    if (
      SAMPLE_COMMITMENTS.some((c) => c.id === focusParam) ||
      overlayState.commitments.some((c) => c.id === focusParam)
    ) {
      setOverlayError(null);
      setFocus({ kind: "commitment", id: focusParam });
      return;
    }
    const ctrl = new AbortController();
    let alive = true;
    (async () => {
      try {
        const res = await getStructureOverlay(focusParam, ctrl.signal);
        if (!alive) return;
        setOverlayState((prev) =>
          mergeOverlayBundle(prev, {
            commitments: [res.commitment],
            goals: res.goals,
            people: res.people,
            customers: res.customers,
            decisions: res.decisions,
          })
        );
        setOverlayError(null);
        setFocus({ kind: "commitment", id: res.commitment.id });
      } catch (err) {
        if (!alive) return;
        if (err instanceof Error && err.name === "AbortError") return;
        setOverlayError(
          err instanceof Error ? err.message : "overlay fetch failed"
        );
      }
    })();
    return () => {
      alive = false;
      ctrl.abort();
    };
  }, [focusParam, overlayState.commitments]);

  const overlayCustomerLabelById = useMemo(() => {
    const m = new Map<string, string>();
    for (const c of overlayState.customers) m.set(c.id, c.label);
    return m;
  }, [overlayState.customers]);

  const overlayCommitments = useMemo<Commitment[]>(() => {
    return overlayState.commitments.map((c) =>
      adaptOverlayCommitment(
        c,
        c.customer ? overlayCustomerLabelById.get(c.customer) ?? null : null,
        todayIso
      )
    );
  }, [overlayState.commitments, overlayCustomerLabelById, todayIso]);

  const overlayPeople = useMemo<PersonProfile[]>(() => {
    return overlayState.people.map(adaptOverlayPerson);
  }, [overlayState.people]);

  // When overlay has any data, the graph reflects the real tenant.
  // SAMPLE_* is the no-API fallback for the static dev mode only —
  // and we suppress it while the initial fetch is still pending so
  // the user never sees a placeholder count flash before the real
  // tenant graph lands.
  const hasOverlayData =
    overlayCommitments.length > 0 ||
    overlayState.goals.length > 0 ||
    overlayState.people.length > 0 ||
    overlayState.customers.length > 0 ||
    overlayState.decisions.length > 0;
  const useApiData = hasOverlayData || initialFetchPending;

  const allDecisions = useMemo(() => {
    if (useApiData) {
      return overlayState.decisions.map((d) => ({
        id: d.id,
        label: d.label,
        state: d.state,
      }));
    }
    return SAMPLE_DECISIONS;
  }, [useApiData, overlayState.decisions]);

  const allCommitments = useMemo(() => {
    if (useApiData) return overlayCommitments;
    return SAMPLE_COMMITMENTS;
  }, [useApiData, overlayCommitments]);

  const allGoals = useMemo(() => {
    if (useApiData) return overlayState.goals;
    return SAMPLE_GOALS;
  }, [useApiData, overlayState.goals]);

  const allPeople = useMemo(() => {
    if (useApiData) return overlayPeople;
    return SAMPLE_PEOPLE;
  }, [useApiData, overlayPeople]);

  const allOwners = useMemo(() => {
    if (useApiData) {
      return overlayState.people.map((p) => ({ id: p.id, label: p.label }));
    }
    return SAMPLE_OWNERS;
  }, [useApiData, overlayState.people]);

  const allCustomers = useMemo(() => {
    if (useApiData) return overlayState.customers;
    return SAMPLE_CUSTOMERS;
  }, [useApiData, overlayState.customers]);

  // Resources for the graph. Prefer the aggregate-endpoint payload
  // when present (carries capacity + utilization), fall back to the
  // bare entries that came in via the recent-commitments overlay,
  // and finally the static SAMPLE_RESOURCES for the no-API mode.
  const allResources = useMemo(() => {
    if (useApiData) {
      if (resourceAggregate.length > 0) {
        return resourceAggregate.map((r) => ({
          id: r.id,
          label: r.label,
          kind: r.kind,
          unit: r.unit,
          capacity: r.capacity,
          deployed: r.deployed,
          utilization_pct: r.utilization_pct,
          deployments_count: r.deployments_count,
          health: r.health,
        }));
      }
      // Even before the aggregate endpoint resolves, the recent-overlay
      // payload carries resource id/label/kind so the graph can still
      // render resource chips on commit focus.
      return overlayState.resources.map((r) => ({
        id: r.id,
        label: r.label,
        kind: r.kind,
        unit: r.unit ?? null,
      }));
    }
    return SAMPLE_RESOURCES;
  }, [useApiData, resourceAggregate, overlayState.resources]);

  const allPeopleIndex = useMemo(() => {
    if (useApiData) {
      const idx: Record<string, PersonProfile> = {};
      for (const p of overlayPeople) idx[p.id] = p;
      return idx;
    }
    return SAMPLE_PEOPLE_INDEX;
  }, [useApiData, overlayPeople]);

  const newestOverlayCommitment = overlayCommitments[0];

  const maxDaysVisible =
    filters.time === "next-7" ? 7 : filters.time === "all" ? 365 : 90;

  const visibleCommitments = useMemo(() => {
    return allCommitments.filter((c) => {
      if (!filters.statuses.has(c.status)) return false;
      if (filters.owner && c.owner !== filters.owner) return false;
      if (filters.customer && c.customer !== filters.customer) return false;
      const days = (new Date(c.due_date).getTime() - now.getTime()) / DAY_MS;
      if (days > maxDaysVisible) return false;
      return true;
    });
  }, [allCommitments, filters, maxDaysVisible, now]);

  // Goals are filtered indirectly: hide goals with zero contributing
  // commitments after the commitment filter is applied (so the list
  // stays in sync with what's actually visible). When customer/owner
  // filters are set, goals only show if they have at least one
  // remaining commitment.
  const visibleGoals = useMemo(() => {
    if (filters.owner === null && filters.customer === null) return allGoals;
    const linked = new Set<string>();
    for (const c of visibleCommitments) {
      for (const gid of c.edges?.contributes_to ?? []) linked.add(gid);
    }
    return allGoals.filter((g) => linked.has(g.id));
  }, [allGoals, filters.owner, filters.customer, visibleCommitments]);

  // People mirror the goal logic: when no owner/customer filter is set,
  // show the whole team. Otherwise narrow to people who own or contribute
  // to a currently-visible commitment, plus the explicitly-filtered owner.
  const visiblePeople = useMemo(() => {
    if (filters.owner === null && filters.customer === null) return allPeople;
    const linked = new Set<string>();
    for (const c of visibleCommitments) {
      linked.add(c.owner);
      for (const cid of c.edges?.contributors ?? []) linked.add(cid);
    }
    if (filters.owner) linked.add(filters.owner);
    return allPeople.filter((p) => linked.has(p.id));
  }, [allPeople, filters.owner, filters.customer, visibleCommitments]);

  useEffect(() => {
    function onKey(e: KeyboardEvent) {
      const tag = (document.activeElement as HTMLElement | null)?.tagName;
      const isInput =
        tag === "INPUT" || tag === "TEXTAREA" ||
        (document.activeElement as HTMLElement | null)?.isContentEditable;
      if (e.key === "Escape") {
        if (shortcutsOpen) {
          setShortcutsOpen(false);
          e.preventDefault();
          return;
        }
        if (focus) {
          setFocus(null);
          e.preventDefault();
        }
        return;
      }
      if (isInput) return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === "?") {
        e.preventDefault();
        setShortcutsOpen(true);
      }
      if (e.key === "l" || e.key === "L") {
        e.preventDefault();
        setListOpen((v) => !v);
      }
    }
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [focus, shortcutsOpen]);

  const nav = useMemo(
    () => [
      {
        id: "primary",
        label: "Surfaces",
        items: [
          { id: "today", label: "Today", active: false, href: "/" },
          { id: "structure", label: "Structure", active: true },
          { id: "history", label: "History", active: false },
        ],
      },
    ],
    []
  );

  return (
    <>
      <div className="cockpit">
        <Sidebar
          brand={{ name: "Fyralis", mark: "F", pulse_day: 3 }}
          nav={nav}
          onBrandClick={() => {
            // Reset Structure to its default view: no focus, no filter.
            setFocus(null);
            setHoveredCommitmentId(null);
            setFilters({
              entityKind: "all",
              time: "quarter",
              statuses: new Set<CommitmentStatus>([
                "on-track", "slipping", "at-risk", "blocked",
              ]),
              owner: null,
              customer: null,
            });
          }}
          onNavigate={(_s, item) => {
            if (item === "today") navigate("/");
            else if (item === "structure") navigate("/structure");
            else if (item === "history") navigate("/history");
          }}
        />

        <main className="structure-main">
          {today?.just_updated ? (
            <JustUpdated
              text_html={today.just_updated.text_html}
              onDismiss={dismissJustUpdated}
            />
          ) : null}

          {newestOverlayCommitment ? (
            <JustUpdated
              text_html={`Just tracked: <strong>${newestOverlayCommitment.label}</strong>${
                overlayCommitments.length > 1
                  ? ` (+${overlayCommitments.length - 1} more)`
                  : newestOverlayCommitment.stakeholder_label &&
                    newestOverlayCommitment.stakeholder === "customer"
                  ? ` — ${newestOverlayCommitment.stakeholder_label}`
                  : ""
              }`}
              onDismiss={() => {
                setOverlayState(emptyOverlayState());
                setSearchParams((prev) => {
                  const next = new URLSearchParams(prev);
                  next.delete("focus");
                  return next;
                });
              }}
            />
          ) : null}

          {overlayError ? (
            <div className="just-updated" role="status">
              <span>Couldn't load the new commitment ({overlayError}).</span>
            </div>
          ) : null}

          <MapControls
            filters={filters}
            ownerOptions={allOwners}
            customerOptions={allCustomers}
            onFiltersChange={setFilters}
            listOpen={listOpen}
            onToggleList={() => setListOpen((v) => !v)}
          />

          <div className={"relational-shell" + (listOpen ? " list-open" : "")}>
            <CommitmentList
              commitments={visibleCommitments}
              goals={visibleGoals}
              people={visiblePeople}
              entityKind={filters.entityKind}
              focus={focus}
              onFocus={setFocus}
              onHover={setHoveredCommitmentId}
            />
            {filters.entityKind === "resources" && focus === null
              && resourceAggregate.length > 0 ? (
              <ResourceAggregateView
                resources={resourceAggregate}
                onFocus={(rid) => setFocus({ kind: "resource", id: rid })}
              />
            ) : (
              <RelationshipGraph
                commitments={visibleCommitments}
                goals={visibleGoals}
                decisions={allDecisions}
                resources={allResources}
                resourceFocus={resourceFocus}
                peopleIndex={allPeopleIndex}
                goalLearnings={SAMPLE_GOAL_LEARNINGS}
                ownerLabels={Object.fromEntries(
                  allOwners.map((o) => [o.id, o.label])
                )}
                focus={focus}
                hoveredCommitmentId={hoveredCommitmentId}
                onFocus={setFocus}
              />
            )}
          </div>
        </main>
      </div>

      {shortcutsOpen ? (
        <ShortcutsOverlay onClose={() => setShortcutsOpen(false)} />
      ) : null}
    </>
  );
}
