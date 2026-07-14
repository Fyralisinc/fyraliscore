"""Query-conditioned Synthesis Reader for the SAGE loop.

This is the production bridge across SAGE Phases 2-8:

1. structured cue extraction,
2. retrieval intent inference,
3. soft activation from shortcuts, affordances, lexical/entity cues,
4. structurally gated propagation,
5. compact subgraph selection,
6. node-to-evidence projection.

The reader only reads canonical Synthesis tables. Its explainable
activation trace is returned to the caller for persistence in the
Discovery Utility Layer after the inquiry session row exists.
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow, ObservationRow
from services.domain.models.repo import _SELECT_COLS_SQL as _MODEL_SELECT_COLS_SQL
from services.domain.models.repo import _hydrate_row as _hydrate_model_row
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.sage.affordances.repo import AffordanceProfilesRepo
from services.reasoning.sage.cue_extractor import CueExtractor, StructuredCues
from services.reasoning.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.reasoning.sage.discovery.shortcuts_repo import DiscoveryShortcutsRepo
from services.reasoning.sage.evidence_projection import (
    EvidenceProjector,
    ProjectionBudget,
    ProjectionResult,
)
from services.reasoning.sage.intent_inferer import (
    RetrievalIntent,
    RetrievalIntentInferer,
)
from services.reasoning.sage.structural_features.types import (
    EdgeStructuralFeatures,
    ModelStructuralFeatures,
)
from services.reasoning.sage.structural_gates import GateInputs, StructuralGateScorer
from services.reasoning.sage.subgraph_selector import (
    ActivatedNode,
    CandidateEdge,
    SelectionBudget,
    SubgraphSelection,
    SubgraphSelector,
)
from services.reasoning.synthesis.query_understanding import (
    alternative_terms,
    compact_alternative_key,
    extract_query_alternatives,
)
from services.reasoning.synthesis.operational_facets import infer_operational_query_plan


_log = structlog.get_logger(__name__)
_MAX_LEXICAL_DISCOVERY_PATTERNS = 12
_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS = 1500
_SPARSE_STRONG_SINGLE_MATCH_MAX_DF = 32
_ANSWERABILITY_TERM_DF_PROBE_CAP = 1024


def _answerability_max_term_df(limit: int) -> int:
    return max(128, min(_ANSWERABILITY_TERM_DF_PROBE_CAP, max(1, int(limit)) * 64))


@dataclass(frozen=True, slots=True)
class ReaderBudget:
    max_nodes: int = 80
    max_edges: int = 120
    max_evidence_items: int = 100
    lexical_candidates: int = 40
    shortcut_candidates: int = 12
    affordance_candidates: int = 40
    propagation_neighbors: int = 80
    learned_planning_enabled: bool = True
    focused_lexical_candidates: int = 8
    focused_max_nodes: int = 24
    focused_max_edges: int = 48
    focused_propagation_neighbors: int = 32
    abstain_negative_memory_threshold: int = 3
    activation_seed_limit: int = 80
    row_cache_enabled: bool = False
    shared_substrate_enabled: bool = True
    substrate_model_limit: int = 96
    substrate_edge_seed_limit: int = 48
    substrate_edge_limit: int = 96
    rerank_min_substrate_models: int = 8
    rerank_lexical_candidates: int = 6
    lexical_microquery_enabled: bool = True
    lexical_microquery_terms: int = 8
    lexical_microquery_per_term_limit: int = 16


class _BoundedLookupRows(list[asyncpg.Record]):
    def __init__(
        self,
        rows: list[asyncpg.Record] | None = None,
        *,
        timed_out: bool = False,
    ) -> None:
        super().__init__(rows or [])
        self.timed_out = timed_out


def _bounded_lookup_timed_out(rows: object) -> bool:
    return bool(getattr(rows, "timed_out", False))


@dataclass(frozen=True, slots=True)
class ReaderActivationTrace:
    question_id: str
    model_id: UUID
    activation_score: float
    activation_reasons: tuple[str, ...]
    selected: bool = False
    selection_rank: int | None = None
    source_breakdown: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SynthesisReaderResult:
    question_id: str
    question_primitive: str
    signature: dict[str, Any]
    cues: StructuredCues
    intents: tuple[RetrievalIntent, ...]
    activations: tuple[ReaderActivationTrace, ...]
    selection: SubgraphSelection
    projected_evidence: tuple[dict[str, Any], ...]
    omitted_projection: tuple[tuple[str, str], ...]
    models: tuple[ModelRow, ...]
    observations: tuple[ObservationRow, ...]
    model_scores: dict[UUID, float]
    pathway_result: PathwayResult
    debug: dict[str, Any]


@dataclass(slots=True)
class SageReadSubstrate:
    """Reusable, signal-scoped read material.

    This is intentionally not an answer context. Question-specific cues,
    signatures, learned plans, scoring, selection, and projection still happen
    inside each read. The substrate only carries material that is invariant for
    all questions spawned by the same signal.
    """

    tenant_id: UUID
    aliases: dict[str, str] | None = None
    baseline_model_ids: tuple[UUID, ...] = ()
    candidate_edges_by_key: dict[tuple[tuple[UUID, ...], int], list[dict[str, Any]]] = (
        field(default_factory=dict)
    )
    counters: Counter[str] = field(default_factory=Counter)
    timings_ms: dict[str, int] = field(default_factory=dict)

    @property
    def model_count(self) -> int:
        return len(self.baseline_model_ids)


@dataclass(frozen=True, slots=True)
class _LearnedReadPlan:
    mode: str = "default"
    skip_broad_discovery: bool = False
    gate_broad_actions: bool = False
    abstain_early: bool = False
    lexical_candidates: int | None = None
    max_nodes: int | None = None
    max_edges: int | None = None
    propagation_neighbors: int | None = None
    confidence: float = 0.0
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _NegativeMemorySignal:
    count: int = 0
    suppressed_count: int = 0


@dataclass(frozen=True, slots=True)
class _ReaderPolicyMemory:
    shortcut_hits: int
    contextual_affordance_hits: int
    negative_memory: _NegativeMemorySignal
    learned_plan: _LearnedReadPlan
    stage_started: float


@dataclass(frozen=True, slots=True)
class _ReaderGraphRows:
    candidate_count_before_edge_seed: int
    edge_seed_limit: int
    seed_ids: list[UUID]
    edges: list[dict[str, Any]]
    models: dict[UUID, ModelRow]
    features: dict[UUID, ModelStructuralFeatures]
    edge_features: dict[UUID, EdgeStructuralFeatures]


@dataclass(frozen=True, slots=True)
class _ReaderScoredGraph:
    gate_edges: list[CandidateEdge]
    gate_debug: dict[str, dict[str, Any]]
    activated_nodes: list[ActivatedNode]
    activation_details: dict[UUID, "_CandidateScore"]


@dataclass(frozen=True, slots=True)
class _ReaderProjectedSelection:
    selection: SubgraphSelection
    selected_model_ids: list[UUID]
    projection_budget: ProjectionBudget
    projection: ProjectionResult
    projected_dicts: tuple[dict[str, Any], ...]
    observations: list[ObservationRow]


def _mark_reader_stage(
    timings: dict[str, int],
    stage: str,
    started: float,
) -> float:
    now = time.perf_counter()
    timings[stage] = int((now - started) * 1000)
    return now


class SynthesisReader:
    """Rule-based v1 SAGE reader.

    The reader is deliberately conservative: every learned signal is a
    retrieval utility boost, never a canonical write. Missing SAGE tables
    degrade to empty shortcut/affordance inputs so deployments can roll
    migrations forward safely.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None = None,
        budget: ReaderBudget | None = None,
    ) -> None:
        self._pool = pool
        self._budget = budget or ReaderBudget()
        self._intent_inferer = RetrievalIntentInferer()
        self._gate_scorer = StructuralGateScorer()
        self._selector = SubgraphSelector(
            budget=SelectionBudget(
                max_nodes=self._budget.max_nodes,
                max_edges=self._budget.max_edges,
                max_summarized_hubs=10,
            )
        )
        self._alias_cache: dict[UUID, dict[str, str]] = {}
        self._model_cache: defaultdict[UUID, dict[UUID, ModelRow]] = defaultdict(dict)
        self._model_feature_cache: defaultdict[
            UUID, dict[UUID, ModelStructuralFeatures | None]
        ] = defaultdict(dict)
        self._edge_feature_cache: defaultdict[
            UUID, dict[UUID, EdgeStructuralFeatures | None]
        ] = defaultdict(dict)
        self._observation_cache: defaultdict[
            UUID, dict[UUID, ObservationRow | None]
        ] = defaultdict(dict)
        self._cache_stats: Counter[str] = Counter()

    def cache_stats_snapshot(self) -> dict[str, int]:
        return {key: int(value) for key, value in sorted(self._cache_stats.items())}

    async def prepare_substrate(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        trigger: TriggerContext,
        baseline_models: tuple[ModelRow, ...] | list[ModelRow] = (),
    ) -> SageReadSubstrate:
        """Build the signal-scoped substrate reused by question reads."""
        substrate = SageReadSubstrate(tenant_id=tenant_id)
        if not self._budget.shared_substrate_enabled:
            substrate.counters["disabled"] += 1
            return substrate

        started = time.perf_counter()
        substrate.aliases = await self._load_aliases_cached(conn, tenant_id)
        substrate.timings_ms["aliases_ms"] = int((time.perf_counter() - started) * 1000)

        model_started = time.perf_counter()
        baseline_ids: list[UUID] = []
        baseline_by_id: dict[UUID, ModelRow] = {}
        for model in baseline_models:
            model_id = getattr(model, "id", None)
            if model_id is None or model_id in baseline_by_id:
                continue
            baseline_by_id[model_id] = model
            baseline_ids.append(model_id)
            if len(baseline_ids) >= max(1, int(self._budget.substrate_model_limit)):
                break
        for model_id in _explicit_seed_ids(trigger):
            if model_id not in baseline_by_id and model_id not in baseline_ids:
                baseline_ids.append(model_id)

        substrate.baseline_model_ids = tuple(baseline_ids)
        if self._budget.row_cache_enabled and baseline_by_id:
            self._model_cache[tenant_id].update(baseline_by_id)
            substrate.counters["models_seeded"] += len(baseline_by_id)
        if baseline_ids:
            await self._load_model_features_cached(conn, tenant_id, baseline_ids)
        substrate.timings_ms["models_ms"] = int(
            (time.perf_counter() - model_started) * 1000
        )

        edge_started = time.perf_counter()
        edge_seed_ids = baseline_ids[
            : max(0, int(self._budget.substrate_edge_seed_limit))
        ]
        if edge_seed_ids:
            await self._load_candidate_edges_for_read(
                conn,
                tenant_id=tenant_id,
                seed_model_ids=edge_seed_ids,
                limit=max(1, int(self._budget.substrate_edge_limit)),
                substrate=substrate,
            )
        substrate.timings_ms["edges_ms"] = int(
            (time.perf_counter() - edge_started) * 1000
        )
        substrate.timings_ms["total_ms"] = int((time.perf_counter() - started) * 1000)
        return substrate

    async def _load_aliases_for_read(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        substrate: SageReadSubstrate | None,
    ) -> dict[str, str]:
        if substrate is not None and substrate.aliases is not None:
            substrate.counters["alias_reuses"] += 1
            return dict(substrate.aliases)
        return await self._load_aliases_cached(conn, tenant_id)

    async def _load_candidate_edges_for_read(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        seed_model_ids: list[UUID],
        limit: int,
        substrate: SageReadSubstrate | None,
    ) -> list[dict[str, Any]]:
        ids = list(dict.fromkeys(seed_model_ids))
        if not ids:
            return []
        effective_limit = max(1, int(limit))
        key = (tuple(sorted(ids, key=str)), effective_limit)
        if substrate is not None and key in substrate.candidate_edges_by_key:
            substrate.counters["edge_cache_hits"] += 1
            return [dict(row) for row in substrate.candidate_edges_by_key[key]]
        rows = await _load_candidate_edges(
            conn,
            tenant_id=tenant_id,
            seed_model_ids=ids,
            limit=effective_limit,
        )
        if substrate is not None:
            substrate.candidate_edges_by_key[key] = [dict(row) for row in rows]
            substrate.counters["edge_cache_misses"] += 1
        return rows

    async def read(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        trigger: TriggerContext,
        question_id: str,
        question: str,
        question_primitive: str,
        hypotheses: tuple[Any, ...] = (),
        substrate: SageReadSubstrate | None = None,
        evidence_state: dict[str, Any] | None = None,
    ) -> SynthesisReaderResult:
        timings: dict[str, int] = {}
        degraded_sources: list[dict[str, Any]] = []
        cache_stats_started = Counter(self._cache_stats)
        read_started = time.perf_counter()

        stage_started = time.perf_counter()
        cues = await CueExtractor(
            pool=self._pool,
            tenant_id=tenant_id,
            alias_loader=(
                lambda: self._load_aliases_for_read(conn, tenant_id, substrate)
            ),
        ).extract(
            signal=_signal_payload(trigger),
            question=question,
            hypotheses=[_hypothesis_text(h) for h in hypotheses],
            evidence_state=evidence_state,
        )
        stage_started = _mark_reader_stage(timings, "cue_extraction_ms", stage_started)
        intents = tuple(
            self._intent_inferer.infer(
                cues=cues,
                evidence_state=evidence_state,
                question_id=question_id,
                question_text=question,
            )
        )
        primitive = _coarse_primitive(question_primitive, intents)
        signature = _signature_for(trigger, primitive, cues)
        stage_started = _mark_reader_stage(
            timings, "intent_signature_ms", stage_started
        )

        candidates = _CandidateAccumulator()
        _add_explicit_trigger_models(candidates, trigger)
        _add_substrate_seed_models(candidates, substrate, self._budget)
        stage_started = _mark_reader_stage(timings, "explicit_seed_ms", stage_started)
        policy_memory = await self._activate_policy_memory(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            primitive=primitive,
            signature=signature,
            candidates=candidates,
            substrate=substrate,
            timings=timings,
            degraded_sources=degraded_sources,
            stage_started=stage_started,
        )
        stage_started = policy_memory.stage_started
        learned_plan = policy_memory.learned_plan
        if learned_plan.abstain_early:
            return _empty_reader_result(
                question_id=question_id,
                primitive=primitive,
                signature=signature,
                cues=cues,
                intents=intents,
                timings=timings,
                read_started=read_started,
                learned_plan=learned_plan,
                degraded_sources=degraded_sources,
                evidence_state=evidence_state,
            )

        lexical_activation_stats = await self._activate_from_lexical_scan(
            conn,
            tenant_id,
            question,
            trigger,
            cues,
            candidates,
            limit=learned_plan.lexical_candidates,
        )
        stage_started = _mark_reader_stage(
            timings, "lexical_activation_ms", stage_started
        )
        if learned_plan.skip_broad_discovery:
            timings["belief_address_activation_ms"] = 0
            timings["operational_facet_activation_ms"] = 0
            timings["alternative_activation_ms"] = 0
        else:
            await self._activate_from_belief_addresses(
                conn,
                tenant_id,
                primitive,
                question,
                trigger,
                cues,
                candidates,
            )
            stage_started = _mark_reader_stage(
                timings, "belief_address_activation_ms", stage_started
            )
            await self._activate_from_operational_facets(
                conn,
                tenant_id,
                question,
                candidates,
            )
            stage_started = _mark_reader_stage(
                timings, "operational_facet_activation_ms", stage_started
            )
            await self._activate_from_alternative_scan(
                conn,
                tenant_id,
                question,
                candidates,
            )
            stage_started = _mark_reader_stage(
                timings, "alternative_activation_ms", stage_started
            )

        graph, stage_started = await self._load_reader_graph_rows(
            conn=conn,
            tenant_id=tenant_id,
            trigger=trigger,
            candidates=candidates,
            learned_plan=learned_plan,
            substrate=substrate,
            timings=timings,
            stage_started=stage_started,
        )
        scored, stage_started = self._score_reader_graph(
            graph=graph,
            candidates=candidates,
            primitive=primitive,
            intents=intents,
            question=question,
            trigger=trigger,
            cues=cues,
            timings=timings,
            stage_started=stage_started,
        )
        projected = await self._select_and_project_reader_graph(
            conn=conn,
            tenant_id=tenant_id,
            primitive=primitive,
            learned_plan=learned_plan,
            scored=scored,
            timings=timings,
            stage_started=stage_started,
        )

        return _build_reader_result(
            budget=self._budget,
            question_id=question_id,
            primitive=primitive,
            signature=signature,
            cues=cues,
            intents=intents,
            graph=graph,
            scored=scored,
            projected=projected,
            substrate=substrate,
            lexical_activation_stats=lexical_activation_stats,
            degraded_sources=degraded_sources,
            cache_stats_delta=_counter_delta(self._cache_stats, cache_stats_started),
            timings=timings,
            read_started=read_started,
            learned_plan=learned_plan,
            evidence_state=evidence_state,
        )

    async def _load_reader_graph_rows(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        trigger: TriggerContext,
        candidates: "_CandidateAccumulator",
        learned_plan: _LearnedReadPlan,
        substrate: SageReadSubstrate | None,
        timings: dict[str, int],
        stage_started: float,
    ) -> tuple[_ReaderGraphRows, float]:
        candidate_count_before_edge_seed = len(candidates.model_ids)
        edge_seed_limit = _edge_seed_limit(self._budget, learned_plan)
        seed_ids = candidates.ranked_model_ids(
            limit=edge_seed_limit,
            required_ids=_explicit_seed_ids(trigger),
        )
        edges = await self._load_candidate_edges_for_read(
            conn,
            tenant_id=tenant_id,
            seed_model_ids=seed_ids,
            limit=learned_plan.propagation_neighbors
            or self._budget.propagation_neighbors,
            substrate=substrate,
        )
        stage_started = _mark_reader_stage(
            timings, "load_candidate_edges_ms", stage_started
        )
        model_ids = set(seed_ids)
        for edge in edges:
            model_ids.add(edge["source_model_id"])
            model_ids.add(edge["target_model_id"])

        models = await self._load_models_cached(
            conn, tenant_id, sorted(model_ids, key=str)
        )
        stage_started = _mark_reader_stage(timings, "load_models_ms", stage_started)
        features = await self._load_model_features_cached(conn, tenant_id, list(models))
        stage_started = _mark_reader_stage(
            timings, "load_model_features_ms", stage_started
        )
        edge_features = await self._load_edge_features_cached(
            conn,
            tenant_id,
            [r["id"] for r in edges],
        )
        stage_started = _mark_reader_stage(
            timings, "load_edge_features_ms", stage_started
        )

        return (
            _ReaderGraphRows(
                candidate_count_before_edge_seed=candidate_count_before_edge_seed,
                edge_seed_limit=edge_seed_limit,
                seed_ids=seed_ids,
                edges=edges,
                models=models,
                features=features,
                edge_features=edge_features,
            ),
            stage_started,
        )

    def _score_reader_graph(
        self,
        *,
        graph: _ReaderGraphRows,
        candidates: "_CandidateAccumulator",
        primitive: str,
        intents: tuple[RetrievalIntent, ...],
        question: str,
        trigger: TriggerContext,
        cues: StructuredCues,
        timings: dict[str, int],
        stage_started: float,
    ) -> tuple[_ReaderScoredGraph, float]:
        gate_edges: list[CandidateEdge] = []
        gate_debug: dict[str, dict[str, Any]] = {}
        for edge in graph.edges:
            source_id = edge["source_model_id"]
            target_id = edge["target_model_id"]
            gate = self._gate_scorer.score(
                gate_inputs=GateInputs(
                    edge_type=str(edge["edge_kind"]),
                    edge_confidence=float(edge["weight"] or 0.65),
                    edge_updated_at=edge["created_at"] or datetime.now(timezone.utc),
                    source_features=graph.features.get(source_id),
                    target_features=graph.features.get(target_id),
                    edge_features=graph.edge_features.get(edge["id"]),
                    source_trust_tier=None,
                    access_allowed=True,
                ),
                question_primitive=primitive,
                intent_kind=(intents[0].intent if intents else None),
            )
            gate_edges.append(
                CandidateEdge(
                    edge_id=edge["id"],
                    source_model_id=source_id,
                    target_model_id=target_id,
                    edge_type=str(edge["edge_kind"]),
                    gate_score=gate.score,
                )
            )
            gate_debug[str(edge["id"])] = {
                "score": gate.score,
                "reason": gate.reason,
                "components": gate.components,
            }
            candidates.propagate(
                source_id,
                target_id,
                gate_score=gate.score,
                edge_kind=str(edge["edge_kind"]),
            )
            candidates.propagate(
                target_id,
                source_id,
                gate_score=gate.score * 0.85,
                edge_kind=str(edge["edge_kind"]),
            )
        stage_started = _mark_reader_stage(
            timings, "gate_propagation_ms", stage_started
        )

        activated_nodes: list[ActivatedNode] = []
        activation_details: dict[UUID, _CandidateScore] = {}
        for mid, model in sorted(
            graph.models.items(),
            key=lambda item: _model_stable_sort_key(item[1]),
        ):
            score = candidates.score_for(mid)
            detail = candidates.details[mid]
            lexical_score, lexical_reasons = _lexical_activation(
                model,
                question,
                trigger,
                cues,
            )
            if lexical_score > 0:
                score += lexical_score
                detail.add(lexical_score, lexical_reasons)
            score = candidates.adjusted_score_for(mid, score)
            score = _clamp(score, 0.0, 1.0)
            if score <= 0:
                continue
            detail.score = score
            activation_details[mid] = detail
            activated_nodes.append(
                ActivatedNode(
                    model_id=mid,
                    activation_score=score,
                    activation_reasons=tuple(detail.reasons[:12]),
                    structural_features=graph.features.get(mid),
                )
            )
        stage_started = _mark_reader_stage(
            timings, "activation_scoring_ms", stage_started
        )
        return (
            _ReaderScoredGraph(
                gate_edges=gate_edges,
                gate_debug=gate_debug,
                activated_nodes=activated_nodes,
                activation_details=activation_details,
            ),
            stage_started,
        )

    async def _select_and_project_reader_graph(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        primitive: str,
        learned_plan: _LearnedReadPlan,
        scored: _ReaderScoredGraph,
        timings: dict[str, int],
        stage_started: float,
    ) -> _ReaderProjectedSelection:
        selector = self._selector
        if learned_plan.max_nodes is not None or learned_plan.max_edges is not None:
            selector = SubgraphSelector(
                budget=SelectionBudget(
                    max_nodes=learned_plan.max_nodes or self._budget.max_nodes,
                    max_edges=learned_plan.max_edges or self._budget.max_edges,
                    max_summarized_hubs=10,
                )
            )

        node_budget = learned_plan.max_nodes or self._budget.max_nodes
        selection = selector.select(
            activated_nodes=scored.activated_nodes,
            candidate_edges=scored.gate_edges,
            question_primitive=primitive,
            required_evidence_roles=_required_roles_for(primitive),
            known_counterevidence_node_ids=_protected_counterevidence_node_ids(
                scored.activated_nodes,
                max_nodes=node_budget,
            ),
        )
        stage_started = _mark_reader_stage(
            timings, "subgraph_selection_ms", stage_started
        )
        selected_model_ids = list(selection.selected_nodes)
        projection_budget = _projection_budget_for(
            self._budget,
            primitive=primitive,
            learned_plan=learned_plan,
        )
        projection = await EvidenceProjector(budget=projection_budget).project(
            pool=self._pool,
            tenant_id=tenant_id,
            selected_model_ids=selected_model_ids,
            question_primitive=primitive,
            conn=conn,
        )
        stage_started = _mark_reader_stage(
            timings, "evidence_projection_ms", stage_started
        )
        projected_dicts = tuple(
            _jsonable(asdict(candidate)) for candidate in projection.projected
        )
        observation_ids = [
            item.evidence_id
            for item in projection.projected
            if item.evidence_kind == "observation"
        ]
        observations = await self._load_observations_cached(
            conn, tenant_id, observation_ids
        )
        _mark_reader_stage(timings, "load_observations_ms", stage_started)
        return _ReaderProjectedSelection(
            selection=selection,
            selected_model_ids=selected_model_ids,
            projection_budget=projection_budget,
            projection=projection,
            projected_dicts=projected_dicts,
            observations=observations,
        )

    async def _load_aliases_cached(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
    ) -> dict[str, str]:
        if not self._budget.row_cache_enabled:
            return await _load_aliases_with_conn(conn, tenant_id)
        cached = self._alias_cache.get(tenant_id)
        if cached is not None:
            self._cache_stats["alias_hits"] += 1
            return dict(cached)
        aliases = await _load_aliases_with_conn(conn, tenant_id)
        self._alias_cache[tenant_id] = dict(aliases)
        self._cache_stats["alias_misses"] += 1
        return aliases

    async def _load_models_cached(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        model_ids: list[UUID],
    ) -> dict[UUID, ModelRow]:
        if not self._budget.row_cache_enabled:
            return await _load_models(conn, tenant_id, model_ids)
        ids = list(dict.fromkeys(model_ids))
        cache = self._model_cache[tenant_id]
        missing = [mid for mid in ids if mid not in cache]
        self._cache_stats["model_hits"] += len(ids) - len(missing)
        if missing:
            loaded = await _load_models(conn, tenant_id, missing)
            cache.update(loaded)
            self._cache_stats["model_misses"] += len(missing)
        return {mid: cache[mid] for mid in ids if mid in cache}

    async def _load_model_features_cached(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        model_ids: list[UUID],
    ) -> dict[UUID, ModelStructuralFeatures]:
        if not self._budget.row_cache_enabled:
            return await _load_model_features(conn, tenant_id, model_ids)
        ids = list(dict.fromkeys(model_ids))
        cache = self._model_feature_cache[tenant_id]
        missing = [mid for mid in ids if mid not in cache]
        self._cache_stats["model_feature_hits"] += len(ids) - len(missing)
        if missing:
            loaded = await _load_model_features(conn, tenant_id, missing)
            for mid in missing:
                cache[mid] = loaded.get(mid)
            self._cache_stats["model_feature_misses"] += len(missing)
        return {
            mid: feature
            for mid, feature in ((mid, cache.get(mid)) for mid in ids)
            if feature is not None
        }

    async def _load_edge_features_cached(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        edge_ids: list[UUID],
    ) -> dict[UUID, EdgeStructuralFeatures]:
        if not self._budget.row_cache_enabled:
            return await _load_edge_features(conn, tenant_id, edge_ids)
        ids = list(dict.fromkeys(edge_ids))
        cache = self._edge_feature_cache[tenant_id]
        missing = [eid for eid in ids if eid not in cache]
        self._cache_stats["edge_feature_hits"] += len(ids) - len(missing)
        if missing:
            loaded = await _load_edge_features(conn, tenant_id, missing)
            for eid in missing:
                cache[eid] = loaded.get(eid)
            self._cache_stats["edge_feature_misses"] += len(missing)
        return {
            eid: feature
            for eid, feature in ((eid, cache.get(eid)) for eid in ids)
            if feature is not None
        }

    async def _load_observations_cached(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        observation_ids: list[UUID],
    ) -> list[ObservationRow]:
        if not self._budget.row_cache_enabled:
            return await _load_observations(conn, tenant_id, observation_ids)
        ids = list(dict.fromkeys(observation_ids))
        cache = self._observation_cache[tenant_id]
        missing = [oid for oid in ids if oid not in cache]
        self._cache_stats["observation_hits"] += len(ids) - len(missing)
        if missing:
            loaded = await _load_observations(conn, tenant_id, missing)
            by_id = {obs.id: obs for obs in loaded}
            for oid in missing:
                cache[oid] = by_id.get(oid)
            self._cache_stats["observation_misses"] += len(missing)
        return [obs for oid in ids if (obs := cache.get(oid)) is not None]

    async def _activate_policy_memory(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        trigger: TriggerContext,
        primitive: str,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
        substrate: SageReadSubstrate | None,
        timings: dict[str, int],
        degraded_sources: list[dict[str, Any]],
        stage_started: float,
    ) -> _ReaderPolicyMemory:
        shortcut_hits = await self._activate_from_shortcuts(
            conn, tenant_id, signature, candidates, degraded_sources,
        )
        stage_started = _mark_reader_stage(
            timings, "shortcut_activation_ms", stage_started,
        )
        contextual_affordance_hits = await self._activate_from_affordances(
            conn, tenant_id, primitive, signature, candidates, degraded_sources,
        )
        stage_started = _mark_reader_stage(
            timings, "affordance_activation_ms", stage_started,
        )
        negative_memory = await self._suppress_from_negative_memory(
            conn, tenant_id, signature, candidates, degraded_sources,
        )
        stage_started = _mark_reader_stage(timings, "negative_memory_ms", stage_started)
        learned_plan = _learned_read_plan(
            budget=self._budget,
            signature=signature,
            candidates=candidates,
            shortcut_hits=shortcut_hits,
            contextual_affordance_hits=contextual_affordance_hits,
            negative_memory_count=negative_memory.count,
            suppressed_count=negative_memory.suppressed_count,
            explicit_model_count=_explicit_model_count(trigger),
            substrate_model_count=substrate.model_count if substrate else 0,
        )
        return _ReaderPolicyMemory(
            shortcut_hits=shortcut_hits,
            contextual_affordance_hits=contextual_affordance_hits,
            negative_memory=negative_memory,
            learned_plan=learned_plan,
            stage_started=stage_started,
        )

    async def _activate_from_shortcuts(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
        degraded_sources: list[dict[str, Any]],
    ) -> int:
        if not signature:
            return 0
        try:
            shortcuts = await DiscoveryShortcutsRepo(
                self._pool,
                tenant_id=tenant_id,
            ).find_for_signature(
                signature,
                limit=self._budget.shortcut_candidates,
                conn=conn,
            )
        except Exception as exc:  # noqa: BLE001
            _record_degraded_source(
                degraded_sources,
                tenant_id=tenant_id,
                source="shortcuts",
                exc=exc,
            )
            return 0
        for shortcut in shortcuts:
            score = min(0.42, 0.22 + 0.08 * float(shortcut.utility_score or 0.0))
            if shortcut.to_model_id is not None:
                candidates.add(
                    shortcut.to_model_id,
                    score,
                    f"shortcut:{shortcut.id}",
                    source="shortcut",
                )
                role = _shortcut_role_for_signature(shortcut.from_signature)
                if role:
                    candidates.add(
                        shortcut.to_model_id,
                        0.0,
                        f"role:{role}",
                        source="shortcut",
                    )
        return len(shortcuts)

    async def _activate_from_affordances(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        primitive: str,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
        degraded_sources: list[dict[str, Any]],
    ) -> int:
        entities = _signature_entities(signature)
        try:
            profiles = await AffordanceProfilesRepo(
                self._pool,
                tenant_id=tenant_id,
            ).search_by_primitive_context(
                primitive,
                entities=entities,
                limit=self._budget.affordance_candidates,
                min_utility=0.0,
                conn=conn,
            )
        except Exception as exc:  # noqa: BLE001
            _record_degraded_source(
                degraded_sources,
                tenant_id=tenant_id,
                source="affordances",
                exc=exc,
            )
            return 0
        contextual_hits = 0
        for profile in profiles:
            overlap = _activation_signature_overlap(
                profile.activation_signatures,
                entities,
            )
            utility = float(profile.utility_score or 0.0)
            if overlap > 0:
                contextual_hits += 1
                context_boost = min(0.14, 0.07 * overlap)
                score = min(0.44, 0.16 + 0.045 * utility + context_boost)
            else:
                # Utility without contextual overlap is useful recall signal,
                # but it should not outrank question/entity evidence by itself.
                score = min(0.22, 0.09 + 0.018 * utility)
            candidates.add(
                profile.model_id,
                score,
                (
                    f"affordance:{primitive}:context"
                    if overlap > 0
                    else f"affordance:{primitive}"
                ),
                source="affordance",
            )
        return contextual_hits

    async def _activate_from_lexical_scan(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        question: str,
        trigger: TriggerContext,
        cues: StructuredCues,
        candidates: "_CandidateAccumulator",
        limit: int | None = None,
    ) -> dict[str, int]:
        stats = {
            "text_search_limit": 0,
            "text_search_hits": 0,
        }
        # Question-only Ask paths need a lexical safety rail even when
        # learned shortcuts/affordances have already produced generic
        # candidates. Otherwise a few broad learned hits can starve the
        # exact question terms before the selector ever sees them.
        requested_limit = (
            int(limit) if limit is not None else int(self._budget.lexical_candidates)
        )
        if requested_limit <= 0:
            return stats
        terms = _candidate_terms(question, trigger, cues)
        if not terms:
            return stats
        stats["text_search_limit"] = requested_limit
        rows = await _fetch_search_document_matches(
            conn,
            tenant_id=tenant_id,
            terms=terms,
            limit=requested_limit,
            microquery_enabled=bool(self._budget.lexical_microquery_enabled),
            microquery_terms=int(self._budget.lexical_microquery_terms),
            microquery_per_term_limit=int(
                self._budget.lexical_microquery_per_term_limit
            ),
        )
        stats["text_search_hits"] = len(rows)
        for idx, row in enumerate(rows):
            match_count = int(row["match_count"] or 1)
            candidates.add(
                row["id"],
                _lexical_seed_score(
                    natural=str(row["natural"] or ""),
                    question=question,
                    match_count=match_count,
                    rank=idx,
                ),
                f"lexical:{match_count}",
                source="lexical",
            )
        return stats

    async def _activate_from_alternative_scan(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        question: str,
        candidates: "_CandidateAccumulator",
    ) -> None:
        alternatives = extract_query_alternatives(question)
        if not alternatives:
            return
        terms: list[tuple[str, str]] = []
        for alternative in alternatives:
            for term in alternative_terms(alternative):
                if len(terms) >= 32:
                    break
                terms.append((alternative, term))
            if len(terms) >= 32:
                break
        if not terms:
            return

        rows = await _fetch_search_document_matches(
            conn,
            tenant_id=tenant_id,
            terms=[term for _, term in terms],
            limit=max(12, min(self._budget.lexical_candidates, len(terms) * 4)),
            microquery_enabled=bool(self._budget.lexical_microquery_enabled),
            microquery_terms=int(self._budget.lexical_microquery_terms),
            microquery_per_term_limit=int(
                self._budget.lexical_microquery_per_term_limit
            ),
        )

        for rank, row in enumerate(rows):
            natural = str(row["natural"] or "")
            matched = _matched_alternatives(natural, alternatives)
            if not matched:
                continue
            match_count = int(row["match_count"] or len(matched))
            score = _alternative_seed_score(
                natural=natural,
                question=question,
                alternative_count=len(matched),
                match_count=match_count,
                rank=rank,
            )
            reason = "alternative:" + ",".join(matched[:3])
            candidates.add(row["id"], score, reason, source="alternative")

    async def _activate_from_operational_facets(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        question: str,
        candidates: "_CandidateAccumulator",
    ) -> None:
        plan = infer_operational_query_plan(question)
        if not plan.roles:
            return
        seed_roles = [
            role
            for role in plan.roles
            if role in {"action", "count", "delta", "invariant", "sequence"}
        ]
        if not seed_roles:
            return
        rows = await _fetch_operational_role_matches(
            conn,
            tenant_id=tenant_id,
            seed_roles=seed_roles,
            terms=[term.casefold() for term in plan.terms],
            limit=max(8, min(self._budget.lexical_candidates, 24)),
            per_role_limit=max(
                32,
                min(
                    192,
                    int(self._budget.lexical_microquery_per_term_limit)
                    * max(2, len(seed_roles)),
                ),
            ),
        )
        for rank, row in enumerate(rows):
            role_count = int(row["role_match_count"] or 1)
            lexical_count = int(row["lexical_match_count"] or 0)
            matched_roles = [
                str(role) for role in (row["matched_roles"] or []) if role is not None
            ]
            score = (
                0.16
                + min(0.18, 0.055 * role_count)
                + min(0.14, 0.025 * lexical_count)
                - rank * 0.002
            )
            candidates.add(
                row["id"],
                _clamp(score, 0.10, 0.42),
                "operational:" + ",".join(matched_roles[:4]),
                source="operational_facet",
            )

    async def _activate_from_belief_addresses(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        primitive: str,
        question: str,
        trigger: TriggerContext,
        cues: StructuredCues,
        candidates: "_CandidateAccumulator",
    ) -> None:
        primitives = _belief_address_primitives_for(primitive)
        if not primitives:
            return
        rows = await _fetch_belief_address_matches(
            conn,
            tenant_id=tenant_id,
            primitives=primitives,
            terms=_candidate_terms(question, trigger, cues),
            limit=max(8, min(self._budget.lexical_candidates, 28)),
        )
        for rank, row in enumerate(rows):
            primitive_count = int(row["primitive_match_count"] or 1)
            lexical_count = int(row["lexical_match_count"] or 0)
            if lexical_count <= 0 and row["lexical_terms_present"]:
                continue
            matched = [
                str(item)
                for item in (row["matched_primitives"] or [])
                if item is not None
            ]
            score = (
                0.15
                + min(0.14, 0.06 * primitive_count)
                + min(0.14, 0.028 * lexical_count)
                - rank * 0.002
            )
            reason = "belief_address:" + ",".join(matched[:4])
            if lexical_count:
                reason = f"{reason}:lexical={lexical_count}"
            candidates.add(
                row["id"],
                _clamp(score, 0.10, 0.40),
                reason,
                source="belief_address",
            )

    async def _suppress_from_negative_memory(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
        degraded_sources: list[dict[str, Any]],
    ) -> _NegativeMemorySignal:
        if not signature:
            return _NegativeMemorySignal()
        try:
            repo = NegativeMemoryRepo(self._pool, tenant_id=tenant_id)
            by_id = {}
            for probe in _negative_memory_signature_probes(signature):
                for memory in await repo.find_for_signature(probe, conn=conn):
                    by_id[memory.id] = memory
            memories = tuple(by_id.values())
        except Exception as exc:  # noqa: BLE001
            _record_degraded_source(
                degraded_sources,
                tenant_id=tenant_id,
                source="negative_memory",
                exc=exc,
            )
            return _NegativeMemorySignal()
        candidate_ids = candidates.model_ids
        suppressed = 0
        for memory in memories:
            for model_id in _negative_memory_model_ids(memory.rejected_path):
                if model_id in candidate_ids:
                    candidates.suppress(
                        model_id,
                        f"negative_memory:{memory.memory_type}",
                    )
                    suppressed += 1
        return _NegativeMemorySignal(count=len(memories), suppressed_count=suppressed)


def _record_degraded_source(
    degraded_sources: list[dict[str, Any]],
    *,
    tenant_id: UUID,
    source: str,
    exc: BaseException,
) -> None:
    event = {
        "source": source,
        "error_type": type(exc).__name__,
        "error": str(exc),
    }
    degraded_sources.append(event)
    _log.warning(
        "sage.reader.degraded_source",
        tenant_id=str(tenant_id),
        source=source,
        error_type=type(exc).__name__,
        error=str(exc),
    )


@dataclass(slots=True)
class _CandidateScore:
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    sources: dict[str, float] = field(default_factory=dict)

    def add(self, score: float, reasons: list[str] | tuple[str, ...]) -> None:
        self.score += float(score)
        for reason in reasons:
            if reason and reason not in self.reasons:
                self.reasons.append(str(reason))


class _CandidateAccumulator:
    def __init__(self) -> None:
        self.details: defaultdict[UUID, _CandidateScore] = defaultdict(_CandidateScore)
        self.suppressed: dict[UUID, str] = {}

    @property
    def model_ids(self) -> set[UUID]:
        return set(self.details)

    @property
    def contextual_hit_count(self) -> int:
        return sum(
            1
            for detail in self.details.values()
            if any(reason.endswith(":context") for reason in detail.reasons)
        )

    def add(
        self,
        model_id: UUID,
        score: float,
        reason: str,
        *,
        source: str,
    ) -> None:
        detail = self.details[model_id]
        detail.score += float(score)
        detail.sources[source] = detail.sources.get(source, 0.0) + float(score)
        if reason not in detail.reasons:
            detail.reasons.append(reason)

    def score_for(self, model_id: UUID) -> float:
        return self.details[model_id].score

    def source_total(self, source: str) -> float:
        return sum(
            float(detail.sources.get(source, 0.0)) for detail in self.details.values()
        )

    def top_source_score(self, sources: tuple[str, ...]) -> float:
        top = 0.0
        for detail in self.details.values():
            score = sum(float(detail.sources.get(source, 0.0)) for source in sources)
            if score > top:
                top = score
        return top

    def adjusted_score_for(self, model_id: UUID, score: float) -> float:
        if model_id not in self.suppressed:
            return score
        return min(float(score) * 0.18, 0.12)

    def ranked_model_ids(
        self,
        *,
        limit: int,
        required_ids: set[UUID] | tuple[UUID, ...] | list[UUID] = (),
    ) -> list[UUID]:
        required = [
            mid for mid in sorted(set(required_ids), key=str) if mid in self.details
        ]
        effective_limit = max(len(required), max(0, int(limit)))
        ranked = sorted(
            self.details,
            key=lambda mid: (
                -self.adjusted_score_for(mid, self.details[mid].score),
                str(mid),
            ),
        )
        out: list[UUID] = []
        seen: set[UUID] = set()
        for mid in required:
            out.append(mid)
            seen.add(mid)
        for mid in ranked:
            if mid in seen:
                continue
            out.append(mid)
            seen.add(mid)
            if len(out) >= effective_limit:
                break
        return out

    def suppress(self, model_id: UUID, reason: str) -> None:
        self.suppressed[model_id] = reason
        detail = self.details[model_id]
        detail.sources["negative_memory"] = -abs(detail.score)
        if reason not in detail.reasons:
            detail.reasons.append(reason)

    def propagate(
        self,
        source_id: UUID,
        target_id: UUID,
        *,
        gate_score: float,
        edge_kind: str,
    ) -> None:
        source = self.details.get(source_id)
        if source is None or source.score <= 0 or gate_score < 0.20:
            return
        source_score = self.adjusted_score_for(source_id, source.score)
        propagated = min(0.28, source_score * gate_score * 0.45)
        if propagated <= 0:
            return
        self.add(
            target_id,
            propagated,
            f"propagated:{edge_kind}",
            source="propagation",
        )


def _protected_counterevidence_node_ids(
    activated_nodes: list[ActivatedNode],
    *,
    max_nodes: int,
) -> tuple[UUID, ...]:
    cap = max(4, min(24, int(max(1, max_nodes) * 0.4)))
    counter_nodes = [
        node
        for node in activated_nodes
        if any("counterevidence" in reason for reason in node.activation_reasons)
        and not any(
            reason.startswith("negative_memory:")
            for reason in node.activation_reasons
        )
    ]
    counter_nodes.sort(
        key=lambda node: (-float(node.activation_score or 0.0), str(node.model_id))
    )
    return tuple(node.model_id for node in counter_nodes[:cap])


def _build_reader_result(
    *,
    budget: ReaderBudget,
    question_id: str,
    primitive: str,
    signature: dict[str, Any],
    cues: StructuredCues,
    intents: tuple[RetrievalIntent, ...],
    graph: _ReaderGraphRows,
    scored: _ReaderScoredGraph,
    projected: _ReaderProjectedSelection,
    substrate: SageReadSubstrate | None,
    lexical_activation_stats: dict[str, int],
    degraded_sources: list[dict[str, Any]],
    cache_stats_delta: dict[str, int],
    timings: dict[str, int],
    read_started: float,
    learned_plan: _LearnedReadPlan,
    evidence_state: dict[str, Any] | None,
) -> SynthesisReaderResult:
    selection_rank = {mid: idx for idx, mid in enumerate(projected.selected_model_ids)}
    traces = _build_activation_traces(
        question_id=question_id,
        activated_nodes=scored.activated_nodes,
        activation_details=scored.activation_details,
        selection_rank=selection_rank,
        max_nodes=budget.max_nodes,
    )
    selected_models = tuple(
        graph.models[mid] for mid in projected.selected_model_ids if mid in graph.models
    )
    model_scores = {
        mid: scored.activation_details.get(mid, _CandidateScore()).score
        for mid in projected.selected_model_ids
    }
    pathway = PathwayResult(
        models=list(selected_models),
        observations=list(projected.observations),
        source_pathway="SAGE",  # type: ignore[arg-type]
        notes={
            "sage_reader": True,
            "question_id": question_id,
            "question_primitive": primitive,
            "signature": signature,
            "selected_model_ids": [str(mid) for mid in projected.selected_model_ids],
            "projected_evidence_count": len(projected.projected_dicts),
            "degraded_sources": list(degraded_sources),
        },
    )
    debug = _build_reader_debug_payload(
        cues=cues,
        intents=intents,
        traces=traces,
        scored=scored,
        projected=projected,
        substrate=substrate,
        graph=graph,
        lexical_activation_stats=lexical_activation_stats,
        degraded_sources=degraded_sources,
        cache_stats_delta=cache_stats_delta,
        timings=timings,
        read_started=read_started,
        learned_plan=learned_plan,
        evidence_state=evidence_state,
    )
    return SynthesisReaderResult(
        question_id=question_id,
        question_primitive=primitive,
        signature=signature,
        cues=cues,
        intents=intents,
        activations=tuple(traces),
        selection=projected.selection,
        projected_evidence=projected.projected_dicts,
        omitted_projection=tuple(
            (str(mid), reason) for mid, reason in projected.projection.omitted
        ),
        models=selected_models,
        observations=tuple(projected.observations),
        model_scores=model_scores,
        pathway_result=pathway,
        debug=debug,
    )


def _build_activation_traces(
    *,
    question_id: str,
    activated_nodes: list[ActivatedNode],
    activation_details: dict[UUID, _CandidateScore],
    selection_rank: dict[UUID, int],
    max_nodes: int,
) -> list[ReaderActivationTrace]:
    traces: list[ReaderActivationTrace] = []
    for node in sorted(
        activated_nodes,
        key=lambda n: (
            -n.activation_score,
            selection_rank.get(n.model_id, len(selection_rank)),
            str(n.model_id),
        ),
    )[: max_nodes * 3]:
        detail = activation_details[node.model_id]
        traces.append(
            ReaderActivationTrace(
                question_id=question_id,
                model_id=node.model_id,
                activation_score=node.activation_score,
                activation_reasons=node.activation_reasons,
                selected=node.model_id in selection_rank,
                selection_rank=selection_rank.get(node.model_id),
                source_breakdown=dict(detail.sources),
            )
        )
    return traces


def _build_reader_debug_payload(
    *,
    cues: StructuredCues,
    intents: tuple[RetrievalIntent, ...],
    traces: list[ReaderActivationTrace],
    scored: _ReaderScoredGraph,
    projected: _ReaderProjectedSelection,
    substrate: SageReadSubstrate | None,
    graph: _ReaderGraphRows,
    lexical_activation_stats: dict[str, int],
    degraded_sources: list[dict[str, Any]],
    cache_stats_delta: dict[str, int],
    timings: dict[str, int],
    read_started: float,
    learned_plan: _LearnedReadPlan,
    evidence_state: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "cue_extraction": _jsonable(asdict(cues)),
        "intents": [_jsonable(asdict(intent)) for intent in intents],
        "evidence_state": _jsonable(evidence_state or {}),
        "activation_reasons": {
            str(trace.model_id): list(trace.activation_reasons) for trace in traces
        },
        "gate_scores": scored.gate_debug,
        "selector": {
            "selected_nodes": [str(mid) for mid in projected.selection.selected_nodes],
            "selected_edges": [str(eid) for eid in projected.selection.selected_edges],
            "bridge_nodes": [str(mid) for mid in projected.selection.bridge_nodes],
            "coverage_metrics": projected.selection.coverage_metrics,
        },
        "projection_coverage": projected.projection.coverage,
        "degraded_sources": list(degraded_sources),
        "projection_budget": _jsonable(asdict(projected.projection_budget)),
        "learned_read_plan": _jsonable(asdict(learned_plan)),
        "candidate_pool": {
            "substrate_model_count": substrate.model_count if substrate else 0,
            "substrate_counters": (
                dict(sorted(substrate.counters.items())) if substrate else {}
            ),
            "substrate_timings_ms": dict(substrate.timings_ms) if substrate else {},
            "before_edge_seed_count": graph.candidate_count_before_edge_seed,
            "edge_seed_limit": graph.edge_seed_limit,
            "edge_seed_count": len(graph.seed_ids),
            "edge_seed_pruned_count": max(
                0,
                graph.candidate_count_before_edge_seed - len(graph.seed_ids),
            ),
            "candidate_edge_count": len(graph.edges),
            "loaded_model_count": len(graph.models),
            "lexical_activation": lexical_activation_stats,
        },
        "row_cache": cache_stats_delta,
        "stage_timings_ms": {
            **timings,
            "reader_total_ms": int((time.perf_counter() - read_started) * 1000),
        },
    }


def _edge_seed_limit(budget: ReaderBudget, learned_plan: _LearnedReadPlan) -> int:
    configured = max(1, int(budget.activation_seed_limit))
    propagation_cap = (
        learned_plan.propagation_neighbors
        if learned_plan.propagation_neighbors is not None
        else budget.propagation_neighbors
    )
    return max(1, min(configured, int(propagation_cap)))


def _explicit_seed_ids(trigger: TriggerContext) -> set[UUID]:
    out: set[UUID] = set()
    if trigger.model_id is not None:
        out.add(trigger.model_id)
    out.update(trigger.member_model_ids or [])
    return out


def _learned_read_plan(
    *,
    budget: ReaderBudget,
    signature: dict[str, Any],
    candidates: _CandidateAccumulator,
    shortcut_hits: int,
    contextual_affordance_hits: int,
    negative_memory_count: int,
    suppressed_count: int,
    explicit_model_count: int,
    substrate_model_count: int = 0,
) -> _LearnedReadPlan:
    if not budget.learned_planning_enabled:
        return _LearnedReadPlan(reasons=("learned_planning_disabled",))

    learned_score = candidates.source_total("shortcut") + candidates.source_total(
        "affordance"
    )
    top_learned_score = candidates.top_source_score(("shortcut", "affordance"))
    positive_hit_count = shortcut_hits + contextual_affordance_hits
    negative_threshold = max(1, budget.abstain_negative_memory_threshold)
    reasons: list[str] = []

    negative_ready = (
        negative_memory_count >= negative_threshold and explicit_model_count == 0
    )
    negative_only = (
        negative_ready and positive_hit_count == 0 and top_learned_score < 0.24
    )
    negative_dominant = negative_ready and (
        suppressed_count >= max(negative_threshold, positive_hit_count)
        or (
            negative_memory_count >= max(8, positive_hit_count * 4)
            and positive_hit_count <= 1
            and top_learned_score < 0.50
        )
        or (
            negative_memory_count >= max(16, positive_hit_count * 6)
            and learned_score < 0.80
            and top_learned_score < 0.36
        )
    )
    if negative_only or negative_dominant:
        reasons.append(f"negative_memory_count={negative_memory_count}")
        if suppressed_count:
            reasons.append(f"suppressed_by_negative_memory={suppressed_count}")
        if positive_hit_count:
            reasons.append(f"positive_hit_count={positive_hit_count}")
            reasons.append(f"top_learned_score={top_learned_score:.3f}")
        return _LearnedReadPlan(
            mode="abstain",
            abstain_early=True,
            confidence=_clamp(
                0.54 + 0.035 * negative_memory_count + 0.03 * suppressed_count,
                0.0,
                0.94,
            ),
            reasons=tuple(reasons),
        )

    focused = (
        explicit_model_count == 0
        and positive_hit_count >= 2
        and top_learned_score >= 0.42
        and learned_score >= 0.70
    )
    if focused:
        reasons.extend(
            [
                f"positive_hit_count={positive_hit_count}",
                f"top_learned_score={top_learned_score:.3f}",
                f"learned_score={learned_score:.3f}",
            ]
        )
        if suppressed_count:
            reasons.append(f"suppressed_by_negative_memory={suppressed_count}")
        return _LearnedReadPlan(
            mode="focused",
            skip_broad_discovery=True,
            gate_broad_actions=True,
            lexical_candidates=max(0, int(budget.focused_lexical_candidates)),
            max_nodes=max(4, min(budget.max_nodes, budget.focused_max_nodes)),
            max_edges=max(8, min(budget.max_edges, budget.focused_max_edges)),
            propagation_neighbors=max(
                8,
                min(
                    budget.propagation_neighbors,
                    budget.focused_propagation_neighbors,
                ),
            ),
            confidence=_clamp(0.48 + top_learned_score * 0.55, 0.0, 0.92),
            reasons=tuple(reasons),
        )

    rerank_ready = substrate_model_count >= max(
        1, int(budget.rerank_min_substrate_models)
    ) and (
        explicit_model_count > 0 or positive_hit_count >= 1 or top_learned_score >= 0.24
    )
    if rerank_ready:
        reasons.extend(
            [
                f"substrate_model_count={substrate_model_count}",
                f"positive_hit_count={positive_hit_count}",
                f"top_learned_score={top_learned_score:.3f}",
            ]
        )
        if suppressed_count:
            reasons.append(f"suppressed_by_negative_memory={suppressed_count}")
        return _LearnedReadPlan(
            mode="rerank",
            skip_broad_discovery=True,
            gate_broad_actions=False,
            lexical_candidates=max(0, int(budget.rerank_lexical_candidates)),
            max_nodes=max(8, min(budget.max_nodes, max(16, budget.focused_max_nodes))),
            max_edges=max(16, min(budget.max_edges, max(32, budget.focused_max_edges))),
            propagation_neighbors=max(
                12,
                min(
                    budget.propagation_neighbors,
                    max(24, budget.focused_propagation_neighbors),
                ),
            ),
            confidence=_clamp(0.42 + top_learned_score * 0.35, 0.0, 0.82),
            reasons=tuple(reasons),
        )

    guarded_negative_route = (
        negative_ready and top_learned_score < 0.42 and learned_score < 0.90
    )
    if guarded_negative_route:
        reasons.append(f"negative_memory_count={negative_memory_count}")
        if positive_hit_count:
            reasons.append(f"positive_hit_count={positive_hit_count}")
        if suppressed_count:
            reasons.append(f"suppressed_by_negative_memory={suppressed_count}")
        return _LearnedReadPlan(
            mode="guarded_negative_memory",
            skip_broad_discovery=True,
            gate_broad_actions=True,
            lexical_candidates=0,
            max_nodes=max(4, min(budget.max_nodes, 8)),
            max_edges=max(8, min(budget.max_edges, 16)),
            propagation_neighbors=max(8, min(budget.propagation_neighbors, 12)),
            confidence=_clamp(0.38 + 0.02 * negative_memory_count, 0.0, 0.76),
            reasons=tuple(reasons),
        )

    if suppressed_count:
        reasons.append(f"suppressed_by_negative_memory={suppressed_count}")
    return _LearnedReadPlan(
        mode="default",
        confidence=_clamp(top_learned_score, 0.0, 0.55),
        reasons=tuple(reasons),
    )


_PRIMITIVE_PROJECTED_ITEM_CAPS: dict[str, int] = {
    "OWNERSHIP": 48,
    "COUNTEREVIDENCE": 48,
    "FALSIFICATION": 48,
    "CONSTRAINT": 56,
    "DEPENDENCY": 64,
    "RECURRENCE": 40,
}


def _projection_budget_for(
    budget: ReaderBudget,
    *,
    primitive: str,
    learned_plan: _LearnedReadPlan,
) -> ProjectionBudget:
    """Translate reader intent into a projection-level sufficiency cap.

    The reader's node budget answers "which models are relevant?", but
    projection needs a separate answer to "how much evidence is enough?".
    Without that second cap, cheap summary/ref-only evidence can preserve
    a very broad tail after token demotion.
    """
    primitive_key = (primitive or "").strip().upper()
    primitive_cap = _PRIMITIVE_PROJECTED_ITEM_CAPS.get(primitive_key, 56)
    node_cap = learned_plan.max_nodes or budget.max_nodes
    cap = min(
        max(1, int(budget.max_evidence_items)),
        max(12, int(node_cap)),
        primitive_cap,
    )
    if learned_plan.mode in {"focused", "guarded_negative_memory", "rerank"}:
        cap = min(cap, max(12, int(round(node_cap * 1.5))))
    if learned_plan.abstain_early:
        cap = 0
    return ProjectionBudget(
        max_projected_items=cap,
        max_total_tokens=24_000,
    )


def _empty_reader_result(
    *,
    question_id: str,
    primitive: str,
    signature: dict[str, Any],
    cues: StructuredCues,
    intents: tuple[RetrievalIntent, ...],
    timings: dict[str, int],
    read_started: float,
    learned_plan: _LearnedReadPlan,
    degraded_sources: list[dict[str, Any]] | None = None,
    evidence_state: dict[str, Any] | None = None,
) -> SynthesisReaderResult:
    degraded = list(degraded_sources or [])
    selection = SubgraphSelection(
        selected_nodes=(),
        selected_edges=(),
        bridge_nodes=(),
        summarized_hubs=(),
        excluded=(),
        coverage_metrics={
            "bridge_coverage": 0.0,
            "counterevidence_coverage": 1.0,
            "role_coverage": 1.0,
        },
    )
    pathway = PathwayResult(
        models=[],
        observations=[],
        source_pathway="SAGE",  # type: ignore[arg-type]
        notes={
            "sage_reader": True,
            "question_id": question_id,
            "question_primitive": primitive,
            "signature": signature,
            "selected_model_ids": [],
            "projected_evidence_count": 0,
            "early_abstain": True,
            "degraded_sources": degraded,
        },
    )
    debug = {
        "cue_extraction": _jsonable(asdict(cues)),
        "intents": [_jsonable(asdict(intent)) for intent in intents],
        "evidence_state": _jsonable(evidence_state or {}),
        "activation_reasons": {},
        "gate_scores": {},
        "selector": {
            "selected_nodes": [],
            "selected_edges": [],
            "bridge_nodes": [],
            "coverage_metrics": selection.coverage_metrics,
        },
        "projection_coverage": {},
        "degraded_sources": degraded,
        "learned_read_plan": _jsonable(asdict(learned_plan)),
        "stage_timings_ms": {
            **timings,
            "lexical_activation_ms": 0,
            "belief_address_activation_ms": 0,
            "operational_facet_activation_ms": 0,
            "alternative_activation_ms": 0,
            "load_candidate_edges_ms": 0,
            "load_models_ms": 0,
            "load_model_features_ms": 0,
            "load_edge_features_ms": 0,
            "gate_propagation_ms": 0,
            "activation_scoring_ms": 0,
            "subgraph_selection_ms": 0,
            "evidence_projection_ms": 0,
            "load_observations_ms": 0,
            "reader_total_ms": int((time.perf_counter() - read_started) * 1000),
        },
    }
    return SynthesisReaderResult(
        question_id=question_id,
        question_primitive=primitive,
        signature=signature,
        cues=cues,
        intents=intents,
        activations=(),
        selection=selection,
        projected_evidence=(),
        omitted_projection=(),
        models=(),
        observations=(),
        model_scores={},
        pathway_result=pathway,
        debug=debug,
    )


def _explicit_model_count(trigger: TriggerContext) -> int:
    count = 1 if trigger.model_id is not None else 0
    return count + len(trigger.member_model_ids or ())


async def _load_aliases_with_conn(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, str]:
    table = await conn.fetchval("SELECT to_regclass('public.entity_aliases')")
    if table is None:
        return {}
    rows = await conn.fetch(
        """
        SELECT alias_text, resolved_entity_ref
        FROM entity_aliases
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    out: dict[str, str] = {}
    for row in rows:
        alias = str(row["alias_text"] or "").strip()
        if not alias:
            continue
        ref = _coerce_obj(row["resolved_entity_ref"])
        handle = ref.get("name") or ref.get("title") or ref.get("id") or alias
        out[alias.casefold()] = str(handle)
    return out


async def _load_models(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    model_ids: list[UUID],
) -> dict[UUID, ModelRow]:
    if not model_ids:
        return {}
    rows = await conn.fetch(
        f"""
        SELECT {_MODEL_SELECT_COLS_SQL}
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        model_ids,
    )
    return {row["id"]: _hydrate_model_row(row) for row in rows}


async def _load_observations(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_ids: list[UUID],
) -> list[ObservationRow]:
    ids = list(dict.fromkeys(observation_ids))
    if not ids:
        return []
    rows = await conn.fetch(
        """
        SELECT id, tenant_id, occurred_at, ingested_at, kind,
               source_channel, source_actor_ref, actor_id,
               content, content_text, embedding, embedding_pending,
               trust_tier, external_id, cause_id, sequence_num,
               entities_mentioned
        FROM observations
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        ids,
    )
    by_id = {row["id"]: _hydrate_observation_row(row) for row in rows}
    return [by_id[oid] for oid in ids if oid in by_id]


def _hydrate_observation_row(row: asyncpg.Record) -> ObservationRow:
    raw = dict(row)
    for key in ("content", "entities_mentioned"):
        value = raw.get(key)
        if isinstance(value, (bytes, bytearray)):
            value = value.decode()
        if isinstance(value, str):
            raw[key] = json.loads(value)
    emb = raw.get("embedding")
    if emb is not None and not isinstance(emb, list):
        if isinstance(emb, (bytes, bytearray)):
            emb = emb.decode()
        if isinstance(emb, str):
            try:
                raw["embedding"] = json.loads(emb)
            except (json.JSONDecodeError, ValueError):
                raw["embedding"] = None
        else:
            try:
                raw["embedding"] = [float(x) for x in emb]
            except (TypeError, ValueError):
                raw["embedding"] = None
    return ObservationRow.model_validate(raw)


async def _load_model_features(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    model_ids: list[UUID],
) -> dict[UUID, ModelStructuralFeatures]:
    if not model_ids:
        return {}
    table = await conn.fetchval(
        "SELECT to_regclass('public.model_structural_features')"
    )
    if table is None:
        return {}
    rows = await conn.fetch(
        """
        SELECT model_id, tenant_id, degree_total, degree_in, degree_out,
               clustering_coefficient, core_number, avg_neighbor_degree,
               bridge_score, hub_score, community_id, region_ids, updated_at
        FROM model_structural_features
        WHERE tenant_id = $1
          AND model_id = ANY($2::uuid[])
        """,
        tenant_id,
        model_ids,
    )
    return {
        row["model_id"]: ModelStructuralFeatures.model_validate(dict(row))
        for row in rows
    }


async def _load_edge_features(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    edge_ids: list[UUID],
) -> dict[UUID, EdgeStructuralFeatures]:
    if not edge_ids:
        return {}
    table = await conn.fetchval(
        "SELECT to_regclass('public.model_edge_structural_features')"
    )
    if table is None:
        return {}
    rows = await conn.fetch(
        """
        SELECT edge_id, tenant_id, source_model_id, target_model_id,
               degree_difference, common_neighbors, jaccard_overlap,
               edge_betweenness_approx, bridge_likelihood,
               redundancy_score, updated_at
        FROM model_edge_structural_features
        WHERE tenant_id = $1
          AND edge_id = ANY($2::uuid[])
        """,
        tenant_id,
        edge_ids,
    )
    return {
        row["edge_id"]: EdgeStructuralFeatures.model_validate(dict(row)) for row in rows
    }


async def _load_candidate_edges(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_model_ids: list[UUID],
    limit: int,
) -> list[dict[str, Any]]:
    if not seed_model_ids:
        return []
    model_edge_rows = await conn.fetch(
        """
        SELECT id, source_model_id, target_model_id, edge_kind,
               weight, created_at
        FROM model_edges
        WHERE tenant_id = $1
          AND status = 'active'
          AND (
            source_model_id = ANY($2::uuid[])
            OR target_model_id = ANY($2::uuid[])
          )
        ORDER BY created_at DESC
        LIMIT $3
        """,
        tenant_id,
        seed_model_ids,
        int(limit),
    )
    candidate_edge_rows = await conn.fetch(
        """
        SELECT id, source_model_id, target_model_id, edge_kind,
               COALESCE(confidence_score, judgment_leverage_score, 0.55)::float
                 AS weight,
               created_at
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND candidate_kind = 'edge'
          AND review_status IN ('candidate', 'needs_review', 'accepted')
          AND (expires_at IS NULL OR expires_at > now())
          AND source_model_id IS NOT NULL
          AND target_model_id IS NOT NULL
          AND (
            source_model_id = ANY($2::uuid[])
            OR target_model_id = ANY($2::uuid[])
          )
        ORDER BY judgment_leverage_score DESC, created_at DESC
        LIMIT $3
        """,
        tenant_id,
        seed_model_ids,
        int(limit),
    )
    edge_type_rows = await conn.fetch(
        """
        SELECT id,
               member_model_ids[1] AS source_model_id,
               member_model_ids[2] AS target_model_id,
               COALESCE(
                 metadata->'ontology_gap'->>'retrieval_fallback_kind',
                 proposed_proposition->>'parent_kind',
                 proposed_proposition->>'nearest_existing_kind',
                 'same_issue_as'
               ) AS edge_kind,
               COALESCE(confidence_score, judgment_leverage_score, 0.55)::float
                 AS weight,
               created_at
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND candidate_kind = 'edge_type'
          AND review_status IN ('candidate', 'needs_review', 'accepted')
          AND (expires_at IS NULL OR expires_at > now())
          AND cardinality(member_model_ids) >= 2
          AND member_model_ids && $2::uuid[]
        ORDER BY judgment_leverage_score DESC, created_at DESC
        LIMIT $3
        """,
        tenant_id,
        seed_model_ids,
        int(limit),
    )
    rows = [dict(row) for row in model_edge_rows]
    rows.extend(dict(row) for row in candidate_edge_rows)
    rows.extend(dict(row) for row in edge_type_rows)
    rows = [
        row
        for row in rows
        if row.get("source_model_id")
        and row.get("target_model_id")
        and row.get("source_model_id") != row.get("target_model_id")
    ]
    rows.sort(
        key=lambda row: (
            row.get("created_at") or datetime.min.replace(tzinfo=timezone.utc)
        ),
        reverse=True,
    )
    return rows[: int(limit)]


def _signal_payload(trigger: TriggerContext) -> dict[str, Any]:
    return {
        "summary": trigger.seed_natural_text or "",
        "signal_summary": trigger.seed_natural_text or "",
        "trigger_kind": trigger.kind,
        "seed_signature": trigger.seed_signature or {},
        "region_spec": trigger.region_spec or {},
    }


def _hypothesis_text(hypothesis: Any) -> str:
    return " ".join(
        str(part)
        for part in (
            getattr(hypothesis, "id", ""),
            getattr(hypothesis, "claim", hypothesis),
        )
        if part is not None
    ).strip()


def _coarse_primitive(
    question_primitive: str,
    intents: tuple[RetrievalIntent, ...],
) -> str:
    primitive = (question_primitive or "").strip().upper()
    if primitive:
        if primitive == "COMMITMENT":
            return "DEPENDENCY"
        return primitive
    if not intents:
        return "DEPENDENCY"
    mapping = {
        "test_dependency": "DEPENDENCY",
        "find_active_commitment": "DEPENDENCY",
        "find_counterevidence": "COUNTEREVIDENCE",
        "find_owner": "OWNERSHIP",
        "find_pattern_recurrence": "RECURRENCE",
        "find_blocking_resource": "CONSTRAINT",
        "test_falsification": "COUNTEREVIDENCE",
        "find_action_candidates": "ACTION",
        "find_goal_impact": "GOAL_IMPACT",
    }
    return mapping.get(intents[0].intent, "DEPENDENCY")


def _signature_for(
    trigger: TriggerContext,
    primitive: str,
    cues: StructuredCues,
) -> dict[str, Any]:
    entities: list[str] = []
    for raw in (
        list(cues.explicit_entities)
        + list(cues.aliases)
        + list(cues.customer_mentions)
        + list(cues.system_mentions)
        + [
            str(e.get("id") or e.get("name") or e.get("type"))
            for e in trigger.seed_entity_ids
            if isinstance(e, dict)
        ]
    ):
        text = str(raw).strip()
        if text and text not in entities:
            entities.append(text)
    out: dict[str, Any] = {
        "signal_type": trigger.kind,
        "question_primitive": primitive,
    }
    if entities:
        out["entities"] = entities[:12]
    return out


def _signature_entities(signature: dict[str, Any]) -> list[str]:
    raw = signature.get("entities")
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        text = str(item).strip()
        if text and text not in out:
            out.append(text)
    return out


def _negative_memory_signature_probes(
    signature: dict[str, Any],
) -> tuple[dict[str, Any], ...]:
    """Build progressively softer probes for learned dead-end routes.

    Exact signature matching is too brittle for language-derived cues: one
    run may extract "Nimbus", while a later run extracts "Nimbus Bank" plus
    extra nouns. We still avoid an unconditional primitive-only probe when
    entities exist because that would let one noisy query suppress a useful
    query in a different part of the company.
    """

    signal_type = signature.get("signal_type")
    primitive = signature.get("question_primitive")
    if not signal_type or not primitive:
        return (dict(signature),) if signature else ()

    probes: list[dict[str, Any]] = [dict(signature)]
    entities = _signature_entities(signature)
    for entity in entities[:8]:
        probes.append(
            {
                "signal_type": signal_type,
                "question_primitive": primitive,
                "entities": [entity],
            }
        )
    if not entities:
        probes.append(
            {
                "signal_type": signal_type,
                "question_primitive": primitive,
            }
        )

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for probe in probes:
        key = json.dumps(probe, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            deduped.append(probe)
    return tuple(deduped)


def _activation_signature_overlap(
    activation_signatures: dict[str, Any],
    entities: list[str],
) -> int:
    raw = activation_signatures.get("entities")
    if not isinstance(raw, list) or not entities:
        return 0
    profile_entities = {str(item).casefold() for item in raw if item is not None}
    return sum(1 for entity in entities if entity.casefold() in profile_entities)


def _add_explicit_trigger_models(
    candidates: _CandidateAccumulator,
    trigger: TriggerContext,
) -> None:
    if trigger.model_id is not None:
        candidates.add(
            trigger.model_id,
            0.72,
            "explicit:trigger_model",
            source="explicit",
        )
    for mid in trigger.member_model_ids or []:
        candidates.add(mid, 0.48, "explicit:member_model", source="explicit")


def _add_substrate_seed_models(
    candidates: _CandidateAccumulator,
    substrate: SageReadSubstrate | None,
    budget: ReaderBudget,
) -> None:
    if substrate is None or not substrate.baseline_model_ids:
        return
    limit = max(
        0, min(len(substrate.baseline_model_ids), int(budget.substrate_model_limit))
    )
    if limit <= 0:
        return
    for rank, model_id in enumerate(substrate.baseline_model_ids[:limit]):
        # A small prior keeps primary-retrieved scoped candidates available for
        # question-specific reranking without overpowering lexical/learned cues.
        score = max(0.03, 0.10 - rank * 0.001)
        candidates.add(
            model_id,
            score,
            "substrate:primary_candidate",
            source="substrate",
        )
    substrate.counters["model_priors_added"] += limit


def _candidate_terms(
    question: str,
    trigger: TriggerContext,
    cues: StructuredCues,
) -> list[str]:
    text = " ".join(
        [
            question or "",
            trigger.seed_natural_text or "",
            " ".join(cues.explicit_entities),
            " ".join(cues.aliases),
            " ".join(cues.system_mentions),
            " ".join(cues.customer_mentions),
            " ".join(_signature_entity_texts(trigger.seed_signature or {})),
        ]
    )
    tokens = sorted(_tokens(text), key=lambda t: (-len(t), t))
    phrase_terms = _phrase_terms(text)
    expanded_terms = _expanded_query_terms(text)
    compact_terms = sorted(_compact_terms(text), key=lambda t: (-len(t), t))
    alternative_search_terms = [
        term
        for alternative in extract_query_alternatives(question, include_quoted=True)
        for term in alternative_terms(alternative)
    ]
    out: list[str] = []
    for term in [
        *alternative_search_terms,
        *phrase_terms,
        *tokens,
        *expanded_terms,
        *compact_terms,
    ]:
        if term and term not in out:
            out.append(term)
        if len(out) >= 24:
            break
    return out


def _contains_like_patterns(terms: list[str] | tuple[str, ...]) -> list[str]:
    patterns: list[str] = []
    seen: set[str] = set()
    for raw in terms:
        term = str(raw or "").casefold().strip()
        if len(term) < 3:
            continue
        escaped = term.replace("!", "!!").replace("%", "!%").replace("_", "!_")
        pattern = f"%{escaped}%"
        if pattern in seen:
            continue
        seen.add(pattern)
        patterns.append(pattern)
        if len(patterns) >= _MAX_LEXICAL_DISCOVERY_PATTERNS:
            break
    return patterns


_BELIEF_ADDRESS_FTS_GENERIC_TERMS = {
    "action",
    "active",
    "alternate",
    "assigned",
    "block",
    "blocks",
    "blocked",
    "blocker",
    "capacity",
    "caused",
    "commitment",
    "constraint",
    "counterevidence",
    "customer",
    "dependency",
    "evidence",
    "explanation",
    "goal",
    "impact",
    "observation",
    "observations",
    "owner",
    "owned",
    "owns",
    "recent",
    "resource",
    "responsible",
}

_LEXICAL_DISCOVERY_GENERIC_TERMS = {
    *_BELIEF_ADDRESS_FTS_GENERIC_TERMS,
    "accountable",
    "blocking",
    "currently",
    "issue",
    "launch",
    "matching",
    "model",
    "models",
    "pattern",
    "recurring",
    "related",
    "repeated",
    "risk",
    "same",
    "showing",
    "similar",
    "specific",
    "stable",
    "status",
}
_SPARSE_LOOKUP_ALLOWED_GENERIC_TERMS = {
    # Expanded answer-side discriminator for blocker questions. The surface
    # question verb "blocks" remains generic; "blocked" helps find stored
    # assertions phrased as "X is blocked by Y".
    "blocked",
    # Evidence is generic alone, but useful as a discriminator in bounded
    # multi-term sparse lookups such as procurement/security evidence.
    "evidence",
}


def _belief_address_fts_query_for_terms(terms: list[str] | tuple[str, ...]) -> str:
    clauses: list[str] = []
    seen: set[str] = set()
    for raw in terms:
        raw_tokens = [
            token
            for token in re.findall(r"[a-z0-9][a-z0-9]{1,}", str(raw or "").casefold())
            if token not in _STOPWORDS and not token.isdigit()
        ]
        tokens = [token for token in raw_tokens if len(token) >= 3][:3]
        if not tokens:
            continue
        specific_tokens = [
            token for token in tokens if token not in _BELIEF_ADDRESS_FTS_GENERIC_TERMS
        ]
        if not specific_tokens:
            continue
        if len(specific_tokens) == 1:
            if len(specific_tokens[0]) < 6:
                continue
            clause = _fts_lexeme(specific_tokens[0])
        else:
            clause = (
                "("
                + " & ".join(_fts_lexeme(token) for token in specific_tokens[:3])
                + ")"
            )
        if clause in seen:
            continue
        seen.add(clause)
        clauses.append(clause)
        if len(clauses) >= 8:
            break
    return " | ".join(clauses)


def _fts_lexeme(token: str) -> str:
    clean = re.sub(r"[^a-z0-9]", "", token.casefold())
    if len(clean) >= 4:
        return f"{clean}:*"
    return clean


def _sparse_lookup_terms(
    terms: list[str] | tuple[str, ...],
    *,
    max_terms: int,
) -> list[str]:
    out: list[str] = []
    for raw in terms:
        raw_tokens = [
            token
            for token in re.findall(
                r"[a-z0-9][a-z0-9_-]{2,}", str(raw or "").casefold()
            )
            if token not in _STOPWORDS and not token.isdigit()
        ]
        has_specific_term = any(
            token not in _LEXICAL_DISCOVERY_GENERIC_TERMS
            or "-" in token
            or "_" in token
            or any(ch.isdigit() for ch in token)
            for token in raw_tokens
        )
        for token in raw_tokens:
            if token in _STOPWORDS or token.isdigit():
                continue
            symbol_specific = (
                "-" in token or "_" in token or any(ch.isdigit() for ch in token)
            )
            allowed_generic = token in _SPARSE_LOOKUP_ALLOWED_GENERIC_TERMS
            if (
                token in _LEXICAL_DISCOVERY_GENERIC_TERMS
                and not symbol_specific
                and (not allowed_generic or not has_specific_term)
            ):
                continue
            if len(token) < 4 and not symbol_specific:
                continue
            if token not in out:
                out.append(token)
            if len(out) >= max(1, int(max_terms)):
                return out
    return out


def _sparse_strong_single_match_terms(terms: list[str]) -> list[str]:
    return [
        term
        for term in terms
        if len(term) >= 4 and any(ch.isdigit() or ch in {"-", "_"} for ch in term)
    ]


def _sparse_lookup_groups(
    terms: list[str] | tuple[str, ...],
    *,
    max_groups: int,
    exclude_generics: bool = False,
) -> list[list[str]]:
    groups: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for raw in terms:
        tokens = [
            token
            for token in re.findall(
                r"[a-z0-9][a-z0-9_-]{2,}", str(raw or "").casefold()
            )
            if token not in _STOPWORDS and not token.isdigit()
        ]
        if exclude_generics:
            tokens = [
                token
                for token in tokens
                if token not in _BELIEF_ADDRESS_FTS_GENERIC_TERMS
            ]
        tokens = list(dict.fromkeys(tokens))
        if len(tokens) >= 2:
            group = tokens[:4]
        elif tokens and len(tokens[0]) >= 6:
            group = tokens
        else:
            continue
        key = tuple(group)
        if key in seen:
            continue
        seen.add(key)
        groups.append(group)
        if len(groups) >= max(1, int(max_groups)):
            break
    return groups


async def _fetch_search_document_matches(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    terms: list[str] | tuple[str, ...],
    limit: int,
    microquery_enabled: bool = False,
    microquery_terms: int = 8,
    microquery_per_term_limit: int = 16,
) -> list[asyncpg.Record]:
    sparse_rows = await _fetch_sparse_term_matches(
        conn,
        tenant_id=tenant_id,
        terms=terms,
        limit=limit,
        max_terms=microquery_terms,
        per_term_limit=microquery_per_term_limit,
    )
    if sparse_rows:
        return sparse_rows
    if _bounded_lookup_timed_out(sparse_rows):
        return []

    lookup_terms = _sparse_lookup_terms(terms, max_terms=microquery_terms)
    patterns = _contains_like_patterns(lookup_terms)
    if not patterns:
        return []
    if microquery_enabled and len(patterns) > 1:
        bounded_patterns = patterns[: max(1, int(microquery_terms))]
        per_term_limit = max(1, int(microquery_per_term_limit))
        rows = await _fetch_bounded_lookup_rows(
            conn,
            """
            WITH patterns AS (
              SELECT pattern, ord
              FROM unnest($3::text[]) WITH ORDINALITY AS p(pattern, ord)
            ),
            per_pattern AS MATERIALIZED (
              SELECT hit.model_id,
                     p.ord::int AS pattern_ord
              FROM patterns p
          CROSS JOIN LATERAL (
            SELECT msd.model_id
            FROM model_search_documents msd
            WHERE msd.tenant_id = $1
              AND msd.status = 'active'
              AND msd.search_text LIKE p.pattern ESCAPE '!'
            ORDER BY msd.model_id
            LIMIT $4
          ) hit
            ),
            scored AS MATERIALIZED (
              SELECT model_id,
                     count(*)::int AS match_count,
                     min(pattern_ord)::int AS first_pattern_ord
              FROM per_pattern
              GROUP BY model_id
            )
            SELECT m.id,
                   m."natural",
                   scored.match_count
            FROM scored
            JOIN models m
              ON m.id = scored.model_id
             AND m.tenant_id = $1
            WHERE m.status = 'active'
            ORDER BY scored.match_count DESC,
                     scored.first_pattern_ord ASC,
                     m.activation DESC,
                     m.created_at DESC
            LIMIT $2
            """,
            tenant_id,
            max(1, int(limit)),
            bounded_patterns,
            per_term_limit,
            label="search_documents_microquery",
        )
        if rows:
            return rows

    like_checks: list[str] = []
    count_parts: list[str] = []
    for offset in range(len(patterns)):
        param = f"${offset + 3}"
        check = f"msd.search_text LIKE {param} ESCAPE '!'"
        like_checks.append(check)
        count_parts.append(f"CASE WHEN {check} THEN 1 ELSE 0 END")
    match_expr = " + ".join(count_parts)
    where_expr = " OR ".join(like_checks)

    return await _fetch_bounded_lookup_rows(
        conn,
        f"""
        WITH matching AS MATERIALIZED (
            SELECT msd.model_id,
                   ({match_expr})::int AS match_count
            FROM model_search_documents msd
            WHERE msd.tenant_id = $1
              AND msd.status = 'active'
              AND ({where_expr})
            ORDER BY match_count DESC
            LIMIT $2
        )
        SELECT m.id,
               m."natural",
               matching.match_count
        FROM matching
        JOIN models m
          ON m.id = matching.model_id
        WHERE m.tenant_id = $1
          AND m.status = 'active'
        ORDER BY matching.match_count DESC, m.activation DESC, m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        *patterns,
        label="search_documents_like",
    )


async def _fetch_operational_role_matches(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_roles: list[str] | tuple[str, ...],
    terms: list[str] | tuple[str, ...],
    limit: int,
    per_role_limit: int,
) -> list[asyncpg.Record]:
    roles = [
        role
        for role in dict.fromkeys(
            str(raw or "").strip().casefold() for raw in seed_roles
        )
        if role
    ]
    if not roles:
        return []
    table = await conn.fetchval(
        "SELECT to_regclass('public.model_operational_role_postings')"
    )
    if table is None:
        return await _fetch_operational_role_matches_legacy(
            conn,
            tenant_id=tenant_id,
            seed_roles=roles,
            terms=terms,
            limit=limit,
        )
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH seed_roles AS MATERIALIZED (
          SELECT role::text,
                 ord::int AS role_ord
          FROM unnest($3::text[]) WITH ORDINALITY AS r(role, ord)
        ),
        query_terms AS MATERIALIZED (
          SELECT term::text
          FROM unnest($4::text[]) AS term(value)
          WHERE nullif(term.value, '') IS NOT NULL
        ),
        query_meta AS MATERIALIZED (
          SELECT count(*)::int AS query_term_count
          FROM query_terms
        ),
        role_hits AS MATERIALIZED (
          SELECT sr.role,
                 sr.role_ord,
                 hit.model_id,
                 hit.lexical_match_count
          FROM seed_roles sr
          CROSS JOIN LATERAL (
            SELECT morp.model_id,
                   coalesce(lexical.lexical_match_count, 0)::int
                     AS lexical_match_count
            FROM model_operational_role_postings morp
            JOIN models m
              ON m.id = morp.model_id
             AND m.tenant_id = $1
             AND m.status = 'active'
            LEFT JOIN model_search_documents msd
              ON msd.model_id = morp.model_id
             AND msd.tenant_id = $1
             AND msd.status = 'active'
            LEFT JOIN LATERAL (
              SELECT count(*)::int AS lexical_match_count
              FROM query_terms term
              WHERE strpos(coalesce(msd.search_text, ''), term.term) > 0
            ) lexical ON TRUE
            CROSS JOIN query_meta
            WHERE morp.tenant_id = $1
              AND morp.status = 'active'
              AND morp.role = sr.role
              AND (
                query_meta.query_term_count = 0
                OR coalesce(lexical.lexical_match_count, 0) > 0
              )
            ORDER BY coalesce(lexical.lexical_match_count, 0) DESC,
                     m.activation DESC,
                     m.created_at DESC,
                     morp.model_id
            LIMIT $5
          ) hit
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 array_agg(DISTINCT role ORDER BY role) AS matched_roles,
                 count(DISTINCT role)::int AS role_match_count,
                 min(role_ord)::int AS first_role_ord,
                 max(lexical_match_count)::int AS lexical_match_count
          FROM role_hits
          GROUP BY model_id
        )
        SELECT m.id,
               m."natural",
               scored.matched_roles,
               scored.role_match_count,
               scored.lexical_match_count
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scored.role_match_count DESC,
                 scored.lexical_match_count DESC,
                 scored.first_role_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        roles,
        [str(term or "").casefold() for term in terms],
        max(1, int(per_role_limit)),
        label="operational_role_postings",
    )
    if _bounded_lookup_timed_out(rows):
        return []
    return rows


async def _fetch_operational_role_matches_legacy(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_roles: list[str] | tuple[str, ...],
    terms: list[str] | tuple[str, ...],
    limit: int,
) -> list[asyncpg.Record]:
    rows = await _fetch_bounded_lookup_rows(
        conn,
        """
        SELECT m.id, m."natural",
               role_matches.matched_roles,
               role_matches.role_match_count,
               lexical.lexical_match_count
        FROM model_search_documents msd
        JOIN models m
          ON m.id = msd.model_id
         AND m.tenant_id = msd.tenant_id
        JOIN LATERAL (
          SELECT
            array_agg(role.value ORDER BY role.value) AS matched_roles,
            count(*)::int AS role_match_count
          FROM unnest($3::text[]) AS role(value)
          WHERE coalesce(m.proposition->'operational_roles', '[]'::jsonb)
                ? role.value
        ) role_matches ON role_matches.role_match_count > 0
        LEFT JOIN LATERAL (
          SELECT count(*)::int AS lexical_match_count
          FROM unnest($4::text[]) AS term(value)
          WHERE strpos(msd.search_text, term.value) > 0
        ) lexical ON TRUE
        WHERE msd.tenant_id = $1
          AND msd.status = 'active'
          AND m.status = 'active'
        ORDER BY role_matches.role_match_count DESC,
                 lexical.lexical_match_count DESC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        list(seed_roles),
        [str(term or "").casefold() for term in terms],
        label="operational_role_legacy",
    )
    if _bounded_lookup_timed_out(rows):
        return []
    return rows


async def _fetch_sparse_term_matches(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    terms: list[str] | tuple[str, ...],
    limit: int,
    max_terms: int,
    per_term_limit: int,
) -> list[asyncpg.Record]:
    lookup_terms = _sparse_lookup_terms(terms, max_terms=max_terms)
    if not lookup_terms or limit <= 0:
        return []
    table = await conn.fetchval("SELECT to_regclass('public.model_sparse_terms')")
    if table is None:
        return []
    return await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH query_terms AS MATERIALIZED (
          SELECT term::text,
                 ord::int AS term_ord
          FROM unnest($3::text[]) WITH ORDINALITY AS q(term, ord)
        ),
        query_meta AS MATERIALIZED (
          SELECT count(*)::int AS query_term_count
          FROM query_terms
        ),
        active_models AS MATERIALIZED (
          SELECT greatest(1, count(*)::int)::float8 AS active_model_count
          FROM models
          WHERE tenant_id = $1
            AND status = 'active'
        ),
        term_stats AS MATERIALIZED (
          SELECT qt.term,
                 qt.term_ord,
                 count(mst.model_id)::int AS term_df
          FROM query_terms qt
          LEFT JOIN model_sparse_terms mst
            ON mst.tenant_id = $1
           AND mst.status = 'active'
           AND mst.term = qt.term
          GROUP BY qt.term, qt.term_ord
        ),
        term_hits AS MATERIALIZED (
          SELECT ts.term,
                 ts.term_ord,
                 ts.term_df,
                 hit.model_id,
                 hit.weight,
                 (
                   ln((am.active_model_count + 1.0) / (ts.term_df::float8 + 1.0))
                   + 1.0
                 )::float8 AS idf
          FROM term_stats ts
          CROSS JOIN active_models am
          CROSS JOIN LATERAL (
            SELECT mst.model_id,
                   mst.weight
            FROM model_sparse_terms mst
            WHERE mst.tenant_id = $1
              AND mst.status = 'active'
              AND mst.term = ts.term
            ORDER BY mst.weight DESC,
                     mst.model_id
            LIMIT $4
          ) hit
        ),
        scored AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT term)::int AS match_count,
                 sum(weight * idf)::real AS weighted_score,
                 min(term_ord)::int AS first_term_ord,
                 bool_or(
                   term = ANY($5::text[])
                   AND term_df <= $6::int
                 ) AS has_strong_singleton
          FROM term_hits
          GROUP BY model_id
        )
        SELECT m.id,
               m."natural",
               scored.match_count
        FROM scored
        CROSS JOIN query_meta
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
          AND (
            query_meta.query_term_count <= 1
            OR scored.match_count >= LEAST(2, query_meta.query_term_count)
            OR scored.has_strong_singleton
          )
        ORDER BY scored.match_count DESC,
                 scored.weighted_score DESC,
                 scored.first_term_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        lookup_terms,
        max(1, int(per_term_limit)),
        _sparse_strong_single_match_terms(lookup_terms),
        _SPARSE_STRONG_SINGLE_MATCH_MAX_DF,
        label="sparse_terms",
    )


async def _fetch_bounded_lookup_rows(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
    label: str = "lookup",
) -> list[asyncpg.Record]:
    in_outer_transaction = bool(getattr(conn, "is_in_transaction", lambda: False)())
    previous_timeout: str | None = None
    if in_outer_transaction:
        previous_timeout = await conn.fetchval(
            "SELECT current_setting('statement_timeout')"
        )
    try:
        async with conn.transaction():
            if in_outer_transaction:
                await conn.fetchval(
                    "SELECT set_config('statement_timeout', $1, true)",
                    str(_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS),
                )
            else:
                await conn.execute(
                    "SET LOCAL statement_timeout = "
                    f"{_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS}"
                )
            return _BoundedLookupRows(list(await conn.fetch(query, *args)))
    except asyncpg.QueryCanceledError:
        _log.warning(
            "sage.reader.bounded_lookup_statement_timeout",
            label=label,
            timeout_ms=_LEXICAL_FALLBACK_STATEMENT_TIMEOUT_MS,
        )
        return _BoundedLookupRows(timed_out=True)
    finally:
        if (
            in_outer_transaction
            and previous_timeout is not None
            and bool(getattr(conn, "is_in_transaction", lambda: False)())
        ):
            try:
                await conn.fetchval(
                    "SELECT set_config('statement_timeout', $1, true)",
                    previous_timeout,
                )
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    "sage.reader.bounded_lookup_timeout_restore_failed",
                    label=label,
                    error=str(exc),
                )


async def _fetch_lexical_fallback_rows(
    conn: asyncpg.Connection,
    query: str,
    *args: Any,
) -> list[asyncpg.Record]:
    return await _fetch_bounded_lookup_rows(
        conn,
        query,
        *args,
        label="lexical_fallback",
    )


def _belief_address_primitives_for(primitive: str) -> tuple[str, ...]:
    coarse = str(primitive or "").strip().upper()
    if not coarse:
        return ()
    aliases = {
        "ACTION": ("COMMITMENT", "DEPENDENCY"),
        "COMMITMENT": ("COMMITMENT", "DEPENDENCY"),
        "CONSTRAINT": ("CONSTRAINT", "COUNTEREVIDENCE"),
        "DEPENDENCY": ("DEPENDENCY", "COMMITMENT"),
        "GOAL_IMPACT": ("GOAL_IMPACT",),
    }
    return aliases.get(coarse, (coarse,))


async def _fetch_belief_address_matches(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    primitives: list[str] | tuple[str, ...],
    terms: list[str] | tuple[str, ...],
    limit: int,
) -> list[asyncpg.Record]:
    table = await conn.fetchval("SELECT to_regclass('public.model_belief_addresses')")
    if table is None:
        return []
    primitive_values = [
        value
        for value in dict.fromkeys(str(raw or "").strip().upper() for raw in primitives)
        if value
    ]
    if not primitive_values:
        return []

    indexed_rows = await _fetch_answerability_index_matches(
        conn,
        tenant_id=tenant_id,
        primitive_values=primitive_values,
        terms=terms,
        limit=limit,
    )
    if indexed_rows:
        return indexed_rows
    if _bounded_lookup_timed_out(indexed_rows):
        return []

    fts_query = _belief_address_fts_query_for_terms(terms)
    if fts_query:
        rows = await _fetch_belief_address_matches_via_address_fts(
            conn,
            tenant_id=tenant_id,
            primitive_values=primitive_values,
            query=fts_query,
            limit=limit,
        )
        if rows:
            return rows
        if _bounded_lookup_timed_out(rows):
            return []

    patterns = _contains_like_patterns(terms)
    if patterns:
        search_table = await conn.fetchval(
            "SELECT to_regclass('public.model_search_documents')"
        )
        if search_table is not None:
            rows = await _fetch_belief_address_matches_via_search_documents(
                conn,
                tenant_id=tenant_id,
                primitive_values=primitive_values,
                patterns=patterns,
                limit=limit,
            )
            if rows:
                return rows
            if _bounded_lookup_timed_out(rows):
                return []

    address_text = "mba.search_text"
    count_parts: list[str] = []
    like_checks: list[str] = []
    for offset in range(len(patterns)):
        param = f"${offset + 5}"
        check = f"{address_text} LIKE {param} ESCAPE '!'"
        like_checks.append(check)
        count_parts.append(f"CASE WHEN {check} THEN 1 ELSE 0 END")
    match_expr = " + ".join(count_parts) if count_parts else "0"
    lexical_filter = f"AND ({' OR '.join(like_checks)})" if like_checks else ""

    return await _fetch_bounded_lookup_rows(
        conn,
        f"""
        WITH scored AS MATERIALIZED (
          SELECT mba.model_id,
                 cardinality(ARRAY(
                   SELECT primitive.value
                   FROM unnest(mba.answerable_primitives) AS primitive(value)
                   WHERE primitive.value = ANY($3::text[])
                 ))::int AS primitive_match_count,
                 ({match_expr})::int AS lexical_match_count,
                 ARRAY(
                   SELECT primitive.value
                   FROM unnest(mba.answerable_primitives) AS primitive(value)
                   WHERE primitive.value = ANY($3::text[])
                   ORDER BY primitive.value
                 ) AS matched_primitives,
                 mba.updated_at
          FROM model_belief_addresses mba
          WHERE mba.tenant_id = $1
            AND mba.status = 'active'
            AND mba.answerable_primitives && $3::text[]
            {lexical_filter}
        )
        SELECT m.id,
               m."natural",
               scored.primitive_match_count,
               scored.lexical_match_count,
               scored.matched_primitives,
               $4::boolean AS lexical_terms_present
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
          AND ($4::boolean IS FALSE OR scored.lexical_match_count > 0)
        ORDER BY scored.primitive_match_count DESC,
                 scored.lexical_match_count DESC,
                 m.activation DESC,
                 scored.updated_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        primitive_values,
        bool(patterns),
        *patterns,
        label="belief_address_like",
    )


async def _fetch_belief_address_matches_via_address_fts(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    primitive_values: list[str],
    query: str,
    limit: int,
) -> list[asyncpg.Record]:
    table = await conn.fetchval("SELECT to_regclass('public.model_belief_addresses')")
    if table is None:
        return []
    return await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH query AS (
          SELECT to_tsquery('simple', $4) AS tsq
        ),
        scored AS MATERIALIZED (
          SELECT mba.model_id,
                 cardinality(ARRAY(
                   SELECT primitive.value
                   FROM unnest(mba.answerable_primitives) AS primitive(value)
                   WHERE primitive.value = ANY($3::text[])
                 ))::int AS primitive_match_count,
                 greatest(
                   1,
                   ceil(ts_rank_cd(to_tsvector('simple', mba.search_text), query.tsq) * 100)::int
                 ) AS lexical_match_count,
                 ts_rank_cd(to_tsvector('simple', mba.search_text), query.tsq) AS fts_rank,
                 ARRAY(
                   SELECT primitive.value
                   FROM unnest(mba.answerable_primitives) AS primitive(value)
                   WHERE primitive.value = ANY($3::text[])
                   ORDER BY primitive.value
                 ) AS matched_primitives,
                 mba.updated_at
          FROM model_belief_addresses mba, query
          WHERE mba.tenant_id = $1
            AND mba.status = 'active'
            AND mba.answerable_primitives && $3::text[]
            AND to_tsvector('simple', mba.search_text) @@ query.tsq
          ORDER BY primitive_match_count DESC,
                   fts_rank DESC,
                   mba.updated_at DESC
          LIMIT $2
        )
        SELECT m.id,
               m."natural",
               scored.primitive_match_count,
               scored.lexical_match_count,
               scored.matched_primitives,
               TRUE AS lexical_terms_present
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scored.primitive_match_count DESC,
                 scored.fts_rank DESC,
                 m.activation DESC,
                 scored.updated_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        primitive_values,
        query,
        label="belief_address_fts",
    )


async def _fetch_belief_address_matches_via_search_documents(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    primitive_values: list[str],
    patterns: list[str],
    limit: int,
) -> list[asyncpg.Record]:
    like_checks: list[str] = []
    count_parts: list[str] = []
    for offset in range(len(patterns)):
        param = f"${offset + 5}"
        check = f"msd.search_text LIKE {param} ESCAPE '!'"
        like_checks.append(check)
        count_parts.append(f"CASE WHEN {check} THEN 1 ELSE 0 END")
    match_expr = " + ".join(count_parts)
    where_expr = " OR ".join(like_checks)
    lexical_limit = max(int(limit) * 8, 96)

    return await _fetch_bounded_lookup_rows(
        conn,
        f"""
        WITH lexical AS MATERIALIZED (
          SELECT msd.model_id,
                 ({match_expr})::int AS lexical_match_count
          FROM model_search_documents msd
          WHERE msd.tenant_id = $1
            AND msd.status = 'active'
            AND ({where_expr})
          ORDER BY lexical_match_count DESC
          LIMIT $4
        ),
        scored AS MATERIALIZED (
          SELECT mba.model_id,
                 cardinality(ARRAY(
                   SELECT primitive.value
                   FROM unnest(mba.answerable_primitives) AS primitive(value)
                   WHERE primitive.value = ANY($3::text[])
                 ))::int AS primitive_match_count,
                 lexical.lexical_match_count,
                 ARRAY(
                   SELECT primitive.value
                   FROM unnest(mba.answerable_primitives) AS primitive(value)
                   WHERE primitive.value = ANY($3::text[])
                   ORDER BY primitive.value
                 ) AS matched_primitives,
                 mba.updated_at
          FROM lexical
          JOIN model_belief_addresses mba
            ON mba.model_id = lexical.model_id
           AND mba.tenant_id = $1
          WHERE mba.status = 'active'
            AND mba.answerable_primitives && $3::text[]
        )
        SELECT m.id,
               m."natural",
               scored.primitive_match_count,
               scored.lexical_match_count,
               scored.matched_primitives,
               TRUE AS lexical_terms_present
        FROM scored
        JOIN models m
          ON m.id = scored.model_id
         AND m.tenant_id = $1
        WHERE m.status = 'active'
        ORDER BY scored.primitive_match_count DESC,
                 scored.lexical_match_count DESC,
                 m.activation DESC,
                 scored.updated_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        primitive_values,
        lexical_limit,
        *patterns,
        label="belief_address_search_documents",
    )


async def _fetch_answerability_index_matches(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    primitive_values: list[str],
    terms: list[str] | tuple[str, ...],
    limit: int,
) -> list[asyncpg.Record]:
    lookup_groups = _sparse_lookup_groups(
        terms,
        max_groups=8,
        exclude_generics=True,
    )
    if not lookup_groups:
        return []
    table = await conn.fetchval(
        "SELECT to_regclass('public.model_answerability_index')"
    )
    if table is None:
        return []
    per_token_limit = max(16, min(48, max(1, int(limit)) * 4))
    max_term_df = _answerability_max_term_df(limit)
    return await _fetch_bounded_lookup_rows(
        conn,
        """
        WITH raw_group_tokens AS MATERIALIZED (
          SELECT g.group_ord::int,
                 token.value::text AS term
          FROM jsonb_array_elements($4::jsonb)
               WITH ORDINALITY AS g(tokens, group_ord)
          CROSS JOIN LATERAL jsonb_array_elements_text(g.tokens) AS token(value)
        ),
        primitive_tokens AS MATERIALIZED (
          SELECT gt.term,
                 gt.group_ord,
                 primitive.value::text AS primitive
          FROM raw_group_tokens gt
          CROSS JOIN unnest($3::text[]) AS primitive(value)
        ),
        token_stats AS MATERIALIZED (
          SELECT pt.term,
                 pt.group_ord,
                 pt.primitive,
                 stats.term_df
          FROM primitive_tokens pt
          CROSS JOIN LATERAL (
            SELECT count(*)::int AS term_df
            FROM (
              SELECT 1
              FROM model_answerability_index mai
              WHERE mai.tenant_id = $1
                AND mai.status = 'active'
                AND mai.primitive = pt.primitive
                AND mai.term = pt.term
              LIMIT $6
            ) bounded
          ) stats
          WHERE stats.term_df > 0
            AND stats.term_df <= $7
        ),
        group_sizes AS MATERIALIZED (
          SELECT group_ord,
                 primitive,
                 count(DISTINCT term)::int AS token_count
          FROM token_stats
          GROUP BY group_ord, primitive
        ),
        token_hits AS MATERIALIZED (
          SELECT pt.term,
                 pt.group_ord,
                 hit.model_id,
                 pt.primitive,
                 hit.weight
          FROM token_stats pt
          CROSS JOIN LATERAL (
            SELECT mai.model_id,
                   mai.weight
            FROM model_answerability_index mai
            WHERE mai.tenant_id = $1
              AND mai.status = 'active'
              AND mai.primitive = pt.primitive
              AND mai.term = pt.term
            ORDER BY mai.weight DESC,
                     mai.model_id
            LIMIT $5
          ) hit
        ),
        matched AS MATERIALIZED (
          SELECT mai.model_id,
                 mai.primitive,
                 mai.group_ord,
                 count(DISTINCT mai.term)::int AS matched_terms,
                 sum(mai.weight)::real AS weighted_score
          FROM token_hits mai
          GROUP BY mai.model_id, mai.primitive, mai.group_ord
        ),
        group_hits AS MATERIALIZED (
          SELECT matched.model_id,
                 matched.primitive,
                 matched.group_ord,
                 group_sizes.token_count,
                 matched.weighted_score
          FROM matched
          JOIN group_sizes
            ON group_sizes.group_ord = matched.group_ord
           AND group_sizes.primitive = matched.primitive
          WHERE matched.matched_terms = group_sizes.token_count
        ),
        matched_groups AS MATERIALIZED (
          SELECT model_id,
                 group_ord,
                 max(token_count)::int AS token_count,
                 sum(weighted_score)::real AS weighted_score
          FROM group_hits
          GROUP BY model_id, group_ord
        ),
        matched_primitives AS MATERIALIZED (
          SELECT model_id,
                 count(DISTINCT primitive)::int AS primitive_match_count,
                 array_agg(DISTINCT primitive ORDER BY primitive) AS matched_primitives
          FROM group_hits
          GROUP BY model_id
        ),
        scored AS MATERIALIZED (
          SELECT matched_groups.model_id,
                 matched_primitives.primitive_match_count,
                 sum(matched_groups.token_count)::int AS lexical_match_count,
                 matched_primitives.matched_primitives,
                 min(matched_groups.group_ord)::int AS first_group_ord,
                 sum(matched_groups.weighted_score)::real AS weighted_score
          FROM matched_groups
          JOIN matched_primitives
            ON matched_primitives.model_id = matched_groups.model_id
          GROUP BY matched_groups.model_id,
                   matched_primitives.primitive_match_count,
                   matched_primitives.matched_primitives
        )
        SELECT m.id,
               m."natural",
               scored.primitive_match_count,
               scored.lexical_match_count,
               scored.matched_primitives,
               TRUE AS lexical_terms_present
        FROM scored
        JOIN LATERAL (
          SELECT models.id,
                 models."natural",
                 models.activation,
                 models.created_at
          FROM models
          WHERE models.id = scored.model_id
            AND models.tenant_id = $1
            AND models.status = 'active'
          LIMIT 1
        ) m ON TRUE
        ORDER BY scored.primitive_match_count DESC,
                 scored.lexical_match_count DESC,
                 scored.weighted_score DESC,
                 scored.first_group_ord ASC,
                 m.activation DESC,
                 m.created_at DESC
        LIMIT $2
        """,
        tenant_id,
        max(1, int(limit)),
        primitive_values,
        json.dumps(lookup_groups),
        per_token_limit,
        max_term_df + 1,
        max_term_df,
        label="answerability_index",
    )


def _lexical_activation(
    model: ModelRow,
    question: str,
    trigger: TriggerContext,
    cues: StructuredCues,
) -> tuple[float, list[str]]:
    model_text = " ".join(
        [
            model.natural or "",
            json.dumps(model.proposition or {}, default=str),
            " ".join(str(tag) for tag in model.domain_tags or []),
        ]
    )
    answer_lower = _questionless_text(model_text, question)
    query_tokens = _tokens(" ".join([question or "", trigger.seed_natural_text or ""]))
    model_tokens = _tokens(model_text)
    overlap = query_tokens & model_tokens
    compact_overlap = _compact_terms(
        " ".join([question or "", trigger.seed_natural_text or ""])
    ) & _compact_terms(model_text)
    score = 0.0
    reasons: list[str] = []
    if overlap:
        score += min(0.20, 0.045 * len(overlap))
        reasons.append("lexical:" + ",".join(sorted(overlap)[:4]))
    if compact_overlap:
        score += min(0.16, 0.08 * len(compact_overlap))
        reasons.append("lexical_compact:" + ",".join(sorted(compact_overlap)[:3]))
    model_lower = model_text.casefold()
    for entity in list(cues.explicit_entities) + list(cues.aliases):
        entity_text = str(entity).strip()
        if entity_text and entity_text.casefold() in model_lower:
            score += 0.18
            reasons.append(f"exact:{entity_text[:40]}")
            break
    score += _question_role_score(answer_lower, question, cues, reasons)
    echo_penalty = _question_echo_penalty(model_text, question, reasons)
    if echo_penalty:
        score -= echo_penalty
    trigger_pairs = {
        (str(e.get("type")), str(e.get("id")))
        for e in trigger.seed_entity_ids
        if isinstance(e, dict) and e.get("type") and e.get("id")
    }
    model_pairs = {
        (str(e.get("type")), str(e.get("id")))
        for e in model.scope_entities or []
        if isinstance(e, dict) and e.get("type") and e.get("id")
    }
    if trigger_pairs and trigger_pairs & model_pairs:
        score += 0.22
        reasons.append("shared_scope_entity")
    facet_score, facet_reasons = _operational_facet_activation(
        model.proposition if isinstance(model.proposition, dict) else {},
        question,
    )
    if facet_score:
        score += facet_score
        reasons.extend(facet_reasons)
    alternative_score, alternative_reasons = _alternative_activation(
        model_text,
        question,
    )
    if alternative_score:
        score += alternative_score
        reasons.extend(alternative_reasons)
    return max(0.0, min(score, 0.58)), reasons


def _model_stable_sort_key(model: ModelRow) -> tuple[str, str, str, str]:
    """Evidence-stable ordering key for SAGE activation ties."""
    proposition = json.dumps(
        model.proposition or {},
        sort_keys=True,
        default=str,
    )
    scope_temporal = json.dumps(
        model.scope_temporal or {},
        sort_keys=True,
        default=str,
    )
    natural = _normalize_words(model.natural or "")
    return (natural, proposition, scope_temporal, str(model.id))


_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "because",
    "been",
    "from",
    "have",
    "into",
    "need",
    "needs",
    "that",
    "the",
    "their",
    "this",
    "with",
    "without",
    "what",
    "which",
    "would",
    "should",
    "actually",
    "where",
    "when",
    "whose",
    "who",
    "why",
    "how",
    "sure",
    "only",
    "here",
    "there",
    "current",
    "company",
    "fyralis",
}


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.casefold())
        if token not in _STOPWORDS and not token.isdigit()
    }


def _phrase_terms(text: str) -> list[str]:
    raw_tokens = [
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.casefold())
        if token not in _STOPWORDS and not token.isdigit()
    ]
    out: list[str] = []
    for width in (3, 2):
        for idx in range(0, max(0, len(raw_tokens) - width + 1)):
            phrase = " ".join(raw_tokens[idx : idx + width])
            if phrase not in out:
                out.append(phrase)
    return out[:8]


def _compact_terms(text: str) -> set[str]:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{1,}", text.casefold())
        if token not in _STOPWORDS and not token.isdigit()
    ]
    out: set[str] = set()
    for idx in range(len(tokens) - 1):
        left = tokens[idx]
        right = tokens[idx + 1]
        compact = re.sub(r"[^a-z0-9]", "", left + right)
        if len(compact) >= 5:
            out.add(compact)
    for token in tokens:
        compact = re.sub(r"[^a-z0-9]", "", token)
        if len(compact) >= 5:
            out.add(compact)
    return out


def _expanded_query_terms(text: str) -> list[str]:
    """Bridge common Ask phrasing onto model phrasing.

    The reader is often handed a natural question, while stored Synthesis
    claims use assertion verbs. These expansions are deliberately small and
    primitive-shaped so lexical search can find candidates before richer
    scoring has a chance to run.
    """

    q = _normalize_words(text)
    out: list[str] = []

    def add(*terms: str) -> None:
        for term in terms:
            if term and term not in out:
                out.append(term)

    if any(term in q for term in ("failed", "failure", "repeat", "repeatedly")):
        add("recur", "recurs", "recurring", "stalls", "stall", "failure")
    if any(term in q for term in ("bottleneck", "constrain", "resource")):
        add("constrained", "constraint", "quota", "capacity", "exhaustion")
    if any(term in q for term in ("block", "dependency", "depend")):
        add("blocked", "blocker", "depends", "capacity")
    if any(term in q for term in ("owner", "owns", "responsible", "who owns")):
        add("owner", "assigned", "responsible", "accountable")
    if any(term in q for term in ("contradict", "counterevidence", "disprove")):
        add("contradicted", "contradicts", "signed", "resolved", "falsifies")
    if any(term in q for term in ("next", "action", "should we do")):
        add("next action", "assigning", "owner", "mitigation")
    if any(term in q for term in ("goal", "affected", "impact")):
        add("goal", "risk", "at risk", "affected", "impact")
    if any(
        term in q
        for term in ("which", "compare", "comparison", "option", "choice", "between")
    ):
        add("compare", "choice", "option", "selected", "largest", "least", "count")
    if any(term in q for term in ("current", "latest", "final", "last", "as of")):
        add("current", "latest", "final", "state", "status")
    if any(
        term in q
        for term in ("exact", "how many", "number", "count", "price", "quantity")
    ):
        add("count", "quantity", "number", "price", "value")
    return out[:10]


def _alternative_activation(model_text: str, question: str) -> tuple[float, list[str]]:
    alternatives = extract_query_alternatives(question)
    if not alternatives:
        return 0.0, []
    matched = _matched_alternatives(model_text, alternatives)
    if not matched:
        return 0.0, []
    score = min(0.22, 0.12 + 0.04 * len(matched))
    return score, ["alternative:" + ",".join(matched[:3])]


def _operational_facet_activation(
    proposition: dict[str, Any],
    question: str,
) -> tuple[float, list[str]]:
    plan = infer_operational_query_plan(question)
    if not plan.roles:
        return 0.0, []
    raw_roles = proposition.get("operational_roles")
    if not isinstance(raw_roles, list):
        return 0.0, []
    roles = {str(role) for role in raw_roles if role is not None}
    overlap = roles & set(plan.roles)
    if not overlap:
        return 0.0, []
    score = min(0.18, 0.08 + 0.035 * len(overlap))
    return score, ["operational_role:" + ",".join(sorted(overlap)[:4])]


def _matched_alternatives(
    text: str,
    alternatives: tuple[str, ...] | list[str],
) -> list[str]:
    normalized = _normalize_words(text)
    compact_text = compact_alternative_key(text)
    matched: list[str] = []
    for alternative in alternatives:
        alt_norm = _normalize_words(alternative)
        alt_compact = compact_alternative_key(alternative)
        if not alt_norm:
            continue
        if alt_norm in normalized or (alt_compact and alt_compact in compact_text):
            matched.append(_normalize_words(alternative)[:48])
    return matched


def _alternative_seed_score(
    *,
    natural: str,
    question: str,
    alternative_count: int,
    match_count: int,
    rank: int,
) -> float:
    score = 0.16 + 0.045 * max(1, alternative_count) + 0.018 * max(1, match_count)
    score -= rank * 0.002
    penalty = _question_echo_penalty(natural, question, [])
    if penalty:
        score -= min(0.16, penalty)
    return _clamp(score, 0.08, 0.34)


def _lexical_seed_score(
    *,
    natural: str,
    question: str,
    match_count: int,
    rank: int,
) -> float:
    score = min(0.36, max(0.10, 0.13 + 0.025 * match_count - rank * 0.002))
    penalty = _question_echo_penalty(natural, question, [])
    if penalty:
        score -= min(0.18, penalty)
    return _clamp(score, 0.06, 0.36)


def _signature_entity_texts(signature: dict[str, Any]) -> list[str]:
    entities = signature.get("entities")
    if not isinstance(entities, list):
        return []
    return [str(entity) for entity in entities if entity is not None]


def _question_role_score(
    model_lower: str,
    question: str,
    cues: StructuredCues,
    reasons: list[str],
) -> float:
    q = _normalize_words(question)
    relation_clues = {str(clue).casefold() for clue in cues.relationship_clues or ()}
    score = 0.0
    if _looks_like_non_answer_evidence(model_lower):
        return score
    if (
        "blocks" in relation_clues
        or "depends_on" in relation_clues
        or "critical_path" in relation_clues
        or any(
            term in q
            for term in (
                "block",
                "dependency",
                "critical path",
                "risk",
                "bottleneck",
                "constrain",
                "resource",
            )
        )
    ) and any(
        term in model_lower
        for term in (
            "block",
            "depend",
            "critical path",
            "constraint",
            "constrain",
            "bottleneck",
            "quota",
            "exhaust",
        )
    ):
        score += 0.16
        reasons.append("role:blocker")
    if (
        "owns" in relation_clues
        or "assigned_to" in relation_clues
        or any(term in q for term in ("owner", "owns", "responsible", "accountable"))
    ) and any(
        term in model_lower
        for term in ("owner", "owns", "assigned", "responsible", "accountable")
    ):
        score += 0.14
        reasons.append("role:owner")
    if (
        "contradicts" in relation_clues
        or any(
            term in q
            for term in ("contradict", "counterevidence", "disprove", "wrong", "stale")
        )
    ) and any(
        term in model_lower
        for term in (
            "contradict",
            "counter",
            "disprove",
            "not blocked",
            "resolved",
            "stale",
        )
    ):
        score += 0.16
        reasons.append("role:counterevidence")
    if (
        (
            "recurring" in relation_clues
            or any(
                term in q
                for term in (
                    "recurring",
                    "repeat",
                    "repeated",
                    "repeatedly",
                    "again",
                    "pattern",
                    "failed",
                    "failure",
                )
            )
        )
        and any(
            term in model_lower
            for term in (
                "recurring",
                "recur",
                "repeat",
                "again",
                "pattern",
                "every",
                "stalls",
                "stall",
                "failed",
                "failure",
            )
        )
        and not any(
            term in model_lower for term in ("isolated", "one off", "single retry")
        )
    ):
        score += 0.10
        reasons.append("role:pattern")
    return score


def _looks_like_non_answer_evidence(model_lower: str) -> bool:
    return any(
        term in model_lower
        for term in (
            "distractor",
            "mentions not",
            "generic dashboard",
            "dashboard hub",
            "unrelated model",
            "without actionable synthesis",
            "without the target answer",
        )
    )


def _questionless_text(model_text: str, question: str) -> str:
    model_norm = _normalize_words(model_text)
    question_norm = _normalize_words(question)
    if question_norm:
        model_norm = model_norm.replace(question_norm, " ")
    return re.sub(r"\s+", " ", model_norm).strip()


def _question_echo_penalty(
    model_text: str,
    question: str,
    reasons: list[str],
) -> float:
    model_norm = _normalize_words(model_text)
    question_norm = _normalize_words(question)
    if not model_norm or not question_norm:
        return 0.0
    question_tokens = _tokens(question_norm)
    if not question_tokens:
        return 0.0
    model_tokens = _tokens(model_norm)
    overlap_ratio = len(question_tokens & model_tokens) / max(len(question_tokens), 1)
    echo_markers = (
        "generic dashboard",
        "dashboard hub",
        "repeats",
        "noisy dashboard",
        "without actionable synthesis",
        "without the target answer",
    )
    if overlap_ratio < 0.75 or not any(marker in model_norm for marker in echo_markers):
        return 0.0
    if "question_echo" not in reasons:
        reasons.append("question_echo")
    return 0.22


def _normalize_words(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text.casefold())).strip()


def _required_roles_for(primitive: str) -> tuple[str, ...]:
    if primitive in {"DEPENDENCY", "CONSTRAINT"}:
        return ("blocker", "commitment", "counterevidence")
    if primitive in {"COUNTEREVIDENCE", "FALSIFICATION"}:
        return ("counterevidence", "falsifier")
    if primitive == "OWNERSHIP":
        return ("owner", "commitment")
    return ()


def _shortcut_role_for_signature(signature: dict[str, Any] | None) -> str | None:
    if not isinstance(signature, dict):
        return None
    primitive = str(signature.get("question_primitive") or "").upper()
    if primitive in {"DEPENDENCY", "CONSTRAINT"}:
        return "blocker"
    if primitive == "OWNERSHIP":
        return "owner"
    if primitive in {"COUNTEREVIDENCE", "FALSIFICATION"}:
        return "counterevidence"
    if primitive == "ACTION":
        return "commitment"
    return None


def _coerce_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _coerce_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
    return []


def _negative_memory_model_ids(value: Any) -> set[UUID]:
    out: set[UUID] = set()

    def visit(raw: Any) -> None:
        if raw is None:
            return
        if isinstance(raw, UUID):
            out.add(raw)
            return
        if isinstance(raw, str):
            try:
                out.add(UUID(raw))
            except ValueError:
                return
            return
        if isinstance(raw, dict):
            for key in (
                "model_id",
                "source_model_id",
                "target_model_id",
                "to_model_id",
                "from_model_id",
            ):
                if key in raw:
                    visit(raw[key])
            for key in ("model_ids", "path", "nodes", "models"):
                if key in raw:
                    visit(raw[key])
            return
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                visit(item)

    visit(value)
    return out


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value


def _counter_delta(current: Counter[str], previous: Counter[str]) -> dict[str, int]:
    keys = set(current) | set(previous)
    return {
        key: int(current.get(key, 0) - previous.get(key, 0))
        for key in sorted(keys)
        if current.get(key, 0) != previous.get(key, 0)
    }


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def activation_trace_insert_params(
    *,
    tenant_id: UUID,
    inquiry_session_id: UUID,
    trace: ReaderActivationTrace,
) -> tuple[Any, ...]:
    return (
        uuid7(),
        tenant_id,
        inquiry_session_id,
        trace.question_id,
        trace.model_id,
        float(trace.activation_score),
        json.dumps(list(trace.activation_reasons), default=str),
        bool(trace.selected),
        trace.selection_rank,
        json.dumps(trace.source_breakdown, default=str),
    )


__all__ = [
    "ReaderActivationTrace",
    "ReaderBudget",
    "SynthesisReader",
    "SynthesisReaderResult",
    "activation_trace_insert_params",
]
