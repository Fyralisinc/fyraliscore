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
from collections import defaultdict
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
from services.reasoning.sage.evidence_projection import EvidenceProjector
from services.reasoning.sage.intent_inferer import RetrievalIntent, RetrievalIntentInferer
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


_log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class ReaderBudget:
    max_nodes: int = 80
    max_edges: int = 120
    max_evidence_items: int = 100
    lexical_candidates: int = 40
    shortcut_candidates: int = 12
    affordance_candidates: int = 40
    propagation_neighbors: int = 80


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
        self._projector = EvidenceProjector()

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
    ) -> SynthesisReaderResult:
        cues = await CueExtractor(
            pool=self._pool,
            tenant_id=tenant_id,
            alias_loader=(
                lambda: _load_aliases_with_conn(conn, tenant_id)
            ),
        ).extract(
            signal=_signal_payload(trigger),
            question=question,
            hypotheses=[_hypothesis_text(h) for h in hypotheses],
            evidence_state=None,
        )
        intents = tuple(self._intent_inferer.infer(
            cues=cues,
            evidence_state=None,
            question_id=question_id,
            question_text=question,
        ))
        primitive = _coarse_primitive(question_primitive, intents)
        signature = _signature_for(trigger, primitive, cues)

        candidates = _CandidateAccumulator()
        _add_explicit_trigger_models(candidates, trigger)
        await self._activate_from_shortcuts(
            conn, tenant_id, signature, candidates,
        )
        await self._activate_from_affordances(
            conn, tenant_id, primitive, signature, candidates,
        )
        await self._activate_from_lexical_scan(
            conn, tenant_id, question, trigger, cues, candidates,
        )
        await self._suppress_from_negative_memory(
            conn, tenant_id, signature, candidates,
        )

        seed_ids = list(candidates.model_ids)
        edges = await _load_candidate_edges(
            conn,
            tenant_id=tenant_id,
            seed_model_ids=seed_ids,
            limit=self._budget.propagation_neighbors,
        )
        model_ids = set(seed_ids)
        for edge in edges:
            model_ids.add(edge["source_model_id"])
            model_ids.add(edge["target_model_id"])

        models = await _load_models(conn, tenant_id, sorted(model_ids, key=str))
        features = await _load_model_features(conn, tenant_id, list(models))
        edge_features = await _load_edge_features(
            conn, tenant_id, [r["id"] for r in edges],
        )

        gate_edges: list[CandidateEdge] = []
        gate_debug: dict[str, dict[str, Any]] = {}
        for edge in edges:
            source_id = edge["source_model_id"]
            target_id = edge["target_model_id"]
            gate = self._gate_scorer.score(
                gate_inputs=GateInputs(
                    edge_type=str(edge["edge_kind"]),
                    edge_confidence=float(edge["weight"] or 0.65),
                    edge_updated_at=edge["created_at"] or datetime.now(timezone.utc),
                    source_features=features.get(source_id),
                    target_features=features.get(target_id),
                    edge_features=edge_features.get(edge["id"]),
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

        activated_nodes: list[ActivatedNode] = []
        activation_details: dict[UUID, _CandidateScore] = {}
        for mid, model in models.items():
            score = candidates.score_for(mid)
            detail = candidates.details[mid]
            lexical_score, lexical_reasons = _lexical_activation(
                model, question, trigger, cues,
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
                    structural_features=features.get(mid),
                )
            )

        selection = self._selector.select(
            activated_nodes=activated_nodes,
            candidate_edges=gate_edges,
            question_primitive=primitive,
            required_evidence_roles=_required_roles_for(primitive),
            known_counterevidence_node_ids=tuple(
                n.model_id
                for n in activated_nodes
                if any("counterevidence" in r for r in n.activation_reasons)
            ),
        )
        selected_model_ids = list(selection.selected_nodes)
        projection = await self._projector.project(
            pool=self._pool,
            tenant_id=tenant_id,
            selected_model_ids=selected_model_ids,
            question_primitive=primitive,
            conn=conn,
        )
        projected_dicts = tuple(
            _jsonable(asdict(candidate)) for candidate in projection.projected
        )
        observation_ids = [
            item.evidence_id
            for item in projection.projected
            if item.evidence_kind == "observation"
        ]
        observations = await _load_observations(conn, tenant_id, observation_ids)

        selection_rank = {mid: idx for idx, mid in enumerate(selected_model_ids)}
        traces: list[ReaderActivationTrace] = []
        for node in sorted(
            activated_nodes, key=lambda n: (-n.activation_score, str(n.model_id)),
        )[: self._budget.max_nodes * 3]:
            detail = activation_details[node.model_id]
            selected = node.model_id in selection_rank
            traces.append(
                ReaderActivationTrace(
                    question_id=question_id,
                    model_id=node.model_id,
                    activation_score=node.activation_score,
                    activation_reasons=node.activation_reasons,
                    selected=selected,
                    selection_rank=selection_rank.get(node.model_id),
                    source_breakdown=dict(detail.sources),
                )
            )

        selected_models = tuple(
            models[mid] for mid in selected_model_ids if mid in models
        )
        model_scores = {
            mid: activation_details.get(mid, _CandidateScore()).score
            for mid in selected_model_ids
        }
        pathway = PathwayResult(
            models=list(selected_models),
            observations=list(observations),
            source_pathway="SAGE",  # type: ignore[arg-type]
            notes={
                "sage_reader": True,
                "question_id": question_id,
                "question_primitive": primitive,
                "signature": signature,
                "selected_model_ids": [str(mid) for mid in selected_model_ids],
                "projected_evidence_count": len(projected_dicts),
            },
        )
        debug = {
            "cue_extraction": _jsonable(asdict(cues)),
            "intents": [_jsonable(asdict(intent)) for intent in intents],
            "activation_reasons": {
                str(trace.model_id): list(trace.activation_reasons)
                for trace in traces
            },
            "gate_scores": gate_debug,
            "selector": {
                "selected_nodes": [str(mid) for mid in selection.selected_nodes],
                "selected_edges": [str(eid) for eid in selection.selected_edges],
                "bridge_nodes": [str(mid) for mid in selection.bridge_nodes],
                "coverage_metrics": selection.coverage_metrics,
            },
            "projection_coverage": projection.coverage,
        }
        return SynthesisReaderResult(
            question_id=question_id,
            question_primitive=primitive,
            signature=signature,
            cues=cues,
            intents=intents,
            activations=tuple(traces),
            selection=selection,
            projected_evidence=projected_dicts,
            omitted_projection=tuple((str(mid), reason) for mid, reason in projection.omitted),
            models=selected_models,
            observations=tuple(observations),
            model_scores=model_scores,
            pathway_result=pathway,
            debug=debug,
        )

    async def _activate_from_shortcuts(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
    ) -> None:
        if not signature:
            return
        try:
            shortcuts = await DiscoveryShortcutsRepo(
                self._pool, tenant_id=tenant_id,
            ).find_for_signature(
                signature, limit=self._budget.shortcut_candidates, conn=conn,
            )
        except Exception as exc:  # noqa: BLE001
            _log.debug("sage.reader.shortcuts_unavailable", error=str(exc))
            return
        for shortcut in shortcuts:
            score = min(0.42, 0.22 + 0.08 * float(shortcut.utility_score or 0.0))
            if shortcut.to_model_id is not None:
                candidates.add(
                    shortcut.to_model_id,
                    score,
                    f"shortcut:{shortcut.id}",
                    source="shortcut",
                )

    async def _activate_from_affordances(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        primitive: str,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
    ) -> None:
        entities = _signature_entities(signature)
        try:
            profiles = await AffordanceProfilesRepo(
                self._pool, tenant_id=tenant_id,
            ).search_by_primitive_context(
                primitive,
                entities=entities,
                limit=self._budget.affordance_candidates,
                min_utility=0.0,
                conn=conn,
            )
        except Exception as exc:  # noqa: BLE001
            _log.debug("sage.reader.affordances_unavailable", error=str(exc))
            return
        for profile in profiles:
            overlap = _activation_signature_overlap(
                profile.activation_signatures,
                entities,
            )
            context_boost = min(0.14, 0.07 * overlap)
            score = min(
                0.44,
                0.16 + 0.045 * float(profile.utility_score or 0.0)
                + context_boost,
            )
            candidates.add(
                profile.model_id,
                score,
                (
                    f"affordance:{primitive}:context"
                    if overlap > 0 else f"affordance:{primitive}"
                ),
                source="affordance",
            )

    async def _activate_from_lexical_scan(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        question: str,
        trigger: TriggerContext,
        cues: StructuredCues,
        candidates: "_CandidateAccumulator",
    ) -> None:
        if candidates.contextual_hit_count >= 2:
            return
        terms = _candidate_terms(question, trigger, cues)
        if not terms:
            return
        remaining = max(
            0,
            int(self._budget.lexical_candidates) - len(candidates.model_ids),
        )
        if remaining <= 0:
            return
        conditions = " OR ".join(
            f'"natural" ILIKE ${idx}'
            for idx in range(3, 3 + len(terms))
        )
        rows = await conn.fetch(
            f"""
            SELECT id
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND ({conditions})
            ORDER BY activation DESC, created_at DESC
            LIMIT $2
            """,
            tenant_id,
            remaining,
            *[f"%{term}%" for term in terms],
        )
        for idx, row in enumerate(rows):
            candidates.add(
                row["id"],
                max(0.08, 0.22 - idx * 0.003),
                "lexical",
                source="lexical",
            )

    async def _suppress_from_negative_memory(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        signature: dict[str, Any],
        candidates: "_CandidateAccumulator",
    ) -> None:
        if not signature or not candidates.model_ids:
            return
        try:
            memories = await NegativeMemoryRepo(
                self._pool, tenant_id=tenant_id,
            ).find_for_signature(signature, conn=conn)
        except Exception as exc:  # noqa: BLE001
            _log.debug("sage.reader.negative_memory_unavailable", error=str(exc))
            return
        candidate_ids = candidates.model_ids
        for memory in memories:
            for model_id in _negative_memory_model_ids(memory.rejected_path):
                if model_id in candidate_ids:
                    candidates.suppress(
                        model_id,
                        f"negative_memory:{memory.memory_type}",
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
        self.details: defaultdict[UUID, _CandidateScore] = defaultdict(
            _CandidateScore
        )
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

    def adjusted_score_for(self, model_id: UUID, score: float) -> float:
        if model_id not in self.suppressed:
            return score
        return min(float(score) * 0.18, 0.12)

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


async def _load_aliases_with_conn(
    conn: asyncpg.Connection, tenant_id: UUID,
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
    by_id = {}
    for row in rows:
        raw = dict(row)
        raw["content"] = _coerce_obj(raw.get("content"))
        raw["entities_mentioned"] = _coerce_list(
            raw.get("entities_mentioned")
        )
        by_id[row["id"]] = ObservationRow.model_validate(raw)
    return [by_id[oid] for oid in ids if oid in by_id]


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
        row["edge_id"]: EdgeStructuralFeatures.model_validate(dict(row))
        for row in rows
    }


async def _load_candidate_edges(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_model_ids: list[UUID],
    limit: int,
) -> list[asyncpg.Record]:
    if not seed_model_ids:
        return []
    return list(await conn.fetch(
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
    ))


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
            trigger.model_id, 0.72, "explicit:trigger_model", source="explicit",
        )
    for mid in trigger.member_model_ids or []:
        candidates.add(mid, 0.48, "explicit:member_model", source="explicit")


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
        ]
    )
    return sorted(_tokens(text), key=lambda t: (-len(t), t))[:12]


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
    query_tokens = _tokens(
        " ".join([question or "", trigger.seed_natural_text or ""])
    )
    model_tokens = _tokens(model_text)
    overlap = query_tokens & model_tokens
    score = 0.0
    reasons: list[str] = []
    if overlap:
        score += min(0.20, 0.045 * len(overlap))
        reasons.append("lexical:" + ",".join(sorted(overlap)[:4]))
    model_lower = model_text.casefold()
    for entity in list(cues.explicit_entities) + list(cues.aliases):
        entity_text = str(entity).strip()
        if entity_text and entity_text.casefold() in model_lower:
            score += 0.18
            reasons.append(f"exact:{entity_text[:40]}")
            break
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
    return min(score, 0.45), reasons


_STOPWORDS = {
    "about", "after", "also", "and", "are", "because", "been", "from",
    "have", "into", "need", "needs", "that", "the", "their", "this",
    "with", "without", "what", "which", "would", "should", "actually",
}


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.casefold())
        if token not in _STOPWORDS and not token.isdigit()
    }


def _required_roles_for(primitive: str) -> tuple[str, ...]:
    if primitive in {"DEPENDENCY", "CONSTRAINT"}:
        return ("role:blocker", "role:commitment", "role:counterevidence")
    if primitive in {"COUNTEREVIDENCE", "FALSIFICATION"}:
        return ("role:counterevidence", "role:falsifier")
    if primitive == "OWNERSHIP":
        return ("role:owner", "role:commitment")
    return ()


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
