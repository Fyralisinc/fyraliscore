"""Latent relationship topology for Models.

In this codebase, "topology" now means the upstream latent
relationship field: the cheap, high-recall layer that notices where
Models may share consequence before accepted typed edges exist.

The accepted-memory graph topology from earlier migrations is kept as
schema compatibility only; this module is the live topology path used
from Model insertion.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.reasoning.judgment.scoring import JudgmentScores, clamp_score
from services.reasoning.relationships.candidates import (
    TOPOLOGY_EMITTABLE_EDGE_KINDS,
    RelationshipCandidate,
    make_edge_candidate,
    make_edge_type_candidate,
    make_situation_candidate,
    rank_candidates,
)
from services.reasoning.relationships.repo import RelationshipCandidatesRepo


# Cheap deterministic ontology. These are not "truth labels"; they are
# routing facets that let the topology layer compare consequence rather
# than plain text.
_FLOW_TERMS: dict[str, tuple[str, ...]] = {
    "work": (
        "ship", "deliver", "delivery", "roadmap", "release", "launch",
        "implementation", "execution", "ticket", "milestone",
    ),
    "money": (
        "revenue", "arr", "renewal", "contract", "invoice", "billing",
        "pipeline", "deal", "churn", "expansion", "forecast",
    ),
    "trust": (
        "trust", "confidence", "champion", "relationship", "reputation",
        "customer sentiment", "escalation", "dissatisfied",
    ),
    "risk": (
        "risk", "security", "soc2", "legal", "compliance", "privacy",
        "incident", "outage", "fraud", "audit", "regulatory",
    ),
    "capacity": (
        "capacity", "bandwidth", "overloaded", "understaffed", "blocked",
        "queue", "backlog", "bottleneck", "constraint",
    ),
    "decision": (
        "decision", "approve", "approval", "sign off", "revisit",
        "option", "tradeoff", "prioritize", "priority",
    ),
    "attention": (
        "meeting", "escalate", "dashboard", "focus", "urgent",
        "attention", "review", "follow up",
    ),
}

_PRESSURE_TERMS: dict[str, tuple[str, ...]] = {
    "blocker": (
        "block", "blocked", "blocking", "waiting on", "stuck", "cannot",
        "dependency", "depends on", "prevent",
    ),
    "overload": (
        "overload", "overloaded", "capacity", "bandwidth", "burnout",
        "queue", "backlog", "understaffed",
    ),
    "deadline": (
        "deadline", "due", "by friday", "by monday", "end of week",
        "q1", "q2", "q3", "q4", "slip", "delayed", "late",
    ),
    "contradiction": (
        "contradict", "inconsistent", "but", "however", "cannot both",
        "conflict", "mismatch", "disagree",
    ),
    "dependency": (
        "depends", "dependency", "requires", "prerequisite", "needs",
        "waiting on",
    ),
    "decay": (
        "worse", "decline", "dropping", "losing", "decay", "stale",
        "churn", "degraded",
    ),
    "acceleration": (
        "accelerate", "increasing", "surge", "spike", "growing",
        "expanding", "faster",
    ),
    "opportunity": (
        "opportunity", "unlock", "enable", "upsell", "expansion",
        "improve", "leverage",
    ),
}

_STAKE_TERMS: dict[str, tuple[str, ...]] = {
    "enterprise_value": ("enterprise", "strategic", "tier 1", "key account"),
    "revenue": ("revenue", "arr", "renewal", "contract", "invoice", "billing"),
    "customer_trust": ("customer", "champion", "trust", "escalation", "renewal"),
    "legal_compliance": ("legal", "security", "soc2", "compliance", "audit"),
    "execution": ("ship", "launch", "release", "roadmap", "delivery"),
}

# Pressure → edge_kind mapping is restricted to topology-emittable kinds.
# Pressures that historically suggested an LLM-only kind (e.g. "overload"
# → "explains") are intentionally NOT in this table. Topology must not
# fabricate `explains` / `causes` / `predicts` / `weakens` etc. — those
# are LLM-only kinds; Think proposes them directly.
_PAIR_EDGE_KIND_BY_PRESSURE: dict[str, str] = {
    "blocker": "blocks",
    "dependency": "blocks",
    "contradiction": "contradicts",
    "opportunity": "enables",
    "decay": "early_warning_for",
    "deadline": "early_warning_for",
}


@dataclass(frozen=True)
class OntologyGapSpec:
    proposed_edge_kind: str
    description: str
    relationship_summary: str
    parent_kind: str | None
    nearest_existing_kind: str | None
    directionality: str
    dropped_dimensions: tuple[str, ...]


@dataclass(frozen=True)
class ImpactSignature:
    """Structured consequence fingerprint for one Model."""

    model_id: UUID
    flows: tuple[str, ...] = ()
    pressures: tuple[str, ...] = ()
    surfaces: tuple[str, ...] = ()
    stakes: tuple[str, ...] = ()
    time_shape: str = "unspecified"
    proposition_kind: str = "unknown"
    action_surface: str | None = None
    evidence_strength: float = 0.5

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_id": str(self.model_id),
            "flows": list(self.flows),
            "pressures": list(self.pressures),
            "surfaces": list(self.surfaces),
            "stakes": list(self.stakes),
            "time_shape": self.time_shape,
            "proposition_kind": self.proposition_kind,
            "action_surface": self.action_surface,
            "evidence_strength": self.evidence_strength,
        }


@dataclass(frozen=True)
class TopologyScore:
    latent_affinity: float = 0.0
    consequence_overlap: float = 0.0
    scope_fit: float = 0.0
    temporal_coupling: float = 0.0
    business_leverage: float = 0.0
    structural_surprise: float = 0.0
    evidence_quality: float = 0.0
    actionability: float = 0.0
    novelty: float = 0.0
    existing_explanation_gap: float = 0.0
    noise_penalty: float = 0.0

    @property
    def total(self) -> float:
        positive = (
            0.15 * self.latent_affinity
            + 0.20 * self.consequence_overlap
            + 0.12 * self.scope_fit
            + 0.08 * self.temporal_coupling
            + 0.14 * self.business_leverage
            + 0.10 * self.structural_surprise
            + 0.08 * self.evidence_quality
            + 0.08 * self.actionability
            + 0.03 * self.novelty
            + 0.02 * self.existing_explanation_gap
        )
        return clamp_score(positive - 0.15 * self.noise_penalty)

    def as_dict(self) -> dict[str, float]:
        return {
            "latent_affinity": clamp_score(self.latent_affinity),
            "consequence_overlap": clamp_score(self.consequence_overlap),
            "scope_fit": clamp_score(self.scope_fit),
            "temporal_coupling": clamp_score(self.temporal_coupling),
            "business_leverage": clamp_score(self.business_leverage),
            "structural_surprise": clamp_score(self.structural_surprise),
            "evidence_quality": clamp_score(self.evidence_quality),
            "actionability": clamp_score(self.actionability),
            "novelty": clamp_score(self.novelty),
            "existing_explanation_gap": clamp_score(self.existing_explanation_gap),
            "noise_penalty": clamp_score(self.noise_penalty),
            "total": self.total,
        }


@dataclass(frozen=True)
class _CandidateNeighbor:
    row: dict[str, Any]
    latent_affinity: float
    sources: tuple[str, ...]
    signature: ImpactSignature


@dataclass
class TopologyGenerationResult:
    inserted_candidates: list[dict[str, Any]] = field(default_factory=list)
    enqueued_think_triggers: int = 0
    skipped_reason: str | None = None
    neighbors_considered: int = 0
    pair_candidates_scored: int = 0
    situation_candidates_scored: int = 0
    candidates_ranked: int = 0
    duplicates_suppressed: int = 0
    think_triggers_skipped_low_score: int = 0


@dataclass
class TopologySweepReport:
    tenant_id: UUID
    models_seen: int = 0
    models_skipped: int = 0
    candidates_inserted: int = 0
    think_triggers_enqueued: int = 0
    neighbors_considered: int = 0
    candidates_ranked: int = 0
    duplicates_suppressed: int = 0
    think_triggers_skipped_low_score: int = 0
    errors: list[str] = field(default_factory=list)


class LatentTopologyService:
    """Generate topology candidates from one newly changed Model.

    This is intentionally sparse. It never materializes all pairwise
    scores. One changed Model searches bounded semantic/scope pools,
    scores consequence interactions, persists only top candidates, and
    optionally enqueues a small T4 Think pass for the highest-leverage
    candidate.
    """

    def __init__(
        self,
        *,
        relationship_repo: RelationshipCandidatesRepo | None = None,
        raw_candidate_limit: int = 160,
        candidate_insert_limit: int = 8,
        think_enqueue_limit: int = 1,
        min_insert_score: float = 0.46,
        min_think_score: float = 0.66,
    ) -> None:
        self._relationship_repo = relationship_repo or RelationshipCandidatesRepo()
        self.raw_candidate_limit = raw_candidate_limit
        self.candidate_insert_limit = candidate_insert_limit
        self.think_enqueue_limit = think_enqueue_limit
        self.min_insert_score = min_insert_score
        self.min_think_score = min_think_score

    async def generate_for_model(
        self,
        conn: asyncpg.Connection,
        *,
        model: ModelRow,
        enqueue_think: bool = True,
    ) -> TopologyGenerationResult:
        if model.status != "active":
            return TopologyGenerationResult(skipped_reason="model_not_active")
        if not model.embedding:
            return TopologyGenerationResult(skipped_reason="model_missing_embedding")

        seed_sig = impact_signature(model)
        neighbors = await self._collect_neighbors(
            conn,
            model=model,
            seed_sig=seed_sig,
        )
        if not neighbors:
            return TopologyGenerationResult(skipped_reason="no_candidate_neighbors")

        pair_candidates = await self._pair_candidates(
            conn,
            model=model,
            seed_sig=seed_sig,
            neighbors=neighbors,
        )
        situation_candidates = self._situation_candidates(
            model=model,
            seed_sig=seed_sig,
            neighbors=neighbors,
        )
        ranked_candidates = rank_candidates(
            [
                *pair_candidates,
                *situation_candidates,
            ],
            limit=self.candidate_insert_limit,
        )
        candidates = [
            c for c in ranked_candidates
            if c.judgment_leverage_score >= self.min_insert_score
        ]
        inserted: list[dict[str, Any]] = []
        duplicates_suppressed = 0
        for candidate in candidates:
            if await self._candidate_exists(conn, candidate):
                duplicates_suppressed += 1
                continue
            inserted.append(await self._relationship_repo.insert(conn, candidate))

        enqueued = 0
        skipped_low_score = 0
        if enqueue_think and inserted:
            for record in inserted[: self.think_enqueue_limit]:
                if float(record.get("judgment_leverage_score") or 0.0) < self.min_think_score:
                    skipped_low_score += 1
                    continue
                ok = await self._enqueue_think_for_candidate(conn, record)
                if ok:
                    enqueued += 1

        return TopologyGenerationResult(
            inserted_candidates=inserted,
            enqueued_think_triggers=enqueued,
            neighbors_considered=len(neighbors),
            pair_candidates_scored=len(pair_candidates),
            situation_candidates_scored=len(situation_candidates),
            candidates_ranked=len(ranked_candidates),
            duplicates_suppressed=duplicates_suppressed,
            think_triggers_skipped_low_score=skipped_low_score,
        )

    async def sweep_tenant(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        limit: int = 50,
        min_activation: float = 0.15,
        enqueue_think: bool = True,
    ) -> TopologySweepReport:
        """Run bounded candidate discovery over important active Models.

        This is the background counterpart to insert-time generation.
        It deliberately samples a small, high-activation frontier rather
        than doing all-pairs computation.
        """
        rows = await conn.fetch(
            """
            SELECT id, tenant_id, born_from_event_id, proposition,
                   "natural" AS natural, embedding, scope_actors,
                   scope_entities, scope_temporal, confidence,
                   activation, falsifier, signal_readings,
                   reading_contestable, supporting_event_ids,
                   supporting_model_ids, evidential_weight, status,
                   archived_at, archive_reason, created_at,
                   last_retrieved_at, retrieval_count, evaluate_at,
                   resolution_criteria, contributing_models,
                   visible_to_subjects, proposition_kind,
                   confirmed_count, contested_count, last_confirmed_at,
                   confidence_at_assertion, resolved_at,
                   resolution_outcome, activation_coefficient,
                   target_actor_id, caused_act_change_id
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND embedding IS NOT NULL
              AND activation >= $2
            ORDER BY activation DESC, confidence DESC, created_at DESC
            LIMIT $3
            """,
            tenant_id,
            float(min_activation),
            max(1, int(limit)),
        )
        report = TopologySweepReport(tenant_id=tenant_id, models_seen=len(rows))
        for row in rows:
            try:
                model = ModelRow.model_validate(_row_to_modelish_dict(row))
                result = await self.generate_for_model(
                    conn,
                    model=model,
                    enqueue_think=enqueue_think,
                )
                if result.skipped_reason:
                    report.models_skipped += 1
                report.candidates_inserted += len(result.inserted_candidates)
                report.think_triggers_enqueued += result.enqueued_think_triggers
                report.neighbors_considered += result.neighbors_considered
                report.candidates_ranked += result.candidates_ranked
                report.duplicates_suppressed += result.duplicates_suppressed
                report.think_triggers_skipped_low_score += (
                    result.think_triggers_skipped_low_score
                )
            except Exception as exc:  # noqa: BLE001
                report.errors.append(f"{row['id']}: {type(exc).__name__}: {exc}")
        return report

    async def _collect_neighbors(
        self,
        conn: asyncpg.Connection,
        *,
        model: ModelRow,
        seed_sig: ImpactSignature,
    ) -> list[_CandidateNeighbor]:
        rows_by_id: dict[UUID, _CandidateNeighbor] = {}

        # 1. Latent semantic recall. This is only recall; scoring below
        # decides whether the relationship is consequence-bearing.
        embedding_literal = _vector_literal(model.embedding)
        semantic_rows = await conn.fetch(
            f"""
            SELECT id, tenant_id, proposition, "natural" AS natural,
                   embedding, scope_actors, scope_entities, scope_temporal,
                   confidence, activation, status, proposition_kind,
                   created_at,
                   1.0 - (embedding <=> '{embedding_literal}'::vector) AS latent_affinity
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND id != $2
              AND embedding IS NOT NULL
            ORDER BY embedding <=> '{embedding_literal}'::vector
            LIMIT $3
            """,
            model.tenant_id,
            model.id,
            max(20, self.raw_candidate_limit // 2),
        )
        for row in semantic_rows:
            self._add_neighbor(rows_by_id, row, "latent", row["latent_affinity"])

        # 2. Surface recall: shared customer/commitment/actor/resource.
        scope_entities = _normalized_scope_entities(model.scope_entities)
        if scope_entities or model.scope_actors:
            surface_rows = await conn.fetch(
                """
                SELECT DISTINCT m.id, m.tenant_id, m.proposition,
                       m."natural" AS natural, m.embedding, m.scope_actors,
                       m.scope_entities, m.scope_temporal, m.confidence,
                       m.activation, m.status, m.proposition_kind,
                       m.created_at,
                       NULL::float AS latent_affinity
                FROM models m
                WHERE m.tenant_id = $1
                  AND m.status = 'active'
                  AND m.id != $2
                  AND (
                    ($3::uuid[] != '{}'::uuid[] AND m.scope_actors && $3::uuid[])
                    OR
                    ($4::jsonb != '[]'::jsonb AND m.scope_entities @> $4::jsonb)
                  )
                ORDER BY m.activation DESC, m.confidence DESC, m.created_at DESC
                LIMIT $5
                """,
                model.tenant_id,
                model.id,
                list(model.scope_actors or []),
                json.dumps(scope_entities, sort_keys=True, default=str),
                max(20, self.raw_candidate_limit // 2),
            )
            for row in surface_rows:
                self._add_neighbor(rows_by_id, row, "surface", row["latent_affinity"])

        # 3. Consequence recall: high-activation memory that shares
        # flows/pressures/stakes even when it has no explicit shared
        # scope and embeddings do not put it near the seed. This lane is
        # what makes topology more than semantic search while keeping
        # compute bounded.
        consequence_rows = await conn.fetch(
            """
            SELECT id, tenant_id, proposition, "natural" AS natural,
                   embedding, scope_actors, scope_entities, scope_temporal,
                   confidence, activation, status, proposition_kind,
                   created_at,
                   NULL::float AS latent_affinity
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND id != $2
              AND embedding IS NOT NULL
            ORDER BY activation DESC, confidence DESC, created_at DESC
            LIMIT $3
            """,
            model.tenant_id,
            model.id,
            max(40, self.raw_candidate_limit),
        )
        for row in consequence_rows:
            row_dict = _row_to_modelish_dict(row)
            candidate_sig = impact_signature_from_row(row_dict)
            affinity = _signature_recall_affinity(seed_sig, candidate_sig)
            if affinity >= 0.34:
                self._add_neighbor(rows_by_id, row, "consequence", affinity)

        # 4. Evidence recall: direct support/contribution references are
        # narrow and cheap. They often reveal hidden dependency chains
        # that neither text nor scope will recall reliably.
        evidence_ids = list(
            dict.fromkeys([
                *list(model.supporting_model_ids or []),
                *list(model.contributing_models or []),
            ])
        )
        evidence_rows = await conn.fetch(
            """
            SELECT id, tenant_id, proposition, "natural" AS natural,
                   embedding, scope_actors, scope_entities, scope_temporal,
                   confidence, activation, status, proposition_kind,
                   created_at,
                   NULL::float AS latent_affinity
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND id != $2
              AND embedding IS NOT NULL
              AND (
                id = ANY($3::uuid[])
                OR $2 = ANY(supporting_model_ids)
                OR $2 = ANY(contributing_models)
              )
            ORDER BY activation DESC, confidence DESC, created_at DESC
            LIMIT $4
            """,
            model.tenant_id,
            model.id,
            evidence_ids,
            max(10, self.raw_candidate_limit // 4),
        )
        for row in evidence_rows:
            self._add_neighbor(rows_by_id, row, "evidence", 0.72)

        return sorted(
            rows_by_id.values(),
            key=lambda n: (
                -_source_priority(n.sources),
                -float(n.latent_affinity),
                -float(n.row.get("activation") or 0.0),
                str(n.row["id"]),
            ),
        )[: self.raw_candidate_limit]

    def _add_neighbor(
        self,
        rows_by_id: dict[UUID, _CandidateNeighbor],
        row: asyncpg.Record,
        source: str,
        latent_affinity: Any,
    ) -> None:
        row_dict = _row_to_modelish_dict(row)
        signature = impact_signature_from_row(row_dict)
        affinity = clamp_score(float(latent_affinity or 0.0))
        existing = rows_by_id.get(row_dict["id"])
        if existing is None:
            rows_by_id[row_dict["id"]] = _CandidateNeighbor(
                row=row_dict,
                latent_affinity=affinity,
                sources=(source,),
                signature=signature,
            )
            return
        sources = tuple(dict.fromkeys([*existing.sources, source]))
        if affinity > existing.latent_affinity or sources != existing.sources:
            rows_by_id[row_dict["id"]] = _CandidateNeighbor(
                row=existing.row,
                latent_affinity=max(existing.latent_affinity, affinity),
                sources=sources,
                signature=existing.signature,
            )

    async def _pair_candidates(
        self,
        conn: asyncpg.Connection,
        *,
        model: ModelRow,
        seed_sig: ImpactSignature,
        neighbors: list[_CandidateNeighbor],
    ) -> list[RelationshipCandidate]:
        existing_edge_pairs = await _existing_edge_pairs(
            conn,
            tenant_id=model.tenant_id,
            seed_model_id=model.id,
            other_model_ids=[n.row["id"] for n in neighbors],
        )
        candidates: list[RelationshipCandidate] = []
        for neighbor in neighbors:
            other_sig = neighbor.signature
            score = _score_interaction(
                left=model,
                left_sig=seed_sig,
                right=neighbor.row,
                right_sig=other_sig,
                latent_affinity=neighbor.latent_affinity,
                existing_edge=neighbor.row["id"] in existing_edge_pairs,
            )
            if score.total < self.min_insert_score:
                continue
            ontology_gap = _ontology_gap_for_interaction(
                left=model,
                right=neighbor.row,
                left_sig=seed_sig,
                right_sig=other_sig,
                score=score,
            )
            if ontology_gap is not None:
                metadata = {
                    "topology": {
                        "kind": "latent_relationship_field",
                        "object_type": "edge_type_candidate",
                        "selection_sources": list(neighbor.sources),
                        "score_components": score.as_dict(),
                        "impact_signatures": [
                            seed_sig.as_dict(),
                            other_sig.as_dict(),
                        ],
                    }
                }
                candidate = make_edge_type_candidate(
                    tenant_id=model.tenant_id,
                    proposed_edge_kind=ontology_gap.proposed_edge_kind,
                    description=ontology_gap.description,
                    relationship_summary=ontology_gap.relationship_summary,
                    parent_kind=ontology_gap.parent_kind,
                    nearest_existing_kind=ontology_gap.nearest_existing_kind,
                    directionality=ontology_gap.directionality,  # type: ignore[arg-type]
                    dropped_dimensions=ontology_gap.dropped_dimensions,
                    example_source_model_id=model.id,
                    example_target_model_id=neighbor.row["id"],
                    scores=_judgment_from_topology(score),
                    source="latent_topology",
                    metadata=metadata,
                )
                candidates.append(candidate)
                continue
            edge_kind = _edge_kind_for_interaction(seed_sig, other_sig)
            if edge_kind not in TOPOLOGY_EMITTABLE_EDGE_KINDS:
                # Topology never fabricates LLM-only kinds.
                continue
            source_model_id, target_model_id = _orient_for_edge_kind(
                model.id,
                neighbor.row["id"],
                edge_kind,
                seed_sig,
                other_sig,
            )
            explanation = _pair_explanation(model, neighbor.row, score, edge_kind)
            metadata: dict[str, Any] = {
                "topology": {
                    "kind": "latent_relationship_field",
                    "object_type": "pair_candidate",
                    "selection_sources": list(neighbor.sources),
                    "score_components": score.as_dict(),
                    "impact_signatures": [
                        seed_sig.as_dict(),
                        other_sig.as_dict(),
                    ],
                }
            }
            kwargs: dict[str, Any] = {}
            justification = _edge_kind_justification(
                edge_kind, seed_sig, other_sig
            )
            if justification:
                metadata.update(justification.get("metadata", {}))
                kwargs.update(justification.get("kwargs", {}))
            candidate = make_edge_candidate(
                tenant_id=model.tenant_id,
                source_model_id=source_model_id,
                target_model_id=target_model_id,
                edge_kind=edge_kind,
                basis=justification.get("basis", "topology_suggested") if justification else "topology_suggested",
                explanation=explanation,
                scores=_judgment_from_topology(score),
                evidence_model_ids=(model.id, neighbor.row["id"]),
                source="latent_topology",
                metadata=metadata,
                **kwargs,
            )
            candidates.append(candidate)
        return rank_candidates(candidates, limit=self.candidate_insert_limit)

    def _situation_candidates(
        self,
        *,
        model: ModelRow,
        seed_sig: ImpactSignature,
        neighbors: list[_CandidateNeighbor],
    ) -> list[RelationshipCandidate]:
        grouped: dict[tuple[str, str], list[_CandidateNeighbor]] = {}
        for neighbor in neighbors:
            shared_flows = _intersect(seed_sig.flows, neighbor.signature.flows)
            shared_pressures = _intersect(seed_sig.pressures, neighbor.signature.pressures)
            if not shared_flows and not shared_pressures:
                continue
            flow = shared_flows[0] if shared_flows else "mixed_flow"
            pressure = shared_pressures[0] if shared_pressures else "mixed_pressure"
            grouped.setdefault((flow, pressure), []).append(neighbor)

        out: list[RelationshipCandidate] = []
        for (flow, pressure), group in grouped.items():
            group = sorted(
                group,
                key=lambda n: (
                    -n.latent_affinity,
                    -float(n.row.get("activation") or 0.0),
                    str(n.row["id"]),
                ),
            )[:5]
            if len(group) < 2:
                continue
            members = tuple(dict.fromkeys([model.id, *[n.row["id"] for n in group]]))
            if len(members) < 3:
                continue
            score = _situation_score(model, seed_sig, group)
            if score.total < self.min_insert_score:
                continue
            situation = f"{flow} {pressure}".replace("_", " ")
            summary = (
                f"Multiple Models appear to act on the same {flow} flow "
                f"through a {pressure} pressure."
            )
            relationship_summary = (
                "Topology surfaced this as a possible composite situation "
                "because member Models share consequence signatures, not just "
                "text similarity."
            )
            out.append(
                make_situation_candidate(
                    tenant_id=model.tenant_id,
                    situation=situation,
                    summary=summary,
                    relationship_summary=relationship_summary,
                    member_model_ids=members,
                    basis="topology_suggested",
                    scores=_judgment_from_topology(score),
                    source="latent_topology",
                    metadata={
                        "topology": {
                            "kind": "latent_relationship_field",
                            "object_type": "situation_candidate",
                            "score_components": score.as_dict(),
                            "impact_signatures": [
                                seed_sig.as_dict(),
                                *[n.signature.as_dict() for n in group],
                            ],
                        }
                    },
                )
            )
        return rank_candidates(out, limit=3)

    async def _candidate_exists(
        self,
        conn: asyncpg.Connection,
        candidate: RelationshipCandidate,
    ) -> bool:
        if candidate.candidate_kind == "edge":
            row = await conn.fetchval(
                """
                SELECT 1
                FROM relationship_candidates
                WHERE tenant_id = $1
                  AND candidate_kind = 'edge'
                  AND source_model_id = $2
                  AND target_model_id = $3
                  AND edge_kind = $4
                  AND review_status IN ('candidate', 'needs_review', 'accepted')
                  AND created_at > now() - interval '14 days'
                LIMIT 1
                """,
                candidate.tenant_id,
                candidate.source_model_id,
                candidate.target_model_id,
                candidate.edge_kind,
            )
            return row is not None
        if candidate.candidate_kind == "edge_type":
            proposed = (candidate.proposed_proposition or {}).get(
                "proposed_edge_kind"
            )
            row = await conn.fetchval(
                """
                SELECT 1
                FROM relationship_candidates
                WHERE tenant_id = $1
                  AND candidate_kind = 'edge_type'
                  AND proposed_proposition->>'proposed_edge_kind' = $2
                  AND member_model_ids @> $3::uuid[]
                  AND $3::uuid[] @> member_model_ids
                  AND review_status IN ('candidate', 'needs_review', 'accepted')
                  AND created_at > now() - interval '14 days'
                LIMIT 1
                """,
                candidate.tenant_id,
                proposed,
                list(candidate.member_model_ids),
            )
            return row is not None
        row = await conn.fetchval(
            """
            SELECT 1
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND candidate_kind = 'situation'
              AND member_model_ids @> $2::uuid[]
              AND $2::uuid[] @> member_model_ids
              AND review_status IN ('candidate', 'needs_review', 'accepted')
              AND created_at > now() - interval '14 days'
            LIMIT 1
            """,
            candidate.tenant_id,
            list(candidate.member_model_ids),
        )
        return row is not None

    async def _enqueue_think_for_candidate(
        self,
        conn: asyncpg.Connection,
        record: dict[str, Any],
        parent_payload: dict[str, Any] | None = None,
    ) -> bool:
        candidate_id = record["id"]
        existing = await conn.fetchval(
            """
            SELECT 1
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND trigger_kind = 'T4'
              AND trigger_subkind = 'latent_relationship_candidate'
              AND payload->>'relationship_candidate_id' = $2
              AND completed_at IS NULL
            LIMIT 1
            """,
            record["tenant_id"],
            str(candidate_id),
        )
        if existing is not None:
            return False
        member_ids = [str(v) for v in (record.get("member_model_ids") or [])]
        payload = {
            "relationship_candidate_id": str(candidate_id),
            "seed_natural_text": str(record.get("explanation") or "")[:2000],
            "member_model_ids": member_ids,
            "seed_signature": {
                "kind": "latent_relationship_candidate",
                "candidate_kind": record.get("candidate_kind"),
                "basis": record.get("basis"),
                "score": record.get("judgment_leverage_score"),
                "metadata": record.get("metadata") or {},
            },
        }
        # Cost-plan §3.2: stamp lineage depth (parent + 1; a topology sweep with
        # no parent trigger is a lineage root at depth 1) so the worker's
        # cascade-bound check applies to insert-time T4 candidates too.
        from services.reasoning.think.cascade import propagate_cascade_depth
        payload.update(propagate_cascade_depth(parent_payload))
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
              id, tenant_id, trigger_kind, trigger_subkind, payload
            )
            VALUES ($1, $2, 'T4',
                    'latent_relationship_candidate', $3::jsonb)
            """,
            uuid7(),
            record["tenant_id"],
            json.dumps(payload, sort_keys=True, default=str),
        )
        return True


def impact_signature(model: ModelRow) -> ImpactSignature:
    return impact_signature_from_row(
        {
            "id": model.id,
            "natural": model.natural,
            "proposition": model.proposition,
            "proposition_kind": model.proposition_kind,
            "scope_entities": model.scope_entities,
            "scope_actors": model.scope_actors,
            "scope_temporal": model.scope_temporal,
            "confidence": model.confidence,
            "activation": model.activation,
        }
    )


def impact_signature_from_row(row: dict[str, Any]) -> ImpactSignature:
    text = _text_for_signature(row)
    proposition_kind = str(row.get("proposition_kind") or "unknown")
    flows = _matched_labels(text, _FLOW_TERMS)
    pressures = _matched_labels(text, _PRESSURE_TERMS)
    stakes = _matched_labels(text, _STAKE_TERMS)
    surfaces = _surfaces(row.get("scope_entities") or [], row.get("scope_actors") or [])
    if not flows:
        flows = _default_flows_for_kind(proposition_kind)
    if not pressures:
        pressures = _default_pressures_for_kind(proposition_kind)
    return ImpactSignature(
        model_id=row["id"],
        flows=flows,
        pressures=pressures,
        surfaces=surfaces,
        stakes=stakes,
        time_shape=_time_shape(text, row.get("scope_temporal") or {}),
        proposition_kind=proposition_kind,
        action_surface=_action_surface(row.get("proposition") or {}),
        evidence_strength=clamp_score(
            0.65 * float(row.get("confidence") or 0.5)
            + 0.35 * float(row.get("activation") or 0.0)
        ),
    )


def _score_interaction(
    *,
    left: ModelRow,
    left_sig: ImpactSignature,
    right: dict[str, Any],
    right_sig: ImpactSignature,
    latent_affinity: float,
    existing_edge: bool,
) -> TopologyScore:
    flow_overlap = _overlap_score(left_sig.flows, right_sig.flows)
    pressure_overlap = _overlap_score(left_sig.pressures, right_sig.pressures)
    stake_overlap = _overlap_score(left_sig.stakes, right_sig.stakes)
    consequence = clamp_score(0.45 * flow_overlap + 0.40 * pressure_overlap + 0.15 * stake_overlap)
    scope_fit = _overlap_score(left_sig.surfaces, right_sig.surfaces)
    temporal = _temporal_score(left.created_at, right.get("created_at"))
    leverage = _business_leverage(left_sig, right_sig, left.activation, right.get("activation"))
    surprise = clamp_score(latent_affinity * (1.0 - min(scope_fit, 0.8)))
    evidence = clamp_score((left_sig.evidence_strength + right_sig.evidence_strength) / 2.0)
    actionability = _actionability(left_sig, right_sig)
    novelty = 0.15 if existing_edge else 0.80
    explanation_gap = 0.10 if existing_edge else 0.75
    noise = 0.30 if consequence < 0.25 and scope_fit < 0.25 else 0.0
    return TopologyScore(
        latent_affinity=latent_affinity,
        consequence_overlap=consequence,
        scope_fit=scope_fit,
        temporal_coupling=temporal,
        business_leverage=leverage,
        structural_surprise=surprise,
        evidence_quality=evidence,
        actionability=actionability,
        novelty=novelty,
        existing_explanation_gap=explanation_gap,
        noise_penalty=noise,
    )


def _situation_score(
    seed: ModelRow,
    seed_sig: ImpactSignature,
    group: list[_CandidateNeighbor],
) -> TopologyScore:
    affinities = [n.latent_affinity for n in group]
    consequences = [
        clamp_score(
            0.55 * _overlap_score(seed_sig.flows, n.signature.flows)
            + 0.45 * _overlap_score(seed_sig.pressures, n.signature.pressures)
        )
        for n in group
    ]
    scope_scores = [_overlap_score(seed_sig.surfaces, n.signature.surfaces) for n in group]
    temporal_scores = [_temporal_score(seed.created_at, n.row.get("created_at")) for n in group]
    leverage_scores = [
        _business_leverage(seed_sig, n.signature, seed.activation, n.row.get("activation"))
        for n in group
    ]
    return TopologyScore(
        latent_affinity=_avg(affinities),
        consequence_overlap=_avg(consequences),
        scope_fit=_avg(scope_scores),
        temporal_coupling=_avg(temporal_scores),
        business_leverage=max(leverage_scores or [0.0]),
        structural_surprise=clamp_score(_avg(affinities) * (1.0 - _avg(scope_scores))),
        evidence_quality=_avg([seed_sig.evidence_strength, *[n.signature.evidence_strength for n in group]]),
        actionability=_avg([_actionability(seed_sig, n.signature) for n in group]),
        novelty=0.75,
        existing_explanation_gap=0.70,
    )


def _judgment_from_topology(score: TopologyScore) -> JudgmentScores:
    return JudgmentScores(
        novelty=score.novelty,
        impact=score.business_leverage,
        actionability=score.actionability,
        urgency=clamp_score(0.60 * score.temporal_coupling + 0.40 * score.business_leverage),
        uncertainty=clamp_score(1.0 - score.evidence_quality),
        authority_required=clamp_score(0.35 + 0.45 * score.business_leverage),
        reversibility=0.45,
        confidence=clamp_score(
            0.45 * score.consequence_overlap
            + 0.35 * score.latent_affinity
            + 0.20 * score.evidence_quality
        ),
    )


def _ontology_gap_for_interaction(
    *,
    left: ModelRow,
    right: dict[str, Any],
    left_sig: ImpactSignature,
    right_sig: ImpactSignature,
    score: TopologyScore,
) -> OntologyGapSpec | None:
    """Return a richer proposed edge type when a generic edge would lose value."""
    if score.total < 0.40:
        return None
    text = " ".join([
        str(left.natural or ""),
        str(right.get("natural") or ""),
        json.dumps(left.proposition or {}, sort_keys=True, default=str),
        json.dumps(right.get("proposition") or {}, sort_keys=True, default=str),
    ]).lower()
    flows = set(left_sig.flows + right_sig.flows)
    pressures = set(left_sig.pressures + right_sig.pressures)

    def spec(
        proposed: str,
        description: str,
        summary: str,
        *,
        parent: str | None,
        directionality: str = "directed",
        dropped: tuple[str, ...],
    ) -> OntologyGapSpec:
        return OntologyGapSpec(
            proposed_edge_kind=proposed,
            description=description,
            relationship_summary=summary,
            parent_kind=parent,
            nearest_existing_kind=parent,
            directionality=directionality,
            dropped_dimensions=dropped,
        )

    if _has_any(text, ("obscure", "obscures", "hides", "hidden", "masked", "shadow")) and _has_any(
        text, ("attention", "focus", "urgent", "loud", "noisy", "dashboard")
    ):
        return spec(
            "obscures",
            "One salient Model makes a quieter high-value Model less likely to be noticed.",
            "Topology found an attention-shadow relation; a generic edge would lose salience distortion semantics.",
            parent=None,
            dropped=("attention competition", "salience distortion", "visibility risk"),
        )

    if (
        ("decision" in flows or _has_any(text, ("approval", "approve", "sign off", "decision", "decide")))
        and ({"blocker", "dependency"} & pressures or _has_any(text, ("waiting on", "cannot", "blocked", "requires")))
    ):
        return spec(
            "gated_by_decision",
            "A Model cannot progress until a specific decision or approval is made.",
            "Topology found a decision gate; `blocks` would lose authority and approval-state semantics.",
            parent="blocks",
            dropped=("authority surface", "decision dependency", "approval state"),
        )

    if _has_any(text, ("assumption", "premise", "if ", "unless", "only holds", "conditional")) and _has_any(
        text, ("forecast", "plan", "depends", "requires", "expected")
    ):
        return spec(
            "depends_on_assumption",
            "A forecast or plan only holds if another uncertain premise holds.",
            "Topology found assumption dependency; `supports` would hide conditional truth and fragility.",
            parent="supports",
            dropped=("assumption status", "conditional truth", "fragility"),
        )

    if _has_any(text, ("tradeoff", "trade-off", "frontier", "worsens", "at the cost", "cost of")) or (
        "decision" in flows and "contradiction" in pressures and _has_any(text, ("both", "valid", "true"))
    ):
        return spec(
            "trades_off_with",
            "Improving one Model predictably worsens another without making either false.",
            "Topology found a tradeoff; `contradicts` would incorrectly imply mutual falsity.",
            parent="contradicts",
            directionality="symmetric",
            dropped=("both can be true", "optimization frontier", "choice cost"),
        )

    if _has_any(text, ("priority", "prioritize", "compete", "capacity", "bandwidth", "focus")) and _has_any(
        text, ("queue", "backlog", "attention", "limited", "same team", "same owner")
    ):
        return spec(
            "competes_for_priority_with",
            "Two valid Models compete for the same limited attention or capacity.",
            "Topology found priority contention; `alternative_to` would lose shared-resource pressure.",
            parent="alternative_to",
            directionality="symmetric",
            dropped=("resource contention", "capacity limit", "both valid"),
        )

    if _has_any(text, ("mitigation", "mitigate", "reduces", "offset", "dampen", "dampens", "relieve")):
        return spec(
            "dampens",
            "One intervention reduces another pressure without falsifying it.",
            "Topology found a mitigation relation; `weakens` would confuse residual truth with counterevidence.",
            parent="weakens",
            dropped=("operational mitigation", "residual truth", "intervention surface"),
        )

    if _has_any(text, ("transfer", "transfers", "shift", "moves", "pushes")) and "risk" in flows:
        return spec(
            "transfers_risk_to",
            "Resolving one risk moves exposure to another owner or scope.",
            "Topology found risk transfer; `causes` would lose recipient-scope and second-order cost semantics.",
            parent="causes",
            dropped=("risk movement", "recipient scope", "second-order cost"),
        )

    if _has_any(text, ("proxy", "proxy for", "indicator", "weak signal", "indirect evidence", "measurement")):
        return spec(
            "proxy_for",
            "One weak signal is a proxy for a harder-to-observe state.",
            "Topology found proxy evidence; `early_warning_for` would overstate temporal lead-time.",
            parent="early_warning_for",
            dropped=("proxy validity", "measurement gap", "not necessarily future"),
        )

    if _has_any(text, ("lag", "lags", "lagging", "after", "delay", "delayed response", "leading")) and _has_any(
        text, ("metric", "indicator", "responds", "signal", "state")
    ):
        return spec(
            "lags",
            "One metric or state responds after another with a predictable delay.",
            "Topology found lag structure; `predicts` would lose temporal-offset semantics.",
            parent="predicts",
            dropped=("delay shape", "lagging indicator", "temporal offset"),
        )

    if _has_any(text, ("leverage", "amplify", "amplifies", "multiplier", "sequence")) and (
        "opportunity" in pressures or "opportunity" in text or "unlock" in text
    ):
        return spec(
            "amplifies_leverage_of",
            "One Model makes an intervention on another much more valuable.",
            "Topology found leverage amplification; `enables` would lose marginal-value and sequencing semantics.",
            parent="enables",
            dropped=("marginal value", "sequence leverage", "intervention ordering"),
        )

    if _has_any(text, ("precedent", "policy", "similar future", "future cases", "reuse")):
        return spec(
            "sets_precedent_for",
            "A specific case should shape future treatment of similar cases.",
            "Topology found precedent; `analogous_to` would lose normative reuse and scope-of-policy semantics.",
            parent="analogous_to",
            dropped=("normative precedent", "future policy", "scope of reuse"),
        )

    if _has_any(text, ("portfolio", "roll up", "rollup", "contains", "broader", "narrower", "account-level", "local issue")):
        return spec(
            "contains_scope",
            "A broader situation contains a narrower local issue.",
            "Topology found scope containment; `same_issue_as` would lose hierarchy and roll-up semantics.",
            parent="same_issue_as",
            dropped=("hierarchy", "containment", "roll-up semantics"),
        )

    if (
        {"overload", "decay", "acceleration"} & pressures
        and len(flows & {"money", "trust", "risk", "capacity"}) >= 2
    ):
        return spec(
            "reinforces",
            "Two pressures amplify each other as a compounding loop.",
            "Topology found mutual reinforcement; `causes` would lose loop and amplification semantics.",
            parent="causes",
            directionality="symmetric",
            dropped=("loop directionality", "mutual amplification", "runaway dynamic"),
        )

    if _has_any(text, ("owner", "accountable", "responsible", "no clear owner", "ownership", "escalation")) and (
        {"blocker", "dependency", "overload"} & pressures
    ):
        return spec(
            "accountable_for",
            "A Model says an actor or team is accountable for resolving another Model.",
            "Topology found accountability structure; `supports` would lose owner and escalation semantics.",
            parent="supports",
            dropped=("actor accountability", "ownership status", "escalation route"),
        )

    return None


def _has_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


def _edge_kind_for_interaction(
    left: ImpactSignature,
    right: ImpactSignature,
) -> str:
    for pressure in [*left.pressures, *right.pressures]:
        kind = _PAIR_EDGE_KIND_BY_PRESSURE.get(pressure)
        if kind:
            return kind
    if _intersect(left.flows, right.flows):
        return "same_issue_as"
    # `co_occurs_with` is an LLM-only kind; do not fabricate here.
    return "same_issue_as"


def _edge_kind_justification(
    edge_kind: str,
    seed_sig: ImpactSignature,
    other_sig: ImpactSignature,
) -> dict[str, Any] | None:
    """Build per-kind justification metadata + make_edge_candidate kwargs.

    Topology-level justification is structural (signature-based), not
    LLM-level reasoning. Adjudication still requires these fields to
    accept the candidate as a real edge.
    """
    if edge_kind == "blocks":
        pressures = sorted(set(seed_sig.pressures + other_sig.pressures))
        basis_reason = (
            "blocker" if "blocker" in pressures
            else "dependency" if "dependency" in pressures
            else "topology_pressure_overlap"
        )
        mechanism = (
            "Topology saw a blocker/dependency pressure surface in at least "
            "one Model; downstream Think must confirm a concrete dependency."
        )
        return {
            "basis": "causal_hypothesis",
            "metadata": {
                "mechanism": mechanism,
                "dependency_basis": basis_reason,
            },
            "kwargs": {
                "mechanism_summary": mechanism,
                "intervention_surface": (
                    "remove blocker, clarify owner, or unblock dependency"
                ),
                "expected_delay": "unknown",
            },
        }
    if edge_kind == "early_warning_for":
        evidence_kind = (
            "deadline_pressure_overlap"
            if "deadline" in set(seed_sig.pressures + other_sig.pressures)
            else "decay_pressure_overlap"
        )
        return {
            "basis": "topology_suggested",
            "metadata": {
                "lead_time_evidence": {
                    "kind": evidence_kind,
                    "seed_time_shape": seed_sig.time_shape,
                    "other_time_shape": other_sig.time_shape,
                },
                "historical_basis": evidence_kind,
            },
            "kwargs": {},
        }
    if edge_kind == "enables":
        return {
            "basis": "causal_hypothesis",
            "metadata": {
                "mechanism": (
                    "Topology saw an opportunity/capability pressure in at "
                    "least one Model; downstream Think must verify the target "
                    "is a capability assessment that this source enables."
                ),
            },
            "kwargs": {
                "mechanism_summary": (
                    "Source describes a capability or opportunity that may "
                    "make the target outcome more likely."
                ),
                "intervention_surface": (
                    "preserve prerequisite, allocate support, or reinforce capability"
                ),
                "expected_delay": "unknown",
            },
        }
    return None


def _orient_for_edge_kind(
    left_id: UUID,
    right_id: UUID,
    edge_kind: str,
    left_sig: ImpactSignature,
    right_sig: ImpactSignature,
) -> tuple[UUID, UUID]:
    if edge_kind in {"contradicts", "same_issue_as", "co_occurs_with", "analogous_to"}:
        return (left_id, right_id) if str(left_id) < str(right_id) else (right_id, left_id)
    left_pressure = _pressure_priority(left_sig)
    right_pressure = _pressure_priority(right_sig)
    if right_pressure > left_pressure:
        return right_id, left_id
    return left_id, right_id


def _pair_explanation(
    left: ModelRow,
    right: dict[str, Any],
    score: TopologyScore,
    edge_kind: str,
) -> str:
    return (
        f"Latent topology suggests these Models may be connected as "
        f"`{edge_kind}` because their impact signatures share consequence "
        f"(score={score.total:.2f}) even before an accepted edge exists. "
        f"Seed: {left.natural[:180]} Other: {str(right.get('natural') or '')[:180]}"
    )


async def _existing_edge_pairs(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_model_id: UUID,
    other_model_ids: list[UUID],
) -> set[UUID]:
    if not other_model_ids:
        return set()
    rows = await conn.fetch(
        """
        SELECT CASE
                 WHEN source_model_id = $2 THEN target_model_id
                 ELSE source_model_id
               END AS other_id
        FROM model_edges
        WHERE tenant_id = $1
          AND status = 'active'
          AND (source_model_id = $2 OR target_model_id = $2)
          AND (
            source_model_id = ANY($3::uuid[])
            OR target_model_id = ANY($3::uuid[])
          )
        """,
        tenant_id,
        seed_model_id,
        other_model_ids,
    )
    return {r["other_id"] for r in rows}


def _row_to_modelish_dict(row: asyncpg.Record) -> dict[str, Any]:
    embedding = row["embedding"]
    if embedding is None:
        embedding_list: list[float] = []
    else:
        embedding_list = [float(v) for v in embedding]
    return {
        "id": row["id"],
        "tenant_id": row["tenant_id"],
        "born_from_event_id": _record_value(row, "born_from_event_id"),
        "proposition": _decode_json(row["proposition"]),
        "natural": row["natural"],
        "embedding": embedding_list,
        "scope_actors": list(row["scope_actors"] or []),
        "scope_entities": _decode_json(row["scope_entities"]) or [],
        "scope_temporal": _decode_json(row["scope_temporal"]) or {},
        "confidence": float(row["confidence"] or 0.5),
        "activation": float(row["activation"] or 0.0),
        "falsifier": _decode_json(_record_value(row, "falsifier")),
        "signal_readings": _decode_json(_record_value(row, "signal_readings", [])) or [],
        "reading_contestable": bool(
            _record_value(row, "reading_contestable", True)
        ),
        "supporting_event_ids": list(
            _record_value(row, "supporting_event_ids", []) or []
        ),
        "supporting_model_ids": list(
            _record_value(row, "supporting_model_ids", []) or []
        ),
        "evidential_weight": float(_record_value(row, "evidential_weight", 0.5)),
        "status": row["status"],
        "archived_at": _record_value(row, "archived_at"),
        "archive_reason": _record_value(row, "archive_reason"),
        "proposition_kind": row["proposition_kind"],
        "created_at": row["created_at"],
        "last_retrieved_at": _record_value(row, "last_retrieved_at"),
        "retrieval_count": int(_record_value(row, "retrieval_count", 0) or 0),
        "evaluate_at": _record_value(row, "evaluate_at"),
        "resolution_criteria": _decode_json(
            _record_value(row, "resolution_criteria")
        ),
        "contributing_models": list(
            _record_value(row, "contributing_models", []) or []
        ),
        "visible_to_subjects": bool(_record_value(row, "visible_to_subjects", True)),
        "confirmed_count": int(_record_value(row, "confirmed_count", 0) or 0),
        "contested_count": int(_record_value(row, "contested_count", 0) or 0),
        "last_confirmed_at": _record_value(row, "last_confirmed_at"),
        "confidence_at_assertion": float(
            _record_value(row, "confidence_at_assertion", row["confidence"] or 0.5)
        ),
        "resolved_at": _record_value(row, "resolved_at"),
        "resolution_outcome": _record_value(row, "resolution_outcome"),
        "activation_coefficient": float(
            _record_value(row, "activation_coefficient", 1.0) or 1.0
        ),
        "target_actor_id": _record_value(row, "target_actor_id"),
        "caused_act_change_id": _record_value(row, "caused_act_change_id"),
    }


def _record_value(row: asyncpg.Record, key: str, default: Any = None) -> Any:
    try:
        if key in row.keys():
            return row[key]
    except (AttributeError, TypeError):
        pass
    return default


def _decode_json(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        return json.loads(value.decode())
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _text_for_signature(row: dict[str, Any]) -> str:
    proposition = row.get("proposition") or {}
    return " ".join(
        str(v)
        for v in [
            row.get("natural") or "",
            json.dumps(proposition, sort_keys=True, default=str),
        ]
    ).lower()


def _matched_labels(text: str, lookup: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    out = [
        label for label, terms in lookup.items()
        if any(term in text for term in terms)
    ]
    return tuple(out[:4])


def _default_flows_for_kind(kind: str) -> tuple[str, ...]:
    if kind in {"concern", "prediction"}:
        return ("risk",)
    if kind == "recommendation":
        return ("work", "decision")
    if kind in {"capability_assessment", "pattern"}:
        return ("capacity",)
    return ("work",)


def _default_pressures_for_kind(kind: str) -> tuple[str, ...]:
    if kind == "concern":
        return ("blocker",)
    if kind == "prediction":
        return ("deadline",)
    if kind == "hypothesis":
        return ("dependency",)
    if kind == "recommendation":
        return ("opportunity",)
    return ()


def _surfaces(
    scope_entities: Iterable[dict[str, Any]],
    scope_actors: Iterable[UUID],
) -> tuple[str, ...]:
    out: list[str] = []
    for entity in scope_entities:
        if not isinstance(entity, dict):
            continue
        typ = entity.get("type")
        eid = entity.get("id")
        if typ and eid:
            out.append(f"{typ}:{eid}")
    for actor in scope_actors:
        out.append(f"actor:{actor}")
    return tuple(dict.fromkeys(out))


def _normalized_scope_entities(scope_entities: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for entity in scope_entities:
        if not isinstance(entity, dict):
            continue
        typ = entity.get("type")
        eid = entity.get("id")
        if typ and eid:
            out.append({"type": str(typ), "id": str(eid)})
    return out


def _time_shape(text: str, scope_temporal: dict[str, Any]) -> str:
    if any(term in text for term in _PRESSURE_TERMS["deadline"]):
        return "deadline_bound"
    if "recurring" in text or "again" in text or "repeated" in text:
        return "recurring"
    if scope_temporal.get("valid_until"):
        return "bounded"
    return "unspecified"


def _action_surface(proposition: dict[str, Any]) -> str | None:
    if not isinstance(proposition, dict):
        return None
    if proposition.get("kind") == "recommendation":
        change = proposition.get("proposed_change") or {}
        if isinstance(change, dict):
            return str(change.get("operation") or "recommendation")
    if proposition.get("kind") == "concern":
        return "resolve_concern"
    if proposition.get("kind") == "prediction":
        return "monitor_resolution"
    return None


def _intersect(left: Iterable[str], right: Iterable[str]) -> tuple[str, ...]:
    right_set = set(right)
    return tuple(v for v in left if v in right_set)


def _overlap_score(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = set(left)
    right_set = set(right)
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / len(left_set | right_set)


def _signature_recall_affinity(
    left: ImpactSignature,
    right: ImpactSignature,
) -> float:
    flow = _overlap_score(left.flows, right.flows)
    pressure = _overlap_score(left.pressures, right.pressures)
    stake = _overlap_score(left.stakes, right.stakes)
    surface = _overlap_score(left.surfaces, right.surfaces)
    action = 1.0 if left.action_surface and left.action_surface == right.action_surface else 0.0
    time = 1.0 if left.time_shape == right.time_shape and left.time_shape != "unspecified" else 0.0
    kind = 0.5 if left.proposition_kind == right.proposition_kind else 0.0
    return clamp_score(
        0.30 * flow
        + 0.30 * pressure
        + 0.14 * stake
        + 0.12 * surface
        + 0.06 * action
        + 0.05 * time
        + 0.03 * kind
    )


def _source_priority(sources: Iterable[str]) -> float:
    weights = {
        "evidence": 1.0,
        "surface": 0.85,
        "consequence": 0.75,
        "latent": 0.60,
    }
    return max((weights.get(source, 0.0) for source in sources), default=0.0)


def _temporal_score(left: datetime | None, right: datetime | None) -> float:
    if left is None or right is None:
        return 0.25
    if left.tzinfo is None:
        left = left.replace(tzinfo=timezone.utc)
    if right.tzinfo is None:
        right = right.replace(tzinfo=timezone.utc)
    hours = abs((left - right).total_seconds()) / 3600.0
    if hours <= 24:
        return 1.0
    if hours <= 72:
        return 0.75
    if hours <= 168:
        return 0.45
    return 0.15


def _business_leverage(
    left: ImpactSignature,
    right: ImpactSignature,
    left_activation: float,
    right_activation: Any,
) -> float:
    stake_score = _overlap_score(left.stakes or ("execution",), right.stakes or ("execution",))
    flow_score = 1.0 if {"money", "trust", "risk"} & set(left.flows + right.flows) else 0.35
    activation = max(float(left_activation or 0.0), float(right_activation or 0.0))
    return clamp_score(0.45 * stake_score + 0.35 * flow_score + 0.20 * activation)


def _actionability(left: ImpactSignature, right: ImpactSignature) -> float:
    action_surface = 1.0 if left.action_surface or right.action_surface else 0.35
    pressure = 1.0 if {"blocker", "dependency", "opportunity"} & set(left.pressures + right.pressures) else 0.45
    return clamp_score(0.55 * pressure + 0.45 * action_surface)


def _pressure_priority(sig: ImpactSignature) -> int:
    priority = {
        "blocker": 5,
        "dependency": 4,
        "overload": 3,
        "decay": 3,
        "deadline": 2,
        "opportunity": 1,
    }
    return max((priority.get(p, 0) for p in sig.pressures), default=0)


def _avg(values: Iterable[float]) -> float:
    vals = [float(v) for v in values]
    if not vals:
        return 0.0
    return clamp_score(sum(vals) / len(vals))


def _vector_literal(vec: Iterable[float]) -> str:
    # Avoid relying on a process-wide pgvector codec; asyncpg can cast
    # this stable string literal with `$n::vector`.
    vals = []
    for value in vec:
        f = float(value)
        if not math.isfinite(f):
            f = 0.0
        vals.append(f"{f:.8f}")
    return "[" + ",".join(vals) + "]"


__all__ = [
    "ImpactSignature",
    "LatentTopologyService",
    "TopologyGenerationResult",
    "TopologySweepReport",
    "TopologyScore",
    "impact_signature",
    "impact_signature_from_row",
]
