"""Small eval harness for latent topology discovery.

The harness measures the thing topology is supposed to do: surface
hidden candidate relationships before accepted typed edges exist.
Tests and offline probes can seed Models, call this module with those
ModelRows, and get coverage/miss diagnostics without invoking Think.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow

from .field import LatentTopologyService


@dataclass(frozen=True)
class ExpectedPair:
    left_model_id: UUID
    right_model_id: UUID
    label: str
    allowed_edge_kinds: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExpectedSituation:
    member_model_ids: tuple[UUID, ...]
    label: str


@dataclass
class TopologyEvalReport:
    generated_candidate_ids: list[UUID] = field(default_factory=list)
    pair_hits: dict[str, UUID] = field(default_factory=dict)
    situation_hits: dict[str, UUID] = field(default_factory=dict)
    missed_pairs: list[str] = field(default_factory=list)
    missed_situations: list[str] = field(default_factory=list)
    false_positive_candidate_ids: list[UUID] = field(default_factory=list)

    @property
    def expected_count(self) -> int:
        return (
            len(self.pair_hits)
            + len(self.missed_pairs)
            + len(self.situation_hits)
            + len(self.missed_situations)
        )

    @property
    def hit_count(self) -> int:
        return len(self.pair_hits) + len(self.situation_hits)

    @property
    def recall(self) -> float:
        if self.expected_count <= 0:
            return 1.0
        return self.hit_count / self.expected_count

    def as_dict(self) -> dict[str, object]:
        return {
            "generated_candidate_ids": [
                str(candidate_id) for candidate_id in self.generated_candidate_ids
            ],
            "pair_hits": {label: str(cid) for label, cid in self.pair_hits.items()},
            "situation_hits": {
                label: str(cid) for label, cid in self.situation_hits.items()
            },
            "missed_pairs": list(self.missed_pairs),
            "missed_situations": list(self.missed_situations),
            "false_positive_candidate_ids": [
                str(candidate_id)
                for candidate_id in self.false_positive_candidate_ids
            ],
            "recall": self.recall,
        }


async def run_topology_eval(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    seed_models: list[ModelRow],
    expected_pairs: list[ExpectedPair] | None = None,
    expected_situations: list[ExpectedSituation] | None = None,
    service: LatentTopologyService | None = None,
    enqueue_think: bool = False,
) -> TopologyEvalReport:
    service = service or LatentTopologyService()
    expected_pairs = expected_pairs or []
    expected_situations = expected_situations or []

    generated_ids: list[UUID] = []
    for model in seed_models:
        result = await service.generate_for_model(
            conn,
            model=model,
            enqueue_think=enqueue_think,
        )
        generated_ids.extend(row["id"] for row in result.inserted_candidates)

    rows = await conn.fetch(
        """
        SELECT id, candidate_kind, source_model_id, target_model_id,
               edge_kind, member_model_ids, proposed_proposition
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        """,
        tenant_id,
        generated_ids,
    )

    report = TopologyEvalReport(generated_candidate_ids=generated_ids)
    used_candidate_ids: set[UUID] = set()
    for expected in expected_pairs:
        hit = _find_pair_hit(rows, expected)
        if hit is None:
            report.missed_pairs.append(expected.label)
            continue
        report.pair_hits[expected.label] = hit
        used_candidate_ids.add(hit)

    for expected in expected_situations:
        hit = _find_situation_hit(rows, expected)
        if hit is None:
            report.missed_situations.append(expected.label)
            continue
        report.situation_hits[expected.label] = hit
        used_candidate_ids.add(hit)

    report.false_positive_candidate_ids = [
        candidate_id
        for candidate_id in generated_ids
        if candidate_id not in used_candidate_ids
    ]
    return report


def _find_pair_hit(
    rows: list[asyncpg.Record],
    expected: ExpectedPair,
) -> UUID | None:
    wanted = {expected.left_model_id, expected.right_model_id}
    for row in rows:
        if row["candidate_kind"] not in {"edge", "edge_type"}:
            continue
        actual = (
            {row["source_model_id"], row["target_model_id"]}
            if row["candidate_kind"] == "edge"
            else set(row["member_model_ids"] or [])
        )
        if actual != wanted:
            continue
        edge_kind = row["edge_kind"]
        if row["candidate_kind"] == "edge_type":
            proposal = _decode_json(row["proposed_proposition"]) or {}
            edge_kind = (
                proposal.get("proposed_edge_kind")
                if isinstance(proposal, dict)
                else None
            )
        if (
            expected.allowed_edge_kinds
            and edge_kind not in expected.allowed_edge_kinds
        ):
            continue
        return row["id"]
    return None


def _decode_json(value: object) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _find_situation_hit(
    rows: list[asyncpg.Record],
    expected: ExpectedSituation,
) -> UUID | None:
    wanted = set(expected.member_model_ids)
    for row in rows:
        if row["candidate_kind"] != "situation":
            continue
        actual = set(row["member_model_ids"] or [])
        if wanted.issubset(actual):
            return row["id"]
    return None


__all__ = [
    "ExpectedPair",
    "ExpectedSituation",
    "TopologyEvalReport",
    "run_topology_eval",
]
