"""Feed Think context-use telemetry into model-pair evidence."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from .repo import EdgeIntelligenceRepo
from .types import PairEvidenceObservation, normalize_primitive


async def record_context_use_pair_feedback(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    trigger_ref: UUID,
    context_use: dict[str, Any],
    primitive: str | None = None,
    max_selected_models: int = 24,
) -> None:
    """Persist pair evidence from retrieval/context-use telemetry.

    Selected together is weak evidence (`co_retrieved_delta`). Referenced
    together after validation is stronger (`co_used_valid_diff_delta`).
    Explicit no-edge rationale against graph context records negative memory.
    """
    selected = _uuid_list(context_use.get("selected_model_ids"))[:max_selected_models]
    referenced = _uuid_list(context_use.get("referenced_model_ids"))[
        :max_selected_models
    ]
    graph_selected = _uuid_list(context_use.get("graph_selected_model_ids"))[
        :max_selected_models
    ]
    if len(selected) < 2 and len(referenced) < 2 and len(graph_selected) < 2:
        return

    repo = EdgeIntelligenceRepo()
    pair_primitive = normalize_primitive(primitive)
    metadata = {
        "trigger_ref": str(trigger_ref),
        "context_use_grade": context_use.get("context_use_grade"),
        "graph_relation_contract_basis": context_use.get(
            "graph_relation_contract_basis"
        ),
    }
    try:
        async with conn.transaction():
            for left, right in _pairs(selected):
                await repo.record_pair_observation(
                    conn,
                    PairEvidenceObservation(
                        tenant_id=tenant_id,
                        left_model_id=left,
                        right_model_id=right,
                        primitive=pair_primitive,
                        co_retrieved_delta=1,
                        metadata=metadata,
                    ),
                )
            for left, right in _pairs(referenced):
                await repo.record_pair_observation(
                    conn,
                    PairEvidenceObservation(
                        tenant_id=tenant_id,
                        left_model_id=left,
                        right_model_id=right,
                        primitive=pair_primitive,
                        co_used_valid_diff_delta=1,
                        positive_outcome_delta=1,
                        metadata=metadata,
                    ),
                )
            if context_use.get("graph_no_edge_rationale_present"):
                for left, right in _pairs(graph_selected):
                    await repo.record_pair_observation(
                        conn,
                        PairEvidenceObservation(
                            tenant_id=tenant_id,
                            left_model_id=left,
                            right_model_id=right,
                            primitive=pair_primitive,
                            no_edge_delta=1,
                            metadata=metadata,
                        ),
                    )
    except Exception:  # noqa: BLE001
        return


def _uuid_list(values: Any) -> list[UUID]:
    if not isinstance(values, list):
        return []
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        try:
            uid = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError):
            continue
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _pairs(values: list[UUID]) -> list[tuple[UUID, UUID]]:
    out: list[tuple[UUID, UUID]] = []
    for idx, left in enumerate(values):
        for right in values[idx + 1 :]:
            if left != right:
                out.append((left, right))
    return out


__all__ = ["record_context_use_pair_feedback"]
