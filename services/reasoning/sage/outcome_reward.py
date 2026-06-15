"""Reward feature assembly for the Sage OutcomeEvaluator."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


@dataclass(frozen=True, slots=True)
class RewardFeatureOutcome:
    reward_features: dict[str, float]
    noisy_paths: list[dict[str, Any]]
    retrieved_count: int
    used_count: int
    packet_node_count: int
    applied_acts: int
    proposed_acts: int
    noisy_path_count: int
    used_path_count: int


def build_reward_features(
    *,
    packet: dict[str, Any],
    ops_applied: dict[str, Any],
    evidence_items: list[asyncpg.Record],
    omitted_rows: list[asyncpg.Record],
    used_evidence_ids: set[UUID],
    used_node_ids: list[UUID],
    run_status: str,
    counterevidence_retrieved: int,
    counterevidence_in_packet: int,
    duplicate_evidence: int,
    packet_tokens: float,
) -> RewardFeatureOutcome:
    retrieved_count = len(evidence_items)
    used_count = len(used_evidence_ids)
    packet_node_count = _count_packet_nodes(packet)
    applied_acts, proposed_acts = _collect_act_op_counts(ops_applied)
    added_nodes = sum(
        1
        for op in _coerce_list(ops_applied.get("claim_ops"))
        if isinstance(op, dict) and op.get("op") == "insert"
    )
    merged_nodes = sum(
        1
        for op in _coerce_list(ops_applied.get("claim_ops"))
        if isinstance(op, dict)
        and op.get("op") == "archive"
        and (op.get("reason") or "").startswith("superseded")
    )
    used_path_count = len(used_node_ids)
    noisy_path_count = sum(
        1
        for orow in omitted_rows
        if orow.get("omission_reason") in ("generic_hub", "redundant")
    )
    noisy_paths = (
        [{"count": noisy_path_count, "from": "omitted_evidence"}]
        if noisy_path_count
        else []
    )

    if run_status == "success":
        diff_deducibility = 1.0
    elif run_status == "partial":
        diff_deducibility = 0.5
    else:
        diff_deducibility = 0.0

    reward_features: dict[str, float] = {
        "evidence_coverage": _clamp(used_count / max(retrieved_count, 1), 0.0, 1.0),
        "diff_deducibility": diff_deducibility,
        "compression_gain": _clamp(used_count / max(packet_node_count, 1), 0.0, 2.0),
        "prediction_falsification_value": 0.0,  # TODO Phase 14+
        "action_value": _clamp(applied_acts / max(proposed_acts, 1), 0.0, 1.0),
        "counterevidence_preservation": _clamp(
            counterevidence_in_packet / max(counterevidence_retrieved, 1),
            0.0,
            1.0,
        ),
        "graph_bloat": float(added_nodes - merged_nodes),
        "redundancy": _clamp(duplicate_evidence / max(retrieved_count, 1), 0.0, 1.0),
        "noise_introduced": _clamp(
            noisy_path_count / max(used_path_count, 1),
            0.0,
            2.0,
        ),
        "token_cost": _clamp(packet_tokens / 30000.0, 0.0, 2.0),
        "permission_risk": 0.0,  # TODO Phase 14+
    }
    return RewardFeatureOutcome(
        reward_features=reward_features,
        noisy_paths=noisy_paths,
        retrieved_count=retrieved_count,
        used_count=used_count,
        packet_node_count=packet_node_count,
        applied_acts=applied_acts,
        proposed_acts=proposed_acts,
        noisy_path_count=noisy_path_count,
        used_path_count=used_path_count,
    )


def _count_packet_nodes(packet: dict[str, Any]) -> int:
    return sum(1 for value in _walk_strings(packet) if _try_uuid(value) is not None)


def _collect_act_op_counts(ops_applied: dict[str, Any]) -> tuple[int, int]:
    applied = len(_coerce_list(ops_applied.get("act_ops")))
    dropped = int(ops_applied.get("dropped_op_count") or 0)
    proposed = applied + dropped
    return applied, proposed


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _try_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _clamp(value: float, lo: float, hi: float) -> float:
    if value < lo:
        return lo
    if value > hi:
        return hi
    return value


def _walk_strings(value: Any) -> list[str]:
    out: list[str] = []
    stack: list[Any] = [value]
    while stack:
        cur = stack.pop()
        if cur is None:
            continue
        if isinstance(cur, str):
            out.append(cur)
        elif isinstance(cur, dict):
            stack.extend(cur.values())
        elif isinstance(cur, (list, tuple)):
            stack.extend(cur)
    return out
