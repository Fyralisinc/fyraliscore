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
    validation_error_count: int = 0,
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

    durable_outcome_count = _durable_outcome_count(ops_applied)
    residual_creation_count = _summary_count(ops_applied.get("residual_creations"))
    residual_repair_count = _summary_count(
        ops_applied.get("residual_repair_triggers")
    )
    drop_count = (
        max(0, int(validation_error_count or 0))
        + _summary_count(ops_applied.get("apply_dropped_op_count"))
        + _summary_count(ops_applied.get("dropped_op_count"))
    )
    selected_context_count, selected_context_used = _selected_context_use(ops_applied)
    selected_unused_rate = (
        1.0 if selected_context_count > 0 and not selected_context_used else 0.0
    )
    selected_context_use = 1.0 if selected_context_count == 0 or selected_context_used else 0.0
    metabolism_denominator = max(
        1,
        durable_outcome_count + residual_creation_count + residual_repair_count + drop_count,
    )
    durable_fate_rate = _clamp(durable_outcome_count / metabolism_denominator, 0.0, 1.0)
    residual_creation_rate = _clamp(
        residual_creation_count / max(1, durable_outcome_count + residual_creation_count),
        0.0,
        1.0,
    )
    validation_drop_rate = _clamp(drop_count / metabolism_denominator, 0.0, 1.0)
    omitted_later_requested_rate = _clamp(
        _summary_count(ops_applied.get("omitted_later_requested"))
        / max(1, len(omitted_rows)),
        0.0,
        1.0,
    )
    useful_fate_count = durable_outcome_count + residual_repair_count + (
        1 if selected_context_used else 0
    )
    token_per_useful_fate = _clamp(
        packet_tokens / max(1, useful_fate_count) / 30000.0,
        0.0,
        2.0,
    )

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
        "durable_fate_rate": durable_fate_rate,
        "selected_context_use": selected_context_use,
        "selected_unused_rate": selected_unused_rate,
        "validation_drop_rate": validation_drop_rate,
        "residual_creation_rate": residual_creation_rate,
        "omitted_later_requested_rate": omitted_later_requested_rate,
        "token_per_useful_fate": token_per_useful_fate,
        "retrieval_outcome_reward": _retrieval_outcome_reward(
            durable_fate_rate=durable_fate_rate,
            selected_context_use=selected_context_use,
            selected_unused_rate=selected_unused_rate,
            validation_drop_rate=validation_drop_rate,
            residual_creation_rate=residual_creation_rate,
            omitted_later_requested_rate=omitted_later_requested_rate,
            token_per_useful_fate=token_per_useful_fate,
            diff_deducibility=diff_deducibility,
            counterevidence_preservation=(
                1.0
                if counterevidence_retrieved <= 0
                else _clamp(
                    counterevidence_in_packet / counterevidence_retrieved,
                    0.0,
                    1.0,
                )
            ),
            action_value=(
                1.0
                if proposed_acts <= 0
                else _clamp(applied_acts / proposed_acts, 0.0, 1.0)
            ),
        ),
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


_DURABLE_OP_KEYS = (
    "claim_ops",
    "memory_lifecycle_ops",
    "relation_claim_ops",
    "relation_frame_ops",
    "edge_ops",
    "ontology_gap_ops",
    "open_question_ops",
    "formation_resolutions",
    "act_ops",
    "resource_ops",
)


def _durable_outcome_count(ops_applied: dict[str, Any]) -> int:
    count = _summary_count(ops_applied.get("state_changes_emitted"))
    count += _summary_count(ops_applied.get("negative_memory_inserts"))
    count += _summary_count(ops_applied.get("residual_absorptions"))
    count += len(_coerce_list(ops_applied.get("applied_model_ids")))
    for key in _DURABLE_OP_KEYS:
        count += sum(
            1 for item in _coerce_list(ops_applied.get(key)) if _item_is_durable(item)
        )
    return max(0, count)


def _item_is_durable(item: Any) -> bool:
    if item is None:
        return False
    if not isinstance(item, dict):
        return True
    if item.get("error") or item.get("status") == "dropped":
        return False
    op = str(item.get("op") or "").lower()
    if op in {"skip", "noop", "no_op"}:
        return False
    decision = str(item.get("decision") or "").lower()
    if decision.startswith("skipped"):
        return False
    return bool(item)


def _summary_count(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return max(0, int(value))
    if isinstance(value, dict):
        return _summary_count(value.get("count"))
    if isinstance(value, (list, tuple, set)):
        return len(value)
    return 0


def _selected_context_use(ops_applied: dict[str, Any]) -> tuple[int, bool]:
    context_use = ops_applied.get("context_use")
    if not isinstance(context_use, dict):
        return 0, False
    selected_count = _summary_count(context_use.get("selected_context_count"))
    if selected_count == 0:
        selected_count = (
            len(_coerce_list(context_use.get("selected_model_ids")))
            + len(_coerce_list(context_use.get("selected_observation_ids")))
            + len(_coerce_list(context_use.get("graph_selected_model_ids")))
        )
    grade = str(context_use.get("context_use_grade") or "")
    selected_used = bool(context_use.get("selected_context_used")) or grade in {
        "graph_context_used",
        "model_context_used",
        "observation_context_used",
        "justified_noop_context_used",
        "selected_context_accounted",
    }
    return selected_count, selected_used


def _retrieval_outcome_reward(
    *,
    durable_fate_rate: float,
    selected_context_use: float,
    selected_unused_rate: float,
    validation_drop_rate: float,
    residual_creation_rate: float,
    omitted_later_requested_rate: float,
    token_per_useful_fate: float,
    diff_deducibility: float,
    counterevidence_preservation: float,
    action_value: float,
) -> float:
    score = (
        (0.34 * durable_fate_rate)
        + (0.18 * selected_context_use)
        + (0.14 * diff_deducibility)
        + (0.12 * counterevidence_preservation)
        + (0.08 * action_value)
        - (0.18 * selected_unused_rate)
        - (0.14 * validation_drop_rate)
        - (0.14 * residual_creation_rate)
        - (0.10 * omitted_later_requested_rate)
        - (0.06 * _clamp(token_per_useful_fate / 2.0, 0.0, 1.0))
    )
    return _clamp(score, 0.0, 1.0)


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
