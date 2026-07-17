"""Continuous bounded evaluation of correction-loop homeostasis."""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping, Sequence


JsonObject = Mapping[str, Any]


def evaluate_correction_homeostasis(
    episodes: Sequence[JsonObject], *, cascade: JsonObject
) -> dict[str, Any]:
    rows = [_normalize(row, index + 1) for index, row in enumerate(episodes)]
    required = sum(row["repair_required"] for row in rows)
    contained = sum(row["fenced"] + row["repaired"] for row in rows)
    unsafe = sum(row["unsafe_readable"] for row in rows)
    debt = sum(sum(row["residual_debt_by_fate"].values()) for row in rows)
    debt_population = Counter()
    for row in rows:
        debt_population.update(row["residual_debt_by_fate"])
    debt_total = sum(debt_population.values())
    debt_distribution = {
        fate: count / debt_total for fate, count in sorted(debt_population.items())
    } if debt_total else {}
    convergence = 1.0 if required == 0 else min(1.0, contained / required)
    safe_containment = 1.0 if required == 0 else max(0.0, 1.0 - unsafe / required)
    replay_idempotency = _mean(row["replay_new_work"] == 0 for row in rows)
    batch_integrity = _mean(row["batch_signal_count"] >= 2 for row in rows)
    fingerprints = [row["durable_state_fingerprint"] for row in rows]
    repeated_correction_stability = _monotonic_nonincreasing(
        [sum(row["residual_debt_by_fate"].values()) for row in rows]
    )
    episode_debt = [sum(row["residual_debt_by_fate"].values()) for row in rows]
    final_debt_clearance = (
        0.0 if not episode_debt
        else 1.0 if episode_debt[-1] == 0
        else max(0.0, 1.0 - episode_debt[-1] / max(1, episode_debt[0]))
    )
    cascade_recall = _ratio(
        int(cascade.get("visited_unique_nodes") or 0),
        int(cascade.get("reachable_unique_nodes") or 0),
        empty=0.0,
    )
    cascade_cycle_safety = 1.0 if (
        cascade.get("terminated") is True
        and int(cascade.get("duplicate_work_items") or 0) == 0
        and int(cascade.get("visited_unique_nodes") or 0)
        <= int(cascade.get("reachable_unique_nodes") or 0)
    ) else 0.0
    restart_state_stability = 1.0 if (
        cascade.get("restart_replay_equal") is True
        and cascade.get("pre_restart_fingerprint")
        == cascade.get("post_restart_fingerprint")
    ) else 0.0
    measurements = {
        "convergence_ratio": convergence,
        "safe_containment_ratio": safe_containment,
        "replay_idempotency_ratio": replay_idempotency,
        "batch_integrity_ratio": batch_integrity,
        "repeated_correction_stability": repeated_correction_stability,
        "cascade_reachability_ratio": cascade_recall,
        "cascade_cycle_safety": cascade_cycle_safety,
        "restart_state_stability": restart_state_stability,
        "residual_debt_per_correction": debt / len(rows) if rows else 0.0,
        "final_residual_debt_clearance": final_debt_clearance,
    }
    quality_dimensions = [
        convergence, safe_containment, replay_idempotency, batch_integrity,
        repeated_correction_stability, cascade_recall, cascade_cycle_safety,
        restart_state_stability,
        final_debt_clearance,
    ]
    checks = {
        "repeated_corrections_exercised": len(rows) >= 3,
        "correction_converges": convergence >= 0.90,
        "unsafe_reads_contained": safe_containment == 1.0,
        "replay_is_idempotent": replay_idempotency == 1.0,
        "restart_preserves_state": restart_state_stability == 1.0,
        "deep_cascade_is_complete_and_cycle_safe": (
            cascade_recall == 1.0 and cascade_cycle_safety == 1.0
            and int(cascade.get("max_depth") or 0) >= 4
        ),
        "repair_debt_does_not_grow": repeated_correction_stability == 1.0,
        "terminal_repair_debt_is_cleared": final_debt_clearance == 1.0,
        "signals_are_batched": batch_integrity == 1.0,
    }
    return {
        "schema_version": "correction-homeostasis-evaluation-v1",
        "population": {
            "correction_episodes": len(rows), "repair_required": required,
            "residual_debt": debt,
        },
        "measurements": measurements,
        "residual_repair_debt": {
            "count": debt, "by_fate": dict(sorted(debt_population.items())),
            "distribution": debt_distribution,
            "per_episode": [row["residual_debt_by_fate"] for row in rows],
        },
        "cascade": dict(cascade),
        "continuous_score": _mean(quality_dimensions),
        "checks": checks,
        "verdict": "meets_policy" if all(checks.values()) else "below_policy",
        "durable_state_fingerprints": fingerprints,
        "proof_boundary": (
            "Bounded correction episodes and dependency traversal are measured; "
            "this does not establish unbounded production convergence or recovery "
            "from infrastructure loss."
        ),
    }


def _normalize(row: JsonObject, sequence: int) -> dict[str, Any]:
    raw_debt = row.get("residual_debt_by_fate")
    debt = {
        str(key): max(0, int(value))
        for key, value in raw_debt.items()
    } if isinstance(raw_debt, Mapping) else {}
    return {
        "sequence": int(row.get("sequence") or sequence),
        "repair_required": max(0, int(row.get("repair_required") or 0)),
        "fenced": max(0, int(row.get("fenced") or 0)),
        "repaired": max(0, int(row.get("repaired") or 0)),
        "unsafe_readable": max(0, int(row.get("unsafe_readable") or 0)),
        "replay_new_work": max(0, int(row.get("replay_new_work") or 0)),
        "batch_signal_count": max(0, int(row.get("batch_signal_count") or 0)),
        "residual_debt_by_fate": debt,
        "durable_state_fingerprint": str(row.get("durable_state_fingerprint") or ""),
    }


def _monotonic_nonincreasing(values: Sequence[int]) -> float:
    if len(values) < 2:
        return 0.0
    return _mean(right <= left for left, right in zip(values, values[1:]))


def _ratio(numerator: int, denominator: int, *, empty: float) -> float:
    return empty if denominator <= 0 else numerator / denominator


def _mean(values) -> float:
    items = [float(value) for value in values]
    return 0.0 if not items else sum(items) / len(items)


__all__ = ["evaluate_correction_homeostasis"]
