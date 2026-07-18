from copy import deepcopy

from services.evaluation.epistemic_repair.p8_latency_diagnostic import CONTROLS, analyze_repeated_warm_pairs


def _artifact(repetitions=5):
    arms = []
    for batch, horizon in CONTROLS:
        for repetition in range(1, repetitions + 1):
            order = (1, 20) if repetition % 2 else (20, 1)
            for execution_order, concurrency in enumerate(order, 1):
                arms.append({
                    "batch_size": batch, "horizon": horizon, "repetition": repetition,
                    "execution_order": execution_order, "concurrency": concurrency,
                    "tenant_denominator": concurrency, "batch_denominator": concurrency * horizon,
                    "sql_call_denominator": concurrency * horizon, "sql_calls_per_barrier_max": 1,
                    "wall_time_ms": 10.0, "evidence_digest": "a" * 64,
                    **{name: {"denominator": concurrency if name in {"bootstrap", "pool_wait", "first_barrier"}
                              else concurrency * (horizon - 1) if name == "steady_barrier"
                              else concurrency * horizon,
                              "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}
                       for name in ("bootstrap", "pool_wait", "first_barrier", "steady_barrier",
                                    "all_barriers", "observation_write", "retrieval")},
                })
    return {"preregistration": {"repetitions": repetitions, "existing_scale_gate_unchanged": True},
            "server_provenance": {"database": {"server_version": "test"}},
            "pg_stat_statements_before": {"status": "unavailable", "rows": []},
            "pg_stat_statements_after": {"status": "unavailable", "rows": []}, "arms": arms}


def test_exact_two_control_five_repetition_denominator_is_complete():
    result = analyze_repeated_warm_pairs(_artifact())
    assert result == {
        "diagnostic_complete": True, "expected_arm_count": 20, "observed_arm_count": 20,
        "exact_denominators": True, "alternating_order_verified": True,
        "existing_scale_gate_modified": False,
        "interpretation_status": "ready_for_structural_diagnosis",
    }


def test_missing_arm_wrong_order_or_denominator_fails_closed():
    for mutate in ("missing", "order", "denominator"):
        artifact = deepcopy(_artifact())
        if mutate == "missing": artifact["arms"].pop()
        elif mutate == "order": artifact["arms"][0]["execution_order"] = 2
        else: artifact["arms"][0]["steady_barrier"]["denominator"] = 0
        assert analyze_repeated_warm_pairs(artifact)["diagnostic_complete"] is False


def test_diagnostic_never_weakens_existing_scale_gate():
    artifact = _artifact(); artifact["preregistration"]["existing_scale_gate_unchanged"] = False
    result = analyze_repeated_warm_pairs(artifact)
    assert result["diagnostic_complete"] is False
    assert result["existing_scale_gate_modified"] is False
