from lib.evaluation.epistemic_repair.p1_finalize import finalize_p1


def test_p1_finalizer_requires_all_three_evidence_planes() -> None:
    deterministic = {
        "deterministic_passed": True,
        "hard_gates": {"HG-01_benchmark_blindness": True, "HG-13_observability": True},
    }
    real = {
        "passed": True,
        "provider": "codex",
        "model": "gpt-5.4",
        "physical_attempt_count": 1,
        "logical_call_count": 1,
        "elapsed_s": 8.0,
        "context_digest_present": True,
        "usage_exactness": ["reported"],
        "cost_usd": 0,
    }
    durable = {
        "passed": True,
        "reopened_on_new_connection": True,
        "identical_replay_idempotent": True,
        "logical_rows": 1,
        "attempt_rows": 1,
    }
    report = finalize_p1(
        deterministic=deterministic, real_smoke=real, durability=durable, commit="a" * 40
    )
    assert report["phase_exit_ready"] is True
    assert report["execution_mode"] == "deterministic_plus_bounded_codex_cli"

    durable["reopened_on_new_connection"] = False
    failed = finalize_p1(
        deterministic=deterministic, real_smoke=real, durability=durable, commit="a" * 40
    )
    assert failed["phase_exit_ready"] is False
