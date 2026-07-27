from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.ingest.source_certification.load_search import (
    LOAD_ARTIFACT_SCHEMA_VERSION,
    LoadMeasurement,
    LoadSearchConfig,
    LoadTopology,
    VerifiedQuotaEvidence,
    certification_stable,
    compare_load_envelopes,
    find_maximum_stable_rate,
    run_artifact_load_search,
)
from services.ingest.source_certification.models import LoadSuite
from services.ingest.synthetic.provider_lab.calibration import (
    LabCalibrationConfig,
    assess_lab_calibration,
)


def _measurement(rate: float, seconds: int, *, stable: bool) -> LoadMeasurement:
    return LoadMeasurement(
        offered_rate=rate,
        duration_seconds=seconds,
        stable=stable,
        requests_per_second=min(rate, 100),
        quota_units_per_second=min(rate, 100),
        records_per_second=min(rate, 100),
        bytes_per_second=min(rate, 100) * 1000,
        p50_latency_ms=5,
        p95_latency_ms=10,
        p99_latency_ms=20,
        kafka_lag=0,
        observation_p99_latency_ms=50,
        backlog_growth_per_second=0 if stable else 1,
        missing_records=0,
        unexpected_duplicates=0,
        cross_tenant_leaks=0,
        cooldown_violations=0,
        cursor_consistency_errors=0,
        dlq_entries=0,
        cpu_percent=50,
        memory_bytes=1024,
        limiting_component="provider_quota" if stable else "kafka",
    )


def _lab_calibration(*, capacity_ratio: float = 3):
    config = LabCalibrationConfig(
        target_fyralis_rps=100,
        client_timeout_seconds=1,
        minimum_samples=100,
    )
    return assess_lab_calibration(
        config=config,
        elapsed_seconds=1,
        latencies_seconds=[0.05] * int(100 * capacity_ratio),
        failed_requests=0,
    )


def _promotion_calibration():
    config = LabCalibrationConfig(
        target_fyralis_rps=1,
        client_timeout_seconds=1,
        probe_seconds=30,
        minimum_samples=1,
    )
    return assess_lab_calibration(
        config=config,
        elapsed_seconds=30,
        latencies_seconds=[0.01] * 60,
        failed_requests=0,
    )


async def test_search_steps_brackets_and_validates_within_five_percent() -> None:
    calls = []

    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        calls.append((rate, seconds, phase, mode))
        return _measurement(rate, seconds, stable=rate <= 100)

    envelope = await find_maximum_stable_rate(
        run,
        mode="provider_safe",
        config=LoadSearchConfig(initial_rate=40),
        lab_calibration=_lab_calibration(),
        include_soak=True,
    )

    assert 95 <= envelope.maximum_stable_rate <= 100
    assert envelope.tolerance_fraction <= 0.05
    assert envelope.validation.duration_seconds == 900
    assert envelope.soak is not None
    assert envelope.soak.duration_seconds == 3600
    assert calls[0][2] == "warmup"
    assert any(call[2] == "binary_search" for call in calls)


async def test_unstable_warmup_fails_without_searching() -> None:
    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        return _measurement(rate, seconds, stable=False)

    with pytest.raises(RuntimeError, match="warmup"):
        await find_maximum_stable_rate(
            run,
            mode="fyralis_ceiling",
            config=LoadSearchConfig(initial_rate=10),
            lab_calibration=_lab_calibration(),
            include_soak=False,
        )


def test_correctness_failure_is_never_stable() -> None:
    measurement = _measurement(10, 120, stable=True)
    broken = replace(measurement, cross_tenant_leaks=1)
    assert certification_stable(broken) is False


async def test_search_rejects_an_underpowered_provider_lab() -> None:
    calls = 0

    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        nonlocal calls
        calls += 1
        return _measurement(rate, seconds, stable=True)

    with pytest.raises(RuntimeError, match="calibration failed"):
        await find_maximum_stable_rate(
            run,
            mode="provider_safe",
            config=LoadSearchConfig(initial_rate=10),
            lab_calibration=_lab_calibration(capacity_ratio=1),
            include_soak=False,
        )
    assert calls == 0


async def test_short_virtual_search_writes_a_non_promotable_artifact(
    tmp_path: Path,
) -> None:
    suite = LoadSuite(
        kind="combined",
        operation_mix=("live", "backfill", "reconcile", "renew"),
        warmup_seconds=120,
        stable_seconds=900,
        weekly_soak_seconds=3_600,
    )

    async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
        return replace(
            _measurement(rate, seconds, stable=rate <= 2),
            operation_counts=(
                ("executed_mix:live", 1),
                ("executed_mix:backfill", 1),
            ),
            wall_elapsed_seconds=0.01,
        )

    artifact_path = tmp_path / "combined" / "fyralis_ceiling.json"
    artifact = await run_artifact_load_search(
        run,
        source_id="slack",
        suite=suite,
        mode="fyralis_ceiling",
        topology=LoadTopology.from_suite(suite),
        config=LoadSearchConfig(
            initial_rate=1,
            warmup_seconds=1,
            step_seconds=1,
            validation_seconds=1,
            soak_seconds=1,
        ),
        clock_mode="virtual",
        lab_calibration=_lab_calibration(),
        include_soak=False,
        operation_coverage_ratio=1.0,
        pipeline_e2e_proven=False,
        artifact_path=artifact_path,
    )

    parsed = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert parsed["schema_version"] == LOAD_ARTIFACT_SCHEMA_VERSION
    assert parsed["operation_mix"] == list(suite.operation_mix)
    assert parsed["promotion_eligible"] is False
    assert artifact.promotion_eligible is False
    assert artifact.operation_coverage_ratio == 0.5
    assert {
        trial["phase"] for trial in parsed["envelope"]["trials"]
    } >= {"warmup", "step", "binary_search", "validation"}
    assert "virtual-clock load cannot be used for promotion" in (
        parsed["promotion_failures"]
    )
    assert any(
        "pipeline proof" in failure
        for failure in parsed["promotion_failures"]
    )


async def test_declared_wall_clock_artifacts_compare_provider_headroom() -> None:
    suite = LoadSuite(
        kind="historical",
        operation_mix=("plan", "fetch", "reconcile"),
        tenants=1,
        installations_per_tenant=1,
        replicas=1,
        warmup_seconds=1,
        stable_seconds=1,
        weekly_soak_seconds=1,
    )

    def runner(limit: float):
        async def run(rate, seconds, phase, mode):  # noqa: ANN001,ANN202
            return replace(
                _measurement(rate, seconds, stable=rate <= limit),
                wall_elapsed_seconds=float(seconds),
                operation_counts=tuple(
                    (f"executed_mix:{operation}", 1)
                    for operation in suite.operation_mix
                ),
            )

        return run

    quota = (
        VerifiedQuotaEvidence(
            bucket="web-api",
            scope="workspace/app",
            capacity=10,
            refill_per_second=1,
            evidence_uri="https://docs.example.test/quota",
            verified_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        ),
    )
    common = {
        "source_id": "slack",
        "suite": suite,
        "topology": LoadTopology.from_suite(suite),
        "config": LoadSearchConfig.from_suite(suite, initial_rate=1),
        "clock_mode": "wall",
        "lab_calibration": _promotion_calibration(),
        "include_soak": True,
        "operation_coverage_ratio": 1.0,
        "pipeline_e2e_proven": True,
    }
    provider = await run_artifact_load_search(
        runner(2),
        mode="provider_safe",
        quota_evidence=quota,
        **common,
    )
    ceiling = await run_artifact_load_search(
        runner(3),
        mode="fyralis_ceiling",
        **common,
    )
    comparison = compare_load_envelopes(provider, ceiling)

    assert provider.promotion_eligible is True
    assert ceiling.promotion_eligible is True
    assert comparison.fyralis_ceiling_rate >= comparison.provider_safe_rate
    assert comparison.headroom_ratio >= 1


def test_verified_quota_evidence_rejects_unlabelled_or_naive_values() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        VerifiedQuotaEvidence(
            bucket="api",
            scope="workspace",
            capacity=1,
            refill_per_second=1,
            evidence_uri="file:///guessed",
            verified_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        VerifiedQuotaEvidence(
            bucket="api",
            scope="workspace",
            capacity=1,
            refill_per_second=1,
            evidence_uri="https://docs.example.test/quota",
            verified_at=datetime(2026, 7, 27),
        )
    with pytest.raises(ValueError, match="supported quota dimensions"):
        VerifiedQuotaEvidence(
            bucket="api",
            scope="opaque-provider-label",
            capacity=1,
            refill_per_second=1,
            evidence_uri="https://docs.example.test/quota",
            verified_at=datetime(2026, 7, 27, tzinfo=timezone.utc),
        )
