from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone

import pytest

from services.ingest.source_certification.cli import main
from services.ingest.source_certification.io import (
    certification_input_dict,
    load_certification_input,
    parse_certification_input,
    write_certification_input,
)
from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    CertificationInvariantError,
    ScenarioResult,
    SuiteResult,
)


NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
METRICS = (
    ("backlog_growth_per_second", 0.0),
    ("bytes_per_second", 1_000.0),
    ("cooldown_violations", 0.0),
    ("cpu_percent", 20.0),
    ("cross_tenant_leaks", 0.0),
    ("cursor_consistency_errors", 0.0),
    ("dlq_entries", 0.0),
    ("hot_loops", 0.0),
    ("installations_per_tenant", 2.0),
    ("kafka_lag", 0.0),
    ("lab_capacity_ratio", 2.0),
    ("lab_p99_timeout_ratio", 0.1),
    ("memory_bytes", 1_024.0),
    ("missing_records", 0.0),
    ("observation_p99_latency_ms", 20.0),
    ("offered_rate", 10.0),
    ("p50_latency_ms", 5.0),
    ("p95_latency_ms", 10.0),
    ("p99_latency_ms", 15.0),
    ("quota_units_per_second", 10.0),
    ("rate_limited_responses", 0.0),
    ("records_per_second", 10.0),
    ("replicas", 2.0),
    ("requests_per_second", 10.0),
    ("retries", 0.0),
    ("search_tolerance_ratio", 0.04),
    ("soak_seconds", 3_600.0),
    ("stable_rate", 9.5),
    ("tenants", 2.0),
    ("unexpected_duplicates", 0.0),
    ("validation_seconds", 900.0),
    ("warmup_seconds", 120.0),
)


def _input() -> CertificationInput:
    suites = tuple(
        SuiteResult(
            kind=kind,
            state="passed",
            artifact_uri=f"artifact://{kind}",
            started_at=NOW,
            completed_at=NOW,
            metrics=METRICS,
            limiting_component="provider_quota",
        )
        for kind in ("historical", "live", "combined")
    )
    return CertificationInput(
        spec_hash="a" * 64,
        local_correctness="passed",
        local_correctness_artifact="artifact://correctness",
        scenario_results=(
            ScenarioResult(
                scenario_id="auth_success_and_expiry",
                state="passed",
                artifact_uri="artifact://scenario/auth",
            ),
        ),
        provider_safe_suites=suites,
        fyralis_ceiling_suites=suites,
        fault_recovery_suites=suites,
        canary=CanaryResult(
            state="passed",
            operation_results=(
                CanaryOperationResult(
                    operation_id="auth.conformance",
                    state="passed",
                    artifact_uri="artifact://canary/auth",
                ),
            ),
            tested_at=NOW,
            account_type="dedicated disposable account",
            api_version="v1",
            artifact_uri="artifact://canary",
            request_count=1,
            account_identity_sha256="b" * 64,
        ),
        legacy_reference_count=0,
    )


def _json_shape() -> dict[str, object]:
    raw = asdict(_input())
    for field in (
        "provider_safe_suites",
        "fyralis_ceiling_suites",
        "fault_recovery_suites",
    ):
        raw[field] = [
            {
                **item,
                "metrics": dict(item["metrics"]),
                "started_at": item["started_at"].isoformat(),
                "completed_at": item["completed_at"].isoformat(),
            }
            for item in raw[field]
        ]
    raw["canary"]["tested_at"] = raw["canary"]["tested_at"].isoformat()
    return raw


def test_strict_json_parser_round_trips_certification_input() -> None:
    assert parse_certification_input(_json_shape()) == _input()


def test_canonical_writer_round_trips_certification_input(tmp_path) -> None:
    path = tmp_path / "slack.json"
    value = _input()

    assert parse_certification_input(certification_input_dict(value)) == value
    write_certification_input(path, value)
    assert load_certification_input(path) == value
    assert not list(tmp_path.glob("*.tmp"))


def test_json_parser_rejects_unknown_fields() -> None:
    raw = _json_shape()
    raw["waiver"] = True
    with pytest.raises(CertificationInvariantError, match="unknown fields"):
        parse_certification_input(raw)


def test_suite_rejects_duplicate_or_non_finite_metrics() -> None:
    with pytest.raises(CertificationInvariantError, match="duplicate"):
        SuiteResult(
            kind="live",
            state="blocked",
            metrics=(("latency", 1.0), ("latency", 2.0)),
        )
    with pytest.raises(CertificationInvariantError, match="finite"):
        SuiteResult(
            kind="live",
            state="blocked",
            metrics=(("latency", float("nan")),),
        )


def test_inventory_command_is_fail_closed(capsys) -> None:  # noqa: ANN001
    assert main(["inventory", "--require-ready"]) == 1
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["state"] == "blocked"
    assert rendered["required_sources"] == 27
    assert rendered["evidence_ready_sources"] == 0
    assert rendered["provider_transport_enforced_sources"] == 27
    assert all(
        source["required_scenarios"]
        and source["required_canary_operations"]
        for source in rendered["sources"]
    )


def test_manifest_with_no_artifacts_reports_all_sources_missing(
    tmp_path,
    capsys,
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "services.ingest.source_certification.cli._strict_ratchet_clean",
        lambda: False,
    )
    assert main(
        ["manifest", "--input-dir", str(tmp_path)]
    ) == 1
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["state"] == "blocked"
    assert len(rendered["missing_sources"]) == 27
    assert "_architecture" in rendered["failures"]


def test_manifest_rejects_unexpected_or_symlinked_inputs(
    tmp_path,
    capsys,
) -> None:  # noqa: ANN001
    (tmp_path / "waiver.json").write_text("{}\n", encoding="utf-8")
    assert main(["manifest", "--input-dir", str(tmp_path)]) == 2
    assert "unexpected files" in capsys.readouterr().err

    (tmp_path / "waiver.json").unlink()
    target = tmp_path.parent / f".{tmp_path.name}.actual.json"
    target.write_text("{}\n", encoding="utf-8")
    (tmp_path / "slack.json").symlink_to(target)
    try:
        assert main(["manifest", "--input-dir", str(tmp_path)]) == 2
        assert "must not be a symlink" in capsys.readouterr().err
    finally:
        target.unlink()
