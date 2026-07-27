from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from services.ingest.source_certification.pipeline_load_runner import (
    PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION,
    DeclaredPipelineWorkload,
    IsolatedPipelineInfrastructure,
    LatencySummary,
    OfferReceipt,
    PipelineBoundaryProof,
    PipelineLoadArtifactError,
    PipelineLoadRunConfig,
    PipelineLoadTiming,
    PipelineLoadTopology,
    PipelineSnapshot,
    QuotaConstraint,
    TrialContext,
    VerifiedQuotaConfiguration,
    WorkItem,
    resolve_isolated_pipeline_infrastructure,
    run_pipeline_load,
    validate_pipeline_load_artifact,
    write_pipeline_load_artifact,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_ACK_ENV,
    PIPELINE_ACK_VALUE,
    PIPELINE_DATABASE_ENV,
    PIPELINE_KAFKA_ENV,
    PIPELINE_S3_BUCKET_ENV,
    PIPELINE_S3_ENDPOINT_ENV,
)


NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)


def _workload(kind: str = "combined") -> DeclaredPipelineWorkload:
    return DeclaredPipelineWorkload(
        kind=kind,  # type: ignore[arg-type]
        operation_mix=("slack.live_ingress", "slack.historical_fetch"),
    )


def _environment() -> dict[str, str]:
    return {
        PIPELINE_ACK_ENV: PIPELINE_ACK_VALUE,
        PIPELINE_DATABASE_ENV: (
            "postgresql://certifier:secret@127.0.0.1:55432/certification"
        ),
        PIPELINE_KAFKA_ENV: "127.0.0.1:59092",
        PIPELINE_S3_ENDPOINT_ENV: "http://127.0.0.1:59001",
        PIPELINE_S3_BUCKET_ENV: "fyralis-certification-raw",
    }


class _Clock:
    release_wall_clock = False

    def __init__(self) -> None:
        self.elapsed = 0.0

    def monotonic(self) -> float:
        return self.elapsed

    def now(self) -> datetime:
        return NOW + timedelta(seconds=self.elapsed)

    async def sleep(self, seconds: float) -> None:
        self.elapsed += max(0.0, seconds)
        await asyncio.sleep(0)


def _latency(count: int, value: float = 10.0) -> LatencySummary:
    return LatencySummary(
        count=count,
        p50_ms=value,
        p95_ms=value,
        p99_ms=value,
        maximum_ms=value,
    )


def _zero_snapshot() -> PipelineSnapshot:
    return PipelineSnapshot(
        offered_items=0,
        accepted_items=0,
        expected_observations=0,
        raw_s3_objects=0,
        raw_kafka_records=0,
        normalized_records=0,
        observations=0,
        t1_triggers=0,
        unique_observation_identities=0,
        unique_t1_observation_ids=0,
        raw_bytes=0,
        normalized_bytes=0,
        provider_requests=0,
        quota_units=0.0,
        unexpected_duplicates=0,
        cross_tenant_leaks=0,
        cursor_checks=0,
        cursor_consistency_errors=0,
        cooldown_violations=0,
        failed_requests=0,
        dlq_entries=0,
        raw_kafka_lag=0,
        normalized_kafka_lag=0,
        observation_to_t1_lag=0,
        peak_backlog=0,
        raw_latency=_latency(0, 0),
        normalized_latency=_latency(0, 0),
        observation_latency=_latency(0, 0),
        t1_latency=_latency(0, 0),
        tenant_ids=(),
        installation_ids=(),
        replica_ids=(),
        replica_processed_items=(),
        event_ledger_sha256="0" * 64,
        cursor_ledger_sha256="0" * 64,
    )


class _Adapter:
    def __init__(
        self,
        infrastructure: IsolatedPipelineInfrastructure,
        source_id: str,
        mode: str,
        workload: DeclaredPipelineWorkload,
        topology: PipelineLoadTopology,
        *,
        stable_through: float,
        clock: _Clock | None = None,
        drain_seconds: float = 0.0,
        inactive_replica: bool = False,
    ) -> None:
        self.boundary = PipelineBoundaryProof(
            evidence_class="test_double",
            source_id=source_id,
            binding_sha256=infrastructure.binding_sha256,
            dedicated_namespace="unit-test-pipeline-namespace",
            workload_kind=workload.kind,
            operation_mix_sha256=workload.declaration_sha256,
            raw_topic=f"ingestion.raw.{source_id}",
            normalized_topic=f"ingestion.normalized.{source_id}",
            observation_relation="observations",
            t1_relation="think_trigger_queue",
            quota_mode="strict" if mode == "provider_safe" else "disabled",
            topology=topology,
        )
        self.topology = topology
        self.stable_through = stable_through
        self.clock = clock
        self.drain_seconds = drain_seconds
        self.inactive_replica = inactive_replica
        self.context: TrialContext | None = None
        self.items: list[WorkItem] = []
        self.closed = False

    async def begin_trial(self, context: TrialContext) -> PipelineSnapshot:
        self.context = context
        self.items = []
        return _zero_snapshot()

    async def offer(self, item: WorkItem) -> OfferReceipt:
        self.items.append(item)
        return OfferReceipt(
            sequence=item.sequence,
            event_id=item.event_id,
            operation_id=item.operation_id,
            accepted=True,
            expected_observations=1,
            raw_bytes=100,
            provider_requests=1,
            quota_units=1.0,
        )

    async def finish_trial(self) -> PipelineSnapshot:
        assert self.context is not None
        if self.clock is not None and self.drain_seconds:
            await self.clock.sleep(self.drain_seconds)
        count = len(self.items)
        stable = self.context.target_rate <= self.stable_through
        completed = count if stable else max(0, count - 1)
        tenant_ids = tuple(
            f"tenant-{slot}" for slot in range(self.topology.tenants)
        )
        installation_ids = tuple(
            f"tenant-{tenant}:installation-{installation}"
            for tenant in range(self.topology.tenants)
            for installation in range(
                self.topology.installations_per_tenant
            )
        )
        replica_ids = tuple(
            f"replica-{slot}" for slot in range(self.topology.replicas)
        )
        replica_counts = [
            count // len(replica_ids)
            + (1 if slot < count % len(replica_ids) else 0)
            for slot in range(len(replica_ids))
        ]
        if self.inactive_replica:
            replica_counts[0] += replica_counts[-1]
            replica_counts[-1] = 0
        ledger = ",".join(item.event_id for item in self.items).encode()
        return PipelineSnapshot(
            offered_items=count,
            accepted_items=count,
            expected_observations=count,
            raw_s3_objects=count,
            raw_kafka_records=count,
            normalized_records=completed,
            observations=completed,
            t1_triggers=completed,
            unique_observation_identities=completed,
            unique_t1_observation_ids=completed,
            raw_bytes=count * 100,
            normalized_bytes=completed * 80,
            provider_requests=count,
            quota_units=float(count),
            unexpected_duplicates=0,
            cross_tenant_leaks=0,
            cursor_checks=max(1, count),
            cursor_consistency_errors=0,
            cooldown_violations=0,
            failed_requests=0,
            dlq_entries=0,
            raw_kafka_lag=0 if stable else 1,
            normalized_kafka_lag=0,
            observation_to_t1_lag=0,
            peak_backlog=0 if stable else 1,
            raw_latency=_latency(count, 4),
            normalized_latency=_latency(completed, 7),
            observation_latency=_latency(completed),
            t1_latency=_latency(completed, 12),
            tenant_ids=tenant_ids,
            installation_ids=installation_ids,
            replica_ids=replica_ids,
            replica_processed_items=tuple(
                (replica_id, replica_counts[slot])
                for slot, replica_id in enumerate(replica_ids)
            ),
            event_ledger_sha256=hashlib.sha256(ledger).hexdigest(),
            cursor_ledger_sha256=hashlib.sha256(
                b"cursor:" + ledger
            ).hexdigest(),
            cpu_percent=25.0,
            memory_bytes=1_024,
        )

    async def close(self) -> None:
        self.closed = True


def _diagnostic_config() -> PipelineLoadRunConfig:
    return PipelineLoadRunConfig(
        timing=PipelineLoadTiming(
            warmup_seconds=4,
            step_seconds=4,
            validation_seconds=4,
            soak_seconds=4,
        ),
        initial_rate=2,
        maximum_offered_rate=5,
        maximum_in_flight=4,
        release=False,
    )


def _quota(
    *,
    verified_at: datetime = NOW,
    rate: float = 4,
) -> VerifiedQuotaConfiguration:
    return VerifiedQuotaConfiguration(
        source_id="slack",
        constraints=(
            QuotaConstraint(
                limit_id="slack-method-workspace",
                scope="app/workspace/method",
                units_per_item=1,
                steady_units=rate,
                steady_window_seconds=1,
                burst_units=rate,
                burst_window_seconds=1,
                evidence_uri="https://api.slack.com/docs/rate-limits",
                verified_at=verified_at,
            ),
        ),
    )


def _factory(
    adapters: list[_Adapter],
    *,
    stable_through: float,
    clock: _Clock | None = None,
    drain_seconds: float = 0.0,
    inactive_replica: bool = False,
):
    def build(
        infrastructure,
        source_id,
        mode,
        workload,
        topology,
        _quota_configuration,
    ):
        adapter = _Adapter(
            infrastructure,
            source_id,
            mode,
            workload,
            topology,
            stable_through=stable_through,
            clock=clock,
            drain_seconds=drain_seconds,
            inactive_replica=inactive_replica,
        )
        adapters.append(adapter)
        return adapter

    return build


def _rehash(artifact: dict[str, object]) -> None:
    unhashed = dict(artifact)
    unhashed.pop("artifact_sha256", None)
    rendered = (
        json.dumps(
            unhashed,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()
    artifact["artifact_sha256"] = hashlib.sha256(rendered).hexdigest()


def test_infrastructure_resolution_is_loopback_and_credential_sealed() -> None:
    infrastructure, error = resolve_isolated_pipeline_infrastructure(
        _environment()
    )
    assert error is None
    assert infrastructure is not None
    assert infrastructure.descriptor["loopback_only"] is True
    rendered = json.dumps(infrastructure.descriptor)
    assert "secret" not in rendered
    assert "certifier" not in rendered

    remote = _environment()
    remote[PIPELINE_DATABASE_ENV] = (
        "postgresql://certifier:secret@db.example.com/certification"
    )
    assert resolve_isolated_pipeline_infrastructure(remote) == (
        None,
        "isolated_infrastructure_rejected",
    )


async def test_provider_safe_fails_closed_without_verified_quota() -> None:
    adapters: list[_Adapter] = []
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory(adapters, stable_through=4),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["state"] == "blocked"
    assert artifact["reason_code"] == "verified_quota_configuration_absent"
    assert adapters == []
    validate_pipeline_load_artifact(artifact)


async def test_runner_fails_closed_without_infrastructure_or_adapter() -> None:
    adapters: list[_Adapter] = []
    no_infrastructure = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env={},
        adapter_factory=_factory(adapters, stable_through=4),
        quota=_quota(),
        config=_diagnostic_config(),
        clock=_Clock(),
    )
    no_adapter = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=None,
        quota=_quota(),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert no_infrastructure["state"] == "blocked"
    assert (
        no_infrastructure["reason_code"]
        == "isolated_infrastructure_not_supplied"
    )
    assert no_adapter["state"] == "blocked"
    assert no_adapter["reason_code"] == "exact_pipeline_adapter_absent"
    assert adapters == []
    validate_pipeline_load_artifact(no_infrastructure)
    validate_pipeline_load_artifact(no_adapter)


async def test_exact_pipeline_search_measures_every_layer_and_topology() -> None:
    adapters: list[_Adapter] = []
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="fyralis_ceiling",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory(adapters, stable_through=4),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["schema_version"] == PIPELINE_LOAD_ARTIFACT_SCHEMA_VERSION
    assert artifact["state"] == "diagnostic"
    assert artifact["promotion_eligible"] is False
    assert 3.8 <= artifact["maximum_stable_rate"] <= 4.0
    phases = [trial["phase"] for trial in artifact["trials"]]
    assert phases[0] == "warmup"
    assert "step" in phases
    assert "binary_search" in phases
    assert phases[-2:] == ["validation", "soak"]
    for trial in artifact["trials"]:
        if not trial["stable"]:
            continue
        metrics = trial["metrics"]
        assert metrics["tenant_count"] == 2
        assert metrics["installation_count"] == 4
        assert metrics["replica_count"] == 2
        assert metrics["participating_replica_count"] == 2
        assert metrics["raw_s3_objects"] == metrics["raw_kafka_records"]
        assert metrics["observations"] == metrics["t1_triggers"]
        assert metrics["p99_raw_latency_ms"] == 4
        assert metrics["p99_normalized_latency_ms"] == 7
        assert metrics["p99_observation_latency_ms"] == 10
        assert metrics["p99_t1_latency_ms"] == 12
        assert metrics["missing_records"] == 0
        assert metrics["unexpected_duplicates"] == 0
        assert metrics["cursor_consistency_errors"] == 0
        assert set(trial["operation_counts"]) == {
            "slack.live_ingress",
            "slack.historical_fetch",
        }
        assert (
            max(trial["operation_counts"].values())
            - min(trial["operation_counts"].values())
            <= 1
        )
    assert adapters[0].closed is True
    validate_pipeline_load_artifact(artifact)


async def test_provider_safe_reaches_evidence_backed_quota_envelope() -> None:
    adapters: list[_Adapter] = []
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory(adapters, stable_through=4),
        quota=_quota(),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["state"] == "diagnostic"
    assert artifact["maximum_stable_rate"] == 4
    assert artifact["quota"]["modeled_maximum_rate"] == 4
    assert artifact["boundary"]["quota_mode"] == "strict"
    validate_pipeline_load_artifact(artifact)


async def test_stale_quota_is_blocked_before_adapter_creation() -> None:
    adapters: list[_Adapter] = []
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory(adapters, stable_through=4),
        quota=_quota(verified_at=NOW - timedelta(days=31)),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["state"] == "blocked"
    assert artifact["reason_code"] == "verified_quota_configuration_rejected"
    assert adapters == []


async def test_unstable_warmup_fails_without_inventing_a_rate() -> None:
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="fyralis_ceiling",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory([], stable_through=1),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["state"] == "failed"
    assert artifact["maximum_stable_rate"] is None
    assert artifact["promotion_eligible"] is False
    validate_pipeline_load_artifact(artifact)


async def test_release_configuration_never_promotes_an_injected_clock() -> None:
    config = PipelineLoadRunConfig(
        timing=PipelineLoadTiming(
            warmup_seconds=120,
            step_seconds=120,
            validation_seconds=900,
            soak_seconds=3_600,
        ),
        initial_rate=0.1,
        maximum_offered_rate=0.1,
        release=True,
    )
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory([], stable_through=1),
        quota=_quota(rate=0.1),
        config=config,
        clock=_Clock(),
    )

    assert artifact["state"] == "diagnostic"
    assert artifact["clock"] == "injected_test_clock"
    assert artifact["promotion_eligible"] is False
    assert [trial["duration_seconds"] for trial in artifact["trials"]] == [
        120,
        900,
        3_600,
    ]


async def test_release_requires_all_eight_topology_lanes() -> None:
    config = PipelineLoadRunConfig(
        initial_rate=0.01,
        maximum_offered_rate=0.01,
    )
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="fyralis_ceiling",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory([], stable_through=1),
        config=config,
        clock=_Clock(),
    )

    assert artifact["state"] == "failed"
    assert "enough items to cover every lane" in artifact["claim_boundary"]


async def test_artifact_semantics_reject_rehashed_count_tampering(
    tmp_path: Path,
) -> None:
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="fyralis_ceiling",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory([], stable_through=4),
        config=_diagnostic_config(),
        clock=_Clock(),
    )
    write_pipeline_load_artifact(tmp_path / "load.json", artifact)
    assert (tmp_path / "load.json").is_file()

    tampered = json.loads(json.dumps(artifact))
    validation = next(
        trial
        for trial in tampered["trials"]
        if trial["phase"] == "validation"
    )
    validation["metrics"]["observations"] -= 1
    _rehash(tampered)
    with pytest.raises(
        PipelineLoadArtifactError,
        match="trial (throughput differs|stable trial has inconsistent)",
    ):
        validate_pipeline_load_artifact(tampered)


async def test_ceiling_mode_rejects_a_stable_safety_cap() -> None:
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="fyralis_ceiling",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory([], stable_through=100),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["state"] == "failed"
    assert artifact["maximum_stable_rate"] is None
    assert "ceiling was not found before the safety cap" in str(
        artifact["claim_boundary"]
    )
    assert not any(
        trial["phase"] in {"validation", "soak"}
        for trial in artifact["trials"]
    )
    validate_pipeline_load_artifact(artifact)


async def test_throughput_uses_wall_elapsed_time_including_drain() -> None:
    clock = _Clock()
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory(
            [],
            stable_through=4,
            clock=clock,
            drain_seconds=2,
        ),
        quota=_quota(),
        config=_diagnostic_config(),
        clock=clock,
    )

    assert artifact["state"] == "failed"
    warmup = artifact["trials"][0]
    metrics = warmup["metrics"]
    assert metrics["scheduled_elapsed_seconds"] == 4
    assert metrics["wall_elapsed_seconds"] == 6
    assert metrics["offered_items_per_second"] == pytest.approx(8 / 6)
    assert metrics["end_to_end_duration_ratio"] == 1.5
    assert metrics["offered_rate_achievement_ratio"] == pytest.approx(2 / 3)
    assert "end-to-end offered rate is below 90% of target" in warmup["failures"]
    validate_pipeline_load_artifact(artifact)


async def test_stable_trial_requires_terminal_replica_participation() -> None:
    artifact = await run_pipeline_load(
        source_id="slack",
        mode="provider_safe",
        workload=_workload(),
        ambient_env=_environment(),
        adapter_factory=_factory(
            [],
            stable_through=4,
            inactive_replica=True,
        ),
        quota=_quota(),
        config=_diagnostic_config(),
        clock=_Clock(),
    )

    assert artifact["state"] == "failed"
    assert artifact["maximum_stable_rate"] is None
    assert (
        "actual replica participation differs"
        in artifact["trials"][0]["failures"]
    )
    validate_pipeline_load_artifact(artifact)


async def test_workload_kind_and_operation_mix_are_boundary_identity() -> None:
    live = DeclaredPipelineWorkload(
        kind="live",
        operation_mix=("slack.live_ingress",),
    )
    historical = DeclaredPipelineWorkload(
        kind="historical",
        operation_mix=("slack.historical_fetch",),
    )
    artifacts = [
        await run_pipeline_load(
            source_id="slack",
            mode="provider_safe",
            workload=workload,
            ambient_env=_environment(),
            adapter_factory=_factory([], stable_through=4),
            quota=_quota(),
            config=_diagnostic_config(),
            clock=_Clock(),
        )
        for workload in (live, historical)
    ]

    assert [artifact["workload"]["kind"] for artifact in artifacts] == [
        "live",
        "historical",
    ]
    assert (
        artifacts[0]["boundary"]["operation_mix_sha256"]
        != artifacts[1]["boundary"]["operation_mix_sha256"]
    )
    for artifact in artifacts:
        validate_pipeline_load_artifact(artifact)


def test_release_config_rejects_short_or_nonexact_runs() -> None:
    with pytest.raises(ValueError, match="release runs require"):
        PipelineLoadRunConfig(
            timing=PipelineLoadTiming(
                warmup_seconds=1,
                step_seconds=1,
                validation_seconds=1,
                soak_seconds=1,
            ),
        )
    with pytest.raises(ValueError, match="release runs require"):
        PipelineLoadRunConfig(
            topology=replace(PipelineLoadTopology(), replicas=1),
        )
