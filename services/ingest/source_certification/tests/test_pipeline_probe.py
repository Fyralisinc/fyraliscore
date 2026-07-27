from __future__ import annotations

import os

import pytest

from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_ACK_ENV,
    PIPELINE_ACK_VALUE,
    PIPELINE_DATABASE_ENV,
    PIPELINE_ENV_NAMES,
    PIPELINE_KAFKA_ENV,
    PIPELINE_DATA_PLANE_SCENARIO_IDS,
    PIPELINE_S3_BUCKET_ENV,
    PIPELINE_S3_ENDPOINT_ENV,
    PIPELINE_SCENARIO_IDS,
    pipeline_scenario_ids_for_source,
    PipelineProbeConfig,
    PipelineProbeError,
    _unique_history_scenarios,
    run_pipeline_probe,
)
from services.ingest.source_certification.tests.pipeline_test_fixtures import (
    passing_pipeline_probe,
)
from services.ingest.synthetic.backfill_harness.harness import (
    BackfillHarness,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS


def _environment() -> dict[str, str]:
    return {
        PIPELINE_ACK_ENV: PIPELINE_ACK_VALUE,
        PIPELINE_DATABASE_ENV: (
            "postgresql://certifier:secret@127.0.0.1:55444/certification"
        ),
        PIPELINE_KAFKA_ENV: "127.0.0.1:59092",
        PIPELINE_S3_ENDPOINT_ENV: "http://127.0.0.1:5601",
        PIPELINE_S3_BUCKET_ENV: "fyralis-certification-raw",
    }


def _passing_pipeline_result(source_id: str) -> dict[str, object]:
    return passing_pipeline_probe(source_id)


def test_harness_exposes_exact_run_scoped_consumer_groups() -> None:
    harness = BackfillHarness(pool=object(), scenarios=[])  # type: ignore[arg-type]

    groups = harness.consumer_group_ids

    assert set(groups) == {"raw", "normalized"}
    assert groups["raw"].startswith("x3-normalizer-")
    assert groups["normalized"].startswith("x3-observation-writer-")
    assert groups["raw"].removeprefix("x3-normalizer-") == (
        groups["normalized"].removeprefix("x3-observation-writer-")
    )


def test_pipeline_scenario_boundary_is_history_capability_aware() -> None:
    for definition in SOURCE_DEFINITIONS:
        expected = (
            PIPELINE_SCENARIO_IDS
            if definition.history is not None
            else PIPELINE_DATA_PLANE_SCENARIO_IDS
        )
        assert pipeline_scenario_ids_for_source(definition.source_id) == expected


@pytest.mark.parametrize(
    "source_id",
    [
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is not None
    ],
)
def test_all_history_probes_build_exact_two_by_two_scenario_topology(
    source_id: str,
) -> None:
    scenarios = _unique_history_scenarios(source_id)

    assert len(scenarios) == 4
    tenant_slugs = {scenario.tenant_slug for scenario in scenarios}
    assert len(tenant_slugs) == 2
    for tenant_slug in tenant_slugs:
        siblings = [
            scenario
            for scenario in scenarios
            if scenario.tenant_slug == tenant_slug
        ]
        assert len(siblings) == 2
        assert len(
            {
                scenario.resolved_installation_key
                for scenario in siblings
            },
        ) == 2
        assert all(
            scenario.expected_observation_count > 0
            for scenario in siblings
        )


async def test_absent_pipeline_infrastructure_is_truthfully_blocked() -> None:
    result = await run_pipeline_probe(
        source_id="slack",
        ambient_env={},
    )

    assert result["state"] == "blocked"
    assert result["certified_scenarios"] == []
    assert result["configuration"]["reason_code"] == (
        "isolated_infrastructure_not_supplied"
    )
    assert set(
        result["configuration"]["required_environment_names"],
    ) == PIPELINE_ENV_NAMES


async def test_partial_pipeline_environment_is_blocked_without_values() -> None:
    result = await run_pipeline_probe(
        source_id="slack",
        ambient_env={PIPELINE_ACK_ENV: PIPELINE_ACK_VALUE},
    )

    assert result["state"] == "blocked"
    assert result["configuration"]["reason_code"] == (
        "isolated_infrastructure_incomplete"
    )
    assert result["configuration"]["credential_values_recorded"] is False
    assert "secret" not in str(result)


async def test_non_loopback_pipeline_endpoint_is_rejected() -> None:
    environment = _environment()
    environment[PIPELINE_KAFKA_ENV] = "kafka.example.com:9092"

    result = await run_pipeline_probe(
        source_id="slack",
        ambient_env=environment,
    )

    assert result["state"] == "blocked"
    assert result["configuration"]["reason_code"] == (
        "isolated_infrastructure_rejected"
    )
    assert "loopback" in result["configuration"]["reason"]


async def test_exact_pipeline_environment_executes_and_restores_process_env(
    monkeypatch,
) -> None:
    environment = _environment()
    monkeypatch.setenv("DATABASE_URL", "preserve-me")
    observed: dict[str, str | None] = {}

    async def _executor(
        *,
        source_id: str,
        config: PipelineProbeConfig,
    ) -> dict[str, object]:
        observed["source_id"] = source_id
        observed["database_url"] = os.environ.get("DATABASE_URL")
        observed["kafka"] = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
        observed["s3"] = os.environ.get("S3_ENDPOINT_URL")
        assert config.s3_raw_bucket == "fyralis-certification-raw"
        return _passing_pipeline_result(source_id)

    result = await run_pipeline_probe(
        source_id="slack",
        ambient_env=environment,
        executor=_executor,
    )

    assert result["state"] == "passed"
    assert set(result["certified_scenarios"]) == PIPELINE_SCENARIO_IDS
    assert observed == {
        "source_id": "slack",
        "database_url": environment[PIPELINE_DATABASE_ENV],
        "kafka": environment[PIPELINE_KAFKA_ENV],
        "s3": environment[PIPELINE_S3_ENDPOINT_ENV],
    }
    assert os.environ["DATABASE_URL"] == "preserve-me"
    serialized = str(result)
    assert "certifier:secret" not in serialized
    assert environment[PIPELINE_DATABASE_ENV] not in serialized


async def test_pipeline_replay_proof_rejects_counter_or_hash_drift() -> None:
    environment = _environment()

    async def _counter_drift(
        *,
        source_id: str,
        config: PipelineProbeConfig,
    ) -> dict[str, object]:
        del config
        result = _passing_pipeline_result(source_id)
        result["pipeline"]["tenant_pipelines"][0]["replay"][  # type: ignore[index]
            "observation_count_after"
        ] = 6
        return result

    with pytest.raises(
        PipelineProbeError,
        match="Observation identity changed",
    ):
        await run_pipeline_probe(
            source_id="slack",
            ambient_env=environment,
            executor=_counter_drift,
        )

    async def _hash_drift(
        *,
        source_id: str,
        config: PipelineProbeConfig,
    ) -> dict[str, object]:
        del config
        result = _passing_pipeline_result(source_id)
        result["pipeline"]["tenant_pipelines"][0]["replay"][  # type: ignore[index]
            "observation_identity_set_sha256_after"
        ] = "c" * 64
        return result

    with pytest.raises(
        PipelineProbeError,
        match="Observation identity changed",
    ):
        await run_pipeline_probe(
            source_id="slack",
            ambient_env=environment,
            executor=_hash_drift,
        )


async def test_history_pipeline_topology_fails_closed_on_replica_or_tenant_drift() -> None:
    environment = _environment()

    async def _replica_drift(
        *,
        source_id: str,
        config: PipelineProbeConfig,
    ) -> dict[str, object]:
        del config
        result = _passing_pipeline_result(source_id)
        result["pipeline"]["topology"][  # type: ignore[index]
            "participating_oauth_replicas"
        ] = 1
        return result

    with pytest.raises(
        PipelineProbeError,
        match="participating_oauth_replicas must equal 2",
    ):
        await run_pipeline_probe(
            source_id="slack",
            ambient_env=environment,
            executor=_replica_drift,
        )

    async def _tenant_count_drift(
        *,
        source_id: str,
        config: PipelineProbeConfig,
    ) -> dict[str, object]:
        del config
        result = _passing_pipeline_result(source_id)
        result["pipeline"]["topology"]["tenants"][0][  # type: ignore[index]
            "observed_observation_count"
        ] = 1
        return result

    with pytest.raises(
        PipelineProbeError,
        match="per-tenant Observation count",
    ):
        await run_pipeline_probe(
            source_id="slack",
            ambient_env=environment,
            executor=_tenant_count_drift,
        )


async def test_whatsapp_pipeline_keeps_topology_scenarios_blocked() -> None:
    result = await run_pipeline_probe(
        source_id="whatsapp",
        ambient_env=_environment(),
        executor=lambda **_kwargs: _passing_pipeline_result("whatsapp"),
    )

    assert result["state"] == "passed"
    assert "exact_tenant_and_installation_resolution" not in result[
        "certified_scenarios"
    ]
    assert "two_replica_cross_tenant_isolation" not in result[
        "certified_scenarios"
    ]


def test_pipeline_descriptor_excludes_database_credentials() -> None:
    first = PipelineProbeConfig(
        database_url=(
            "postgresql://certifier:first-secret@127.0.0.1:55444/"
            "certification"
        ),
        kafka_bootstrap_servers="127.0.0.1:59092",
        s3_endpoint_url="http://127.0.0.1:5601",
        s3_raw_bucket="fyralis-certification-raw",
    )
    second = PipelineProbeConfig(
        database_url=(
            "postgresql://other-user:second-secret@127.0.0.1:55444/"
            "certification"
        ),
        kafka_bootstrap_servers=first.kafka_bootstrap_servers,
        s3_endpoint_url=first.s3_endpoint_url,
        s3_raw_bucket=first.s3_raw_bucket,
    )

    assert first.descriptor == second.descriptor
    assert first.descriptor["credentials_included_in_binding"] is False


async def test_pipeline_runtime_failure_is_redacted_and_fails_closed() -> None:
    environment = _environment()

    async def _executor(**_kwargs) -> dict[str, object]:
        raise RuntimeError(
            f"could not connect to {environment[PIPELINE_DATABASE_ENV]}",
        )

    result = await run_pipeline_probe(
        source_id="slack",
        ambient_env=environment,
        executor=_executor,
    )

    assert result["state"] == "failed"
    assert result["certified_scenarios"] == []
    assert result["error_type"] == "RuntimeError"
    assert "[redacted-endpoint]" in result["error"]
    assert "certifier:secret" not in str(result)
