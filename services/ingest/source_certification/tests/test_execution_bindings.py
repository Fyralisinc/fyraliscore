from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.generate_source_certification_execution_bindings import (
    build_execution_bindings,
    render_binding,
    write_execution_bindings,
)
from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.distributed_transport_diagnostic import (
    DISTRIBUTED_TRANSPORT_REDIS_ENV,
)
from services.ingest.source_certification.execution_driver import (
    declared_execution_plan_sha256,
)
from services.ingest.source_certification.producer import (
    EvidenceProducerError,
    StageCommand,
    _command_environment,
    load_execution_binding,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


REPO_ROOT = Path(__file__).resolve().parents[4]
COMMITTED_BINDING_DIR = (
    REPO_ROOT
    / "services"
    / "ingest"
    / "source_certification"
    / "execution_bindings"
)


def test_committed_execution_bindings_cover_exact_catalog_and_are_current() -> None:
    files = tuple(
        sorted(path.name for path in COMMITTED_BINDING_DIR.glob("*.json"))
    )

    assert files == tuple(
        sorted(f"{source_id}.json" for source_id in CANONICAL_SOURCE_IDS)
    )
    assert len(files) == 27
    for source_id in CANONICAL_SOURCE_IDS:
        spec = SOURCE_CERTIFICATION_CATALOG[source_id]
        binding = load_execution_binding(
            COMMITTED_BINDING_DIR / f"{source_id}.json",
            repo_root=REPO_ROOT,
            spec=spec,
        )
        assert binding.source_id == source_id
        assert binding.spec_hash == spec.declaration_hash()
        for stage in (
            "local_correctness",
            "load",
            "fault_recovery",
            "canary",
        ):
            command = binding.stages[stage]
            assert command is not None
            assert source_id in command.argv
            plan_index = command.argv.index("--plan-sha256")
            assert command.argv[plan_index + 1] == (
                declared_execution_plan_sha256(source_id)
            )
            if stage == "canary":
                assert command.credential_env == (
                    spec.canary.credential_env_prefix,
                )
            else:
                assert command.credential_env == ()


def test_generator_check_accepts_committed_directory() -> None:
    assert write_execution_bindings(
        COMMITTED_BINDING_DIR,
        check=True,
    ) == ()


def test_generator_emits_exact_catalog_without_parallel_source_list() -> None:
    generated = build_execution_bindings()

    assert tuple(generated) == CANONICAL_SOURCE_IDS
    assert len(generated) == 27
    assert all(
        generated[source_id]["spec_hash"]
        == SOURCE_CERTIFICATION_CATALOG[source_id].declaration_hash()
        for source_id in CANONICAL_SOURCE_IDS
    )


def test_spec_hash_drift_is_rejected(tmp_path: Path) -> None:
    source_id = "slack"
    value = build_execution_bindings()[source_id]
    value["spec_hash"] = "0" * 64
    path = tmp_path / f"{source_id}.json"
    path.write_text(render_binding(value), encoding="utf-8")

    with pytest.raises(EvidenceProducerError, match="spec_hash is stale"):
        load_execution_binding(
            path,
            repo_root=tmp_path,
            spec=SOURCE_CERTIFICATION_CATALOG[source_id],
        )


def test_check_rejects_unknown_or_stale_files(tmp_path: Path) -> None:
    write_execution_bindings(tmp_path, check=False)
    slack_path = tmp_path / "slack.json"
    slack = json.loads(slack_path.read_text(encoding="utf-8"))
    slack["spec_hash"] = "f" * 64
    slack_path.write_text(json.dumps(slack), encoding="utf-8")
    (tmp_path / "not-a-source.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="unexpected:not-a-source.json"):
        write_execution_bindings(tmp_path, check=True)


def test_non_secret_optional_quota_budget_reaches_load_driver(
    tmp_path: Path,
) -> None:
    quota_json = '{"slack":{"evidence_uri":"https://example.test/quota"}}'
    selected, presence = _command_environment(
        StageCommand(
            argv=("driver",),
            timeout_seconds=30,
            required_env=(),
        ),
        ambient={"FYRALIS_PROVIDER_QUOTAS_JSON": quota_json},
        source_id="slack",
        stage="load",
        result_path=tmp_path / "result.json",
        artifact_dir=tmp_path / "artifacts",
        commit_sha="a" * 40,
    )

    assert selected["FYRALIS_PROVIDER_QUOTAS_JSON"] == quota_json
    assert presence == {}


def test_optional_isolated_redis_reaches_stage_without_becoming_required(
    tmp_path: Path,
) -> None:
    redis_url = "redis://diagnostic-user:diagnostic-password@redis:6379/15"
    selected, presence = _command_environment(
        StageCommand(
            argv=("driver",),
            timeout_seconds=30,
            required_env=(),
        ),
        ambient={DISTRIBUTED_TRANSPORT_REDIS_ENV: redis_url},
        source_id="slack",
        stage="fault_recovery",
        result_path=tmp_path / "result.json",
        artifact_dir=tmp_path / "artifacts",
        commit_sha="a" * 40,
    )

    assert selected[DISTRIBUTED_TRANSPORT_REDIS_ENV] == redis_url
    assert presence == {}

    local_selected, _local_presence = _command_environment(
        StageCommand(
            argv=("driver",),
            timeout_seconds=30,
            required_env=(),
        ),
        ambient={DISTRIBUTED_TRANSPORT_REDIS_ENV: redis_url},
        source_id="slack",
        stage="local_correctness",
        result_path=tmp_path / "local-result.json",
        artifact_dir=tmp_path / "local-artifacts",
        commit_sha="a" * 40,
    )
    assert DISTRIBUTED_TRANSPORT_REDIS_ENV not in local_selected
