from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.execution_driver import (
    declared_execution_plan_sha256,
    run_stage,
)
from services.ingest.source_certification.io import (
    load_certification_input,
    write_certification_input,
)
from services.ingest.source_certification.models import (
    CanaryOperationResult,
    CanaryResult,
    CertificationInput,
    ScenarioResult,
    SuiteResult,
)
from services.ingest.source_certification.pipeline_probe import (
    PIPELINE_SCENARIO_IDS,
)
from services.ingest.source_certification.producer import (
    BINDING_SCHEMA_VERSION,
    CommandOutcome,
    EvidenceProducerError,
    PRODUCER_SCHEMA_VERSION,
    PRODUCER_SHARD_SCHEMA_VERSION,
    RepositoryIdentity,
    StageCommand,
    _command_environment,
    deterministic_source_shard,
    load_secret_environment_bundle,
    load_execution_binding,
    merge_evidence_shards,
    produce_evidence,
    verify_evidence_bundle,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS
from services.ingest.source_certification.tests.pipeline_test_fixtures import (
    passing_pipeline_probe,
)


COMMIT_SHA = "a" * 40


def test_pipeline_infrastructure_is_forwarded_only_to_local_stage(
    tmp_path: Path,
) -> None:
    command = StageCommand(
        argv=("python", "certify.py"),
        timeout_seconds=60,
        required_env=(),
        credential_env=(),
    )
    ambient = {
        "FYRALIS_CERTIFICATION_DATABASE_URL": (
            "postgresql://certifier:secret@127.0.0.1:55444/certification"
        ),
        "FYRALIS_CERTIFICATION_ISOLATED_INFRA_ACK": (
            "dedicated-loopback-data-plane-v1"
        ),
        "FYRALIS_CERTIFICATION_KAFKA_BOOTSTRAP_SERVERS": (
            "127.0.0.1:59092"
        ),
        "FYRALIS_CERTIFICATION_S3_ENDPOINT_URL": (
            "http://127.0.0.1:5601"
        ),
        "FYRALIS_CERTIFICATION_S3_RAW_BUCKET": "fyralis-certification-raw",
    }
    common = {
        "command": command,
        "ambient": ambient,
        "source_id": "slack",
        "result_path": tmp_path / "result.json",
        "artifact_dir": tmp_path / "artifacts",
        "commit_sha": COMMIT_SHA,
    }

    local, _ = _command_environment(
        stage="local_correctness",
        **common,
    )
    load, _ = _command_environment(stage="load", **common)

    assert all(local[name] == value for name, value in ambient.items())
    assert not (set(ambient) & set(load))


def _identity() -> RepositoryIdentity:
    return RepositoryIdentity(
        commit_sha=COMMIT_SHA,
        head_sha=COMMIT_SHA,
        clean=True,
        status_sha256=hashlib.sha256(b"").hexdigest(),
        status_entry_count=0,
    )


def _outcome(returncode: int = 0) -> CommandOutcome:
    now = datetime.now(timezone.utc)
    return CommandOutcome(
        returncode=returncode,
        started_at=now,
        completed_at=now,
        stdout=b"architecture clean\n",
        stderr=b"",
    )


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    script = repo / "scripts/check_source_architecture_ratchet.py"
    script.parent.mkdir(parents=True)
    script.write_text("# test ratchet\n", encoding="utf-8")
    return repo


def _null_binding(source_id: str) -> dict[str, object]:
    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    return {
        "schema_version": BINDING_SCHEMA_VERSION,
        "source_id": source_id,
        "spec_hash": spec.declaration_hash(),
        "stages": {
            "local_correctness": None,
            "load": None,
            "fault_recovery": None,
            "canary": None,
        },
    }


def _write_binding(
    repo: Path,
    source_id: str,
    value: dict[str, object],
) -> Path:
    path = (
        repo
        / "services/ingest/source_certification/execution_bindings"
        / f"{source_id}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    return path


def _blocked_suites() -> tuple[SuiteResult, ...]:
    return tuple(
        SuiteResult(kind=kind, state="blocked")
        for kind in ("historical", "live", "combined")
    )


def _local_passing_input(source_id: str) -> CertificationInput:
    spec = SOURCE_CERTIFICATION_CATALOG[source_id]
    artifact = "evidence-file:local.json"
    return CertificationInput(
        spec_hash=spec.declaration_hash(),
        local_correctness="passed",
        local_correctness_artifact=artifact,
        scenario_results=tuple(
            ScenarioResult(
                scenario_id=scenario_id,
                state="passed",
                artifact_uri=artifact,
            )
            for scenario_id in spec.required_scenarios
        ),
        provider_safe_suites=_blocked_suites(),
        fyralis_ceiling_suites=_blocked_suites(),
        fault_recovery_suites=_blocked_suites(),
        canary=CanaryResult(
            state="blocked",
            operation_results=tuple(
                CanaryOperationResult(
                    operation_id=operation_id,
                    state="blocked",
                    artifact_uri=artifact,
                )
                for operation_id in spec.canary.required_operations
            ),
        ),
        legacy_reference_count=0,
    )


def _passing_pipeline_probe(source_id: str) -> dict[str, object]:
    return passing_pipeline_probe(source_id)


def test_missing_bindings_emit_exact_truthful_blocked_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo(tmp_path)
    output = tmp_path / "bundle"
    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )
    manifest = produce_evidence(
        repo_root=repo,
        binding_dir=repo / "missing-bindings",
        output_dir=output,
        commit_sha=COMMIT_SHA,
        run_id="blocked-test",
        ambient_env={},
        executor=lambda *_args: _outcome(),
        repository_identity=_identity(),
    )

    assert manifest["state"] == "blocked"
    assert manifest["schema_version"] == PRODUCER_SCHEMA_VERSION
    assert manifest["source_order"] == list(CANONICAL_SOURCE_IDS)
    assert len(list((output / "inputs").glob("*.json"))) == 27
    assert all(
        entry["decision_state"] == "blocked" for entry in manifest["sources"]
    )
    slack = load_certification_input(output / "inputs/slack.json")
    assert slack.local_correctness == "blocked"
    assert slack.canary.state == "blocked"
    assert all(
        operation.state == "blocked"
        for operation in slack.canary.operation_results
    )
    receipt = json.loads(
        (
            output
            / "provenance/receipts/slack/canary/receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["reason_code"] == "source_execution_binding_absent"
    assert receipt["command"] is None

    verified = verify_evidence_bundle(
        repo_root=repo,
        input_dir=output / "inputs",
        provenance_dir=output / "provenance",
        expected_commit_sha=COMMIT_SHA,
        require_complete=False,
    )
    assert verified["verified_sources"] == 27
    with pytest.raises(EvidenceProducerError, match="state must equal passed"):
        verify_evidence_bundle(
            repo_root=repo,
            input_dir=output / "inputs",
            provenance_dir=output / "provenance",
            expected_commit_sha=COMMIT_SHA,
        )


def test_dummy_local_artifact_cannot_create_passing_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo(tmp_path)
    binding = _null_binding("slack")
    binding["stages"]["local_correctness"] = {  # type: ignore[index]
        "argv": ["{python}", "certify.py", "{stage}"],
        "timeout_seconds": 60,
        "required_env": [],
        "credential_env": [],
    }
    _write_binding(repo, "slack", binding)
    invoked: list[str] = []

    def _executor(argv, _cwd, env, _timeout):  # noqa: ANN001
        if "FYRALIS_CERTIFICATION_STAGE" not in env:
            return _outcome()
        invoked.append(env["FYRALIS_CERTIFICATION_STAGE"])
        artifact = Path(env["FYRALIS_CERTIFICATION_ARTIFACT_DIR"]) / "local.json"
        artifact.write_text('{"sanitized":true}\n', encoding="utf-8")
        write_certification_input(
            Path(env["FYRALIS_CERTIFICATION_RESULT_PATH"]),
            _local_passing_input(env["FYRALIS_CERTIFICATION_SOURCE_ID"]),
        )
        return _outcome()

    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )
    output = tmp_path / "bundle"
    produce_evidence(
        repo_root=repo,
        binding_dir=(
            repo / "services/ingest/source_certification/execution_bindings"
        ),
        output_dir=output,
        commit_sha=COMMIT_SHA,
        run_id="local-only",
        ambient_env={},
        executor=_executor,
        repository_identity=_identity(),
    )

    assert invoked == ["local_correctness"]
    supplied = load_certification_input(output / "inputs/slack.json")
    assert supplied.local_correctness == "blocked"
    assert all(result.state == "blocked" for result in supplied.scenario_results)
    assert supplied.canary.state == "blocked"
    assert all(
        result.state == "blocked"
        for result in supplied.canary.operation_results
    )
    receipt = json.loads(
        (
            output
            / "provenance/receipts/slack/local_correctness/receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["state"] == "failed"
    assert receipt["reason_code"] == "command_evidence_rejected"
    assert "typed stage.json" in receipt["reason"]
    assert receipt["result_sha256"] is None
    assert receipt["artifact_sha256"] == {}


def test_typed_partial_idempotency_pass_survives_producer_translation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo(tmp_path)
    source_id = "slack"
    plan_sha = declared_execution_plan_sha256(source_id)
    binding = _null_binding(source_id)
    binding["stages"]["local_correctness"] = {  # type: ignore[index]
        "argv": [
            "{python}",
            "certify.py",
            "--plan-sha256",
            plan_sha,
        ],
        "timeout_seconds": 60,
        "required_env": [],
        "credential_env": [],
    }
    _write_binding(repo, source_id, binding)

    async def _pipeline(**_kwargs) -> dict[str, object]:
        return _passing_pipeline_probe(source_id)

    monkeypatch.setattr(
        "services.ingest.source_certification.execution_driver."
        "run_pipeline_probe",
        _pipeline,
    )
    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )

    def _executor(_argv, _cwd, env, _timeout):  # noqa: ANN001
        if env.get("FYRALIS_CERTIFICATION_STAGE") != "local_correctness":
            return _outcome()
        started = datetime.now(timezone.utc)
        asyncio.run(
            run_stage(
                source_id=source_id,
                stage="local_correctness",
                result_path=Path(
                    env["FYRALIS_CERTIFICATION_RESULT_PATH"],
                ),
                artifact_dir=Path(
                    env["FYRALIS_CERTIFICATION_ARTIFACT_DIR"],
                ),
                ambient_env=env,
                expected_plan_sha256=plan_sha,
            )
        )
        return CommandOutcome(
            returncode=0,
            started_at=started,
            completed_at=datetime.now(timezone.utc),
            stdout=b"",
            stderr=b"",
        )

    output = tmp_path / "bundle"
    manifest = produce_evidence(
        repo_root=repo,
        binding_dir=(
            repo / "services/ingest/source_certification/execution_bindings"
        ),
        output_dir=output,
        commit_sha=COMMIT_SHA,
        run_id="typed-idempotency-pass",
        ambient_env={},
        executor=_executor,
        repository_identity=_identity(),
        shard_index=0,
        shard_count=27,
    )

    assert manifest["state"] == "blocked"
    supplied = load_certification_input(output / "inputs/slack.json")
    passed = {
        result.scenario_id
        for result in supplied.scenario_results
        if result.state == "passed"
    }
    assert passed == PIPELINE_SCENARIO_IDS
    duplicate = next(
        result
        for result in supplied.scenario_results
        if result.scenario_id == "duplicate_delivery_and_idempotency"
    )
    assert duplicate.artifact_uri.startswith(
        "artifact://source-certification-evidence/typed-idempotency-pass/"
    )
    receipt = json.loads(
        (
            output
            / "provenance/receipts/slack/local_correctness/receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["state"] == "blocked"
    assert receipt["reason_code"] == "command_evidence_rejected"
    assert receipt["reason"] == "command result state is blocked"
    assert receipt["artifact_sha256"]


def test_absent_canary_credentials_prevent_command_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo(tmp_path)
    binding = _null_binding("slack")
    binding["stages"]["canary"] = {  # type: ignore[index]
        "argv": ["{python}", "certify.py", "canary"],
        "timeout_seconds": 60,
        "required_env": ["FYRALIS_CANARY_SLACK_BOT_TOKEN"],
        "credential_env": ["FYRALIS_CANARY_SLACK_BOT_TOKEN"],
    }
    _write_binding(repo, "slack", binding)
    executed_stages: list[str] = []

    def _executor(_argv, _cwd, env, _timeout):  # noqa: ANN001
        if stage := env.get("FYRALIS_CERTIFICATION_STAGE"):
            executed_stages.append(stage)
        return _outcome()

    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )
    output = tmp_path / "bundle"
    produce_evidence(
        repo_root=repo,
        binding_dir=(
            repo / "services/ingest/source_certification/execution_bindings"
        ),
        output_dir=output,
        commit_sha=COMMIT_SHA,
        run_id="no-canary-creds",
        ambient_env={},
        executor=_executor,
        repository_identity=_identity(),
    )

    assert executed_stages == []
    receipt = json.loads(
        (
            output
            / "provenance/receipts/slack/canary/receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert (
        receipt["reason_code"]
        == "canary_credentials_or_entitlements_absent"
    )
    assert receipt["required_environment"] == {
        "FYRALIS_CANARY_SLACK_BOT_TOKEN": False
    }
    assert load_certification_input(
        output / "inputs/slack.json"
    ).canary.state == "blocked"


@pytest.mark.parametrize(
    "leak_location",
    ("result", "artifact", "stdout", "stderr"),
)
def test_provided_credential_values_are_discarded_before_receipting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    leak_location: str,
) -> None:
    repo = _repo(tmp_path)
    binding = _null_binding("slack")
    binding["stages"]["canary"] = {  # type: ignore[index]
        "argv": ["{python}", "certify.py", "canary"],
        "timeout_seconds": 60,
        "required_env": ["FYRALIS_CANARY_SLACK_BOT_TOKEN"],
        "credential_env": ["FYRALIS_CANARY_SLACK_BOT_TOKEN"],
    }
    _write_binding(repo, "slack", binding)
    token = "slack-test-token-that-must-never-be-receipted"

    def _executor(_argv, _cwd, env, _timeout):  # noqa: ANN001
        if "FYRALIS_CERTIFICATION_STAGE" not in env:
            return _outcome()
        result = Path(env["FYRALIS_CERTIFICATION_RESULT_PATH"])
        artifact = Path(env["FYRALIS_CERTIFICATION_ARTIFACT_DIR"]) / "stage.json"
        result.write_text(
            json.dumps({"credential_echo": token if leak_location == "result" else ""}),
            encoding="utf-8",
        )
        artifact.write_text(
            json.dumps(
                {
                    "diagnostic": (
                        token if leak_location == "artifact" else "redacted"
                    )
                }
            ),
            encoding="utf-8",
        )
        now = datetime.now(timezone.utc)
        return CommandOutcome(
            returncode=0,
            started_at=now,
            completed_at=now,
            stdout=token.encode() if leak_location == "stdout" else b"",
            stderr=token.encode() if leak_location == "stderr" else b"",
        )

    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )
    output = tmp_path / "bundle"
    produce_evidence(
        repo_root=repo,
        binding_dir=(
            repo / "services/ingest/source_certification/execution_bindings"
        ),
        output_dir=output,
        commit_sha=COMMIT_SHA,
        run_id=f"credential-leak-{leak_location}",
        ambient_env={"FYRALIS_CANARY_SLACK_BOT_TOKEN": token},
        executor=_executor,
        repository_identity=_identity(),
    )

    receipt = json.loads(
        (
            output / "provenance/receipts/slack/canary/receipt.json"
        ).read_text(encoding="utf-8")
    )
    assert receipt["state"] == "failed"
    assert "credential value" in receipt["reason"]
    assert receipt["stdout_sha256"] is None
    assert receipt["stdout_bytes"] == 0
    assert receipt["stderr_sha256"] is None
    assert receipt["stderr_bytes"] == 0
    assert receipt["result_sha256"] is None
    assert receipt["artifact_sha256"] == {}
    assert all(
        token.encode() not in path.read_bytes()
        for path in output.rglob("*")
        if path.is_file()
    )


def test_secret_environment_bundle_requires_exact_mode_and_names(
    tmp_path: Path,
) -> None:
    path = tmp_path / "source-certification-secrets.json"
    path.write_text(
        json.dumps(
            {
                "FYRALIS_CANARY_SLACK_BOT_TOKEN": "test-token-value",
                "FYRALIS_CERTIFICATION_DATABASE_URL": (
                    "postgresql://certifier:password@127.0.0.1/certification"
                ),
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)

    loaded = load_secret_environment_bundle(path, source_id="slack")

    assert loaded["FYRALIS_CANARY_SLACK_BOT_TOKEN"] == "test-token-value"
    path.chmod(0o640)
    with pytest.raises(EvidenceProducerError, match="exactly 0600"):
        load_secret_environment_bundle(path)

    path.chmod(0o600)
    path.write_text(
        json.dumps({"UNSCOPED_PROVIDER_TOKEN": "forbidden-token"}),
        encoding="utf-8",
    )
    with pytest.raises(EvidenceProducerError, match="unsupported environment"):
        load_secret_environment_bundle(path, source_id="slack")

    path.write_text(
        json.dumps(
            {"FYRALIS_CANARY_GITHUB_APP_KEY": "foreign-source-secret"}
        ),
        encoding="utf-8",
    )
    with pytest.raises(EvidenceProducerError, match="unsupported environment"):
        load_secret_environment_bundle(path, source_id="slack")


def test_canary_binding_rejects_non_source_credential_names(
    tmp_path: Path,
) -> None:
    repo = _repo(tmp_path)
    binding = _null_binding("slack")
    binding["stages"]["canary"] = {  # type: ignore[index]
        "argv": ["{python}", "certify.py"],
        "timeout_seconds": 60,
        "required_env": ["SLACK_BOT_TOKEN"],
        "credential_env": ["SLACK_BOT_TOKEN"],
    }
    path = _write_binding(repo, "slack", binding)
    with pytest.raises(EvidenceProducerError, match="FYRALIS_CANARY_SLACK"):
        load_execution_binding(
            path,
            repo_root=repo,
            spec=SOURCE_CERTIFICATION_CATALOG["slack"],
        )


def test_verifier_rejects_tampered_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = _repo(tmp_path)
    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )
    output = tmp_path / "bundle"
    produce_evidence(
        repo_root=repo,
        binding_dir=repo / "missing-bindings",
        output_dir=output,
        commit_sha=COMMIT_SHA,
        run_id="tamper-test",
        ambient_env={},
        executor=lambda *_args: _outcome(),
        repository_identity=_identity(),
    )
    path = output / "inputs/slack.json"
    path.write_text(path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(EvidenceProducerError, match="input checksum differs"):
        verify_evidence_bundle(
            repo_root=repo,
            input_dir=output / "inputs",
            provenance_dir=output / "provenance",
            expected_commit_sha=COMMIT_SHA,
            require_complete=False,
        )


def test_evidence_workflow_uploads_blocked_diagnostics_then_fails_closed() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    producer = (
        repo_root / ".github/workflows/source-certification-evidence.yml"
    ).read_text(encoding="utf-8")
    consumer = (
        repo_root / ".github/workflows/source-certification.yml"
    ).read_text(encoding="utf-8")

    assert "name: Source Certification Evidence" in producer
    assert "runs-on: [self-hosted, linux, source-certification]" in producer
    assert "max-parallel: 3" in producer
    assert "matrix: ${{ fromJSON(needs.plan.outputs.matrix) }}" in producer
    assert "--shard-count 27" in producer
    assert "flock --exclusive --wait 21600" in producer
    assert "source-certification-${{ matrix.source_id }}" in producer
    assert "SOURCE_CERTIFICATION_SECRET_BUNDLE_JSON" in producer
    assert "chmod 0600" in producer
    assert "--secret-bundle" in producer
    assert "--secret-bundle-source" in producer
    assert "GITHUB_ENV" not in producer
    assert "--load-promotion" not in producer
    assert "non-promoting diagnostic" in producer
    assert producer.index(
        "Remove process-local certification secret bundle"
    ) < producer.index("Upload source-isolated bundle")
    assert producer.index("Require exact shard membership and merge") < producer.index(
        "Upload exact certification inputs"
    )
    assert producer.index("Upload merged producer receipts and manifest") < (
        producer.index(
        "Require complete replayable evidence"
        )
    )
    final_command = producer[producer.index("Require complete replayable evidence") :]
    assert "--allow-blocked" not in final_command
    assert "source-certification-provenance" in consumer
    assert consumer.index("Verify producer receipts for the exact commit") < (
        consumer.index("Evaluate and sign the exact release manifest")
    )
    assert "--allow-blocked" not in consumer


def _produce_shards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    shard_count: int = 3,
) -> tuple[Path, tuple[Path, ...]]:
    repo = _repo(tmp_path)
    monkeypatch.setattr(
        "services.ingest.source_certification.producer.inspect_repository",
        lambda *_args, **_kwargs: _identity(),
    )
    shard_dirs = []
    for shard_index in range(shard_count):
        output = tmp_path / f"shard-{shard_index}"
        manifest = produce_evidence(
            repo_root=repo,
            binding_dir=repo / "missing-bindings",
            output_dir=output,
            commit_sha=COMMIT_SHA,
            run_id="shared-workflow-run",
            ambient_env={},
            executor=lambda *_args: _outcome(),
            repository_identity=_identity(),
            shard_index=shard_index,
            shard_count=shard_count,
        )
        assert manifest["schema_version"] == PRODUCER_SHARD_SCHEMA_VERSION
        assert manifest["source_order"] == list(
            deterministic_source_shard(shard_index, shard_count)
        )
        shard_dirs.append(output)
    return repo, tuple(shard_dirs)


def _manifest(path: Path) -> dict[str, object]:
    manifest_path = path / "provenance/producer-manifest.json"
    value = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _write_manifest(path: Path, value: dict[str, object]) -> None:
    (path / "provenance/producer-manifest.json").write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_deterministic_source_shards_merge_into_verifier_compatible_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, shard_dirs = _produce_shards(tmp_path, monkeypatch)
    output = tmp_path / "merged"
    manifest = merge_evidence_shards(
        repo_root=repo,
        shard_dirs=shard_dirs,
        output_dir=output,
        expected_commit_sha=COMMIT_SHA,
    )

    assert manifest["schema_version"] == PRODUCER_SCHEMA_VERSION
    assert manifest["state"] == "blocked"
    assert manifest["source_order"] == list(CANONICAL_SOURCE_IDS)
    assert len(list((output / "inputs").glob("*.json"))) == 27
    assert verify_evidence_bundle(
        repo_root=repo,
        input_dir=output / "inputs",
        provenance_dir=output / "provenance",
        expected_commit_sha=COMMIT_SHA,
        require_complete=False,
    )["verified_sources"] == 27


def test_shard_merge_rejects_missing_and_duplicate_indexes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, shard_dirs = _produce_shards(tmp_path, monkeypatch)
    with pytest.raises(EvidenceProducerError, match="index coverage differs"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs[:-1],
            output_dir=tmp_path / "missing",
            expected_commit_sha=COMMIT_SHA,
        )
    with pytest.raises(EvidenceProducerError, match="duplicate producer shard index"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=(*shard_dirs, shard_dirs[0]),
            output_dir=tmp_path / "duplicate",
            expected_commit_sha=COMMIT_SHA,
        )


def test_shard_merge_rejects_tampered_membership_and_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, shard_dirs = _produce_shards(tmp_path, monkeypatch)
    membership = _manifest(shard_dirs[0])
    membership["source_order"] = list(reversed(membership["source_order"]))
    _write_manifest(shard_dirs[0], membership)
    with pytest.raises(EvidenceProducerError, match="membership is not deterministic"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs,
            output_dir=tmp_path / "membership",
            expected_commit_sha=COMMIT_SHA,
        )

    repo, shard_dirs = _produce_shards(
        tmp_path / "spec-case",
        monkeypatch,
    )
    stale_spec = _manifest(shard_dirs[0])
    sources = stale_spec["sources"]
    assert isinstance(sources, list)
    assert isinstance(sources[0], dict)
    sources[0]["spec_hash"] = "0" * 64
    _write_manifest(shard_dirs[0], stale_spec)
    with pytest.raises(EvidenceProducerError, match="spec hash is stale"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs,
            output_dir=tmp_path / "stale-spec",
            expected_commit_sha=COMMIT_SHA,
        )


def test_shard_merge_rejects_tampered_catalog_and_cross_source_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, shard_dirs = _produce_shards(tmp_path, monkeypatch)
    stale_catalog = _manifest(shard_dirs[0])
    shard = stale_catalog["shard"]
    assert isinstance(shard, dict)
    shard["catalog_sha256"] = "0" * 64
    _write_manifest(shard_dirs[0], stale_catalog)
    with pytest.raises(EvidenceProducerError, match="catalog/spec identity differs"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs,
            output_dir=tmp_path / "stale-catalog",
            expected_commit_sha=COMMIT_SHA,
        )

    repo, shard_dirs = _produce_shards(
        tmp_path / "receipt-case",
        monkeypatch,
    )
    foreign = (
        shard_dirs[0]
        / "provenance/receipts/not-owned-by-shard/local/receipt.json"
    )
    foreign.parent.mkdir(parents=True)
    foreign.write_text("{}\n", encoding="utf-8")
    foreign_manifest = _manifest(shard_dirs[0])
    provenance_files = foreign_manifest["provenance_files"]
    assert isinstance(provenance_files, dict)
    provenance_files[
        "receipts/not-owned-by-shard/local/receipt.json"
    ] = hashlib.sha256(foreign.read_bytes()).hexdigest()
    _write_manifest(shard_dirs[0], foreign_manifest)
    with pytest.raises(EvidenceProducerError, match="not source-isolated"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs,
            output_dir=tmp_path / "foreign-receipt",
            expected_commit_sha=COMMIT_SHA,
        )


def test_shard_merge_rejects_mismatched_run_and_dirty_repository(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, shard_dirs = _produce_shards(tmp_path, monkeypatch)
    mismatched_run = _manifest(shard_dirs[1])
    mismatched_run["run_id"] = "different-workflow-run"
    _write_manifest(shard_dirs[1], mismatched_run)
    with pytest.raises(EvidenceProducerError, match="run IDs differ"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs,
            output_dir=tmp_path / "run-mismatch",
            expected_commit_sha=COMMIT_SHA,
        )

    repo, shard_dirs = _produce_shards(
        tmp_path / "dirty-case",
        monkeypatch,
    )
    dirty = _manifest(shard_dirs[0])
    repository = dirty["repository"]
    assert isinstance(repository, dict)
    repository["final_clean"] = False
    _write_manifest(shard_dirs[0], dirty)
    with pytest.raises(EvidenceProducerError, match="unchanged clean target"):
        merge_evidence_shards(
            repo_root=repo,
            shard_dirs=shard_dirs,
            output_dir=tmp_path / "dirty",
            expected_commit_sha=COMMIT_SHA,
        )
