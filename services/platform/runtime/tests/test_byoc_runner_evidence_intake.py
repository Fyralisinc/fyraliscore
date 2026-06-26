from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerInputs,
    run_byoc_agent_runner,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceSubmissionPayload,
    ByocRunnerEvidenceSubmissionRequest,
    InMemoryByocRunnerEvidenceIntakeStore,
    PostgresByocRunnerEvidenceIntakeStore,
    canonical_runner_evidence_submission_payload,
    digest_runner_evidence_summary,
    model_json_schema_bundle,
    runner_evidence_submission_payload,
    runner_evidence_summary_from_report,
    signed_runner_evidence_submission,
    validate_runner_evidence_submission,
)


ROOT = Path(__file__).resolve().parents[4]
MANIFEST_PATH = ROOT / "deploy/byoc/dataplane.example.yaml"
BUNDLE_NEXT_PATH = ROOT / "deploy/byoc/bootstrap-bundle.next.example.yaml"
INSTALL_TOKEN = "local-install-token-for-runner-evidence-tests"
SIGNING_SECRET = "local-control-plane-intake-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
AGENT_ID = "agt_runnerintake01"
AGENT_VERSION = "2026.06.26-test"
SUBMITTED_AT = datetime(2026, 6, 26, 12, 45, tzinfo=UTC)


class _FakeRunnerEvidencePool:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query: str, *args):
        self.calls.append((query, args))
        if query.lstrip().upper().startswith("INSERT"):
            row = {
                "receipt_id": args[0],
                "deployment_id": args[1],
                "customer_id": args[2],
                "agent_id": args[3],
                "agent_version": args[4],
                "cloud_provider": args[5],
                "region": args[6],
                "control_plane_mode": args[7],
                "evidence_digest": args[8],
                "current_artifact_revision": args[9],
                "desired_revision": args[10],
                "rollout_action": args[11],
                "runner_status": args[12],
                "required_checks_passed": args[13],
                "apply_plan_count": args[14],
                "artifact_verification_count": args[15],
                "digest_pinned_artifact_count": args[16],
                "local_digest_checked_count": args[17],
                "submitted_at": args[18],
                "accepted_at": args[19],
                "stored_scope": args[20],
            }
            self.rows[row["receipt_id"]] = row
            return row
        return self.rows.get(args[0])


async def _runner_payload(
    *,
    nonce: str = "nonce-runner-evidence-001",
) -> ByocRunnerEvidenceSubmissionPayload:
    report = await run_byoc_agent_runner(
        ByocAgentRunnerInputs(
            manifest_path=MANIFEST_PATH,
            install_token=INSTALL_TOKEN,
            agent_id=AGENT_ID,
            agent_version=AGENT_VERSION,
            nonce_prefix="nonce-runner-evidence-contract",
            iterations=1,
            mock_desired_revision="2026.06.26-2",
            mock_config_epoch=3,
            bootstrap_bundle_path=BUNDLE_NEXT_PATH,
            verify_local_bundle_files=True,
            repo_root=ROOT,
            requested_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
            sent_at=datetime(2026, 6, 26, 12, 1, tzinfo=UTC),
        )
    )
    summary = runner_evidence_summary_from_report(report)
    return runner_evidence_submission_payload(
        evidence=summary,
        nonce=nonce,
        submitted_at=SUBMITTED_AT,
    )


async def _runner_request(
    *,
    nonce: str = "nonce-runner-evidence-001",
    signing_secret: str = SIGNING_SECRET,
) -> ByocRunnerEvidenceSubmissionRequest:
    return signed_runner_evidence_submission(
        await _runner_payload(nonce=nonce),
        signing_secret=signing_secret,
        key_ref=SIGNING_KEY_REF,
    )


@pytest.mark.asyncio
async def test_runner_summary_keeps_raw_report_details_out_of_contract() -> None:
    payload = await _runner_payload()
    rendered = payload.model_dump_json()

    assert payload.evidence.runner_status == "pass"
    assert payload.evidence.required_checks_passed is True
    assert payload.evidence.apply_plan_count == 1
    assert payload.evidence.artifact_verification_count == 1
    assert payload.evidence.digest_pinned_artifact_count == 7
    assert payload.evidence.local_digest_checked_count == 1
    assert payload.evidence.stored_scope == "sanitized_agent_metadata_only"
    assert '"checks":' not in rendered
    assert '"iterations":' not in rendered
    assert "required_artifact_roles" not in rendered
    assert "gateway_image" not in rendered
    assert INSTALL_TOKEN not in rendered


@pytest.mark.asyncio
async def test_signed_runner_evidence_verifies_without_serializing_secret() -> None:
    request = await _runner_request()
    payload = await _runner_payload()

    serialized = request.model_dump_json()
    assert SIGNING_SECRET not in serialized
    assert request.signature.key_ref == SIGNING_KEY_REF
    assert canonical_runner_evidence_submission_payload(payload) == (
        canonical_runner_evidence_submission_payload(payload)
    )
    assert validate_runner_evidence_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []


@pytest.mark.asyncio
async def test_runner_evidence_signature_detects_tampering() -> None:
    request = await _runner_request()
    tampered = ByocRunnerEvidenceSubmissionRequest.model_validate(
        {
            **request.model_dump(mode="json"),
            "nonce": "nonce-runner-evidence-002",
        }
    )

    violations = validate_runner_evidence_submission(
        tampered,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert [violation.code for violation in violations] == ["invalid_signature"]


@pytest.mark.asyncio
async def test_runner_evidence_rejects_raw_extra_field() -> None:
    data = (await _runner_request()).model_dump(mode="json")
    data["evidence"]["checks"] = [{"details": "https://gateway.customer.internal"}]

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ByocRunnerEvidenceSubmissionRequest.model_validate(data)


@pytest.mark.asyncio
async def test_runner_evidence_privacy_scan_rejects_token_marker() -> None:
    data = (await _runner_request()).model_dump(mode="json")
    data["nonce"] = "nonce-runner-evidence-token=secret"
    request = ByocRunnerEvidenceSubmissionRequest.model_validate(data)

    violations = validate_runner_evidence_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert "customer_data_marker_forbidden" in {
        violation.code for violation in violations
    }


@pytest.mark.asyncio
async def test_runner_evidence_store_records_only_sanitized_receipt_metadata() -> None:
    store = InMemoryByocRunnerEvidenceIntakeStore()
    request = await _runner_request()

    receipt = await store.put(request, accepted_at=SUBMITTED_AT)
    record = await store.get(receipt.receipt_id)

    assert record is not None
    assert record.receipt.evidence_digest == digest_runner_evidence_summary(
        request.evidence
    )
    assert record.receipt.stored_scope == "sanitized_metadata_only"
    rendered = json.dumps(record.model_dump(mode="json"), sort_keys=True)
    assert '"checks":' not in rendered
    assert "iterations" not in rendered
    assert "apply_plan_ids" not in rendered
    assert "artifact_verification_ids" not in rendered
    assert "required_artifact_roles" not in rendered
    assert "gateway_image" not in rendered
    assert INSTALL_TOKEN not in rendered


@pytest.mark.asyncio
async def test_postgres_runner_evidence_store_writes_only_scalar_metadata() -> None:
    pool = _FakeRunnerEvidencePool()
    store = PostgresByocRunnerEvidenceIntakeStore(pool)
    request = await _runner_request()

    receipt = await store.put(request, accepted_at=SUBMITTED_AT)
    record = await store.get(receipt.receipt_id)

    assert record is not None
    assert record.receipt == receipt
    assert record.cloud_provider == request.evidence.cloud_provider
    assert record.region == request.evidence.region
    flattened_args = json.dumps([str(arg) for _, args in pool.calls for arg in args])
    assert "fyralis.byoc.runner_evidence_summary.v1" not in flattened_args
    assert "apply_plan_ids" not in flattened_args
    assert "artifact_verification_ids" not in flattened_args
    assert "required_artifact_roles" not in flattened_args
    assert "gateway_image" not in flattened_args
    assert INSTALL_TOKEN not in flattened_args


def test_runner_evidence_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()

    assert bundle["summary"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.runner_evidence_summary.v1"
    )
    assert bundle["submission_request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.runner_evidence_submission.v1"
    )
    assert bundle["receipt"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.runner_evidence_receipt.v1"
    )
