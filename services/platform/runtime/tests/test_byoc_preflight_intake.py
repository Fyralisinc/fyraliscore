from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleInputs,
    run_byoc_preflight_bundle,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportSubmissionPayload,
    ByocPreflightReportSubmissionRequest,
    InMemoryByocPreflightReportIntakeStore,
    PostgresByocPreflightReportIntakeStore,
    canonical_preflight_report_submission_payload,
    digest_preflight_report,
    model_json_schema_bundle,
    preflight_report_submission_payload,
    signed_preflight_report_submission,
    validate_preflight_report_submission,
)


ROOT = Path(__file__).resolve().parents[4]
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
IAM_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"
IAC_PACKAGE = ROOT / "deploy/byoc/aws/iac-package.example.yaml"
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
PLAN = ROOT / "deploy/byoc/bootstrap-plan.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"
SIGNING_SECRET = "local-preflight-intake-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
AGENT_ID = "agt_preflight01"
AGENT_VERSION = "2026.06.26-preflight"
SUBMITTED_AT = datetime(2026, 6, 26, 13, 0, tzinfo=UTC)


class _FakePreflightPool:
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
                "artifact_revision": args[5],
                "cloud_provider": args[6],
                "region": args[7],
                "report_digest": args[8],
                "preflight_status": args[9],
                "required_sections_passed": args[10],
                "section_count": args[11],
                "failed_section_count": args[12],
                "terraform_validate_executed": args[13],
                "submitted_at": args[14],
                "accepted_at": args[15],
                "stored_scope": args[16],
            }
            self.rows[row["receipt_id"]] = row
            return row
        return self.rows.get(args[0])


def _report():
    return run_byoc_preflight_bundle(
        ByocPreflightBundleInputs(
            dataplane_manifest_path=DATAPLANE,
            permissions_manifest_path=PERMISSIONS,
            iam_template_path=IAM_TEMPLATE,
            iac_package_path=IAC_PACKAGE,
            bootstrap_bundle_path=BUNDLE,
            bootstrap_plan_path=PLAN,
            env_path=ENV_TEMPLATE,
            repo_root=ROOT,
        )
    )


def _payload(
    *,
    nonce: str = "nonce-preflight-intake-001",
) -> ByocPreflightReportSubmissionPayload:
    return preflight_report_submission_payload(
        preflight_report=_report(),
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce=nonce,
        submitted_at=SUBMITTED_AT,
    )


def _request(
    *,
    nonce: str = "nonce-preflight-intake-001",
) -> ByocPreflightReportSubmissionRequest:
    return signed_preflight_report_submission(
        _payload(nonce=nonce),
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
    )


def test_signed_preflight_submission_verifies_without_serializing_secret() -> None:
    request = _request()
    payload = _payload()

    serialized = request.model_dump_json()
    assert SIGNING_SECRET not in serialized
    assert request.signature.key_ref == SIGNING_KEY_REF
    assert canonical_preflight_report_submission_payload(payload) == (
        canonical_preflight_report_submission_payload(payload)
    )
    assert validate_preflight_report_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []


def test_preflight_submission_signature_detects_tampering() -> None:
    request = _request()
    tampered = ByocPreflightReportSubmissionRequest.model_validate(
        {
            **request.model_dump(mode="json"),
            "agent_version": "2026.06.26-tampered",
        }
    )

    violations = validate_preflight_report_submission(
        tampered,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert [violation.code for violation in violations] == ["invalid_signature"]


def test_preflight_submission_rejects_raw_extra_field() -> None:
    data = _request().model_dump(mode="json")
    data["preflight_report"]["child_report"] = {
        "details": "https://gateway.customer.internal token=secret",
    }

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ByocPreflightReportSubmissionRequest.model_validate(data)


def test_preflight_submission_privacy_scan_rejects_url_or_token_marker() -> None:
    data = _request().model_dump(mode="json")
    data["nonce"] = "nonce-preflight-intake-token=secret"
    request = ByocPreflightReportSubmissionRequest.model_validate(data)

    violations = validate_preflight_report_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert "customer_data_marker_forbidden" in {
        violation.code for violation in violations
    }


@pytest.mark.asyncio
async def test_preflight_intake_store_records_only_sanitized_receipt_metadata() -> None:
    store = InMemoryByocPreflightReportIntakeStore()
    request = _request()

    receipt = await store.put(request, accepted_at=SUBMITTED_AT)
    record = await store.get(receipt.receipt_id)

    assert record is not None
    assert record.receipt.report_digest == digest_preflight_report(
        request.preflight_report
    )
    assert record.receipt.stored_scope == "sanitized_metadata_only"
    assert record.receipt.section_count == len(request.preflight_report.sections)
    rendered = json.dumps(record.model_dump(mode="json"), sort_keys=True)
    assert '"preflight_report":' not in rendered
    assert '"sections":' not in rendered
    assert "gateway.customer.internal" not in rendered
    assert "postgresql://" not in rendered
    assert "ghcr.io" not in rendered


@pytest.mark.asyncio
async def test_postgres_preflight_store_writes_only_scalar_metadata() -> None:
    pool = _FakePreflightPool()
    store = PostgresByocPreflightReportIntakeStore(pool)
    request = _request()

    receipt = await store.put(request, accepted_at=SUBMITTED_AT)
    record = await store.get(receipt.receipt_id)

    assert record is not None
    assert record.receipt == receipt
    assert record.cloud_provider == request.cloud_provider
    assert record.region == request.region
    flattened_args = json.dumps([str(arg) for _, args in pool.calls for arg in args])
    assert "fyralis.byoc.preflight_bundle.v1" not in flattened_args
    assert "preflight_report" not in flattened_args
    assert "sections" not in flattened_args
    assert "gateway.customer.internal" not in flattened_args
    assert "ghcr.io" not in flattened_args


def test_preflight_intake_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()

    assert bundle["submission_request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.preflight_report_submission.v1"
    )
    assert bundle["receipt"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.preflight_report_receipt.v1"
    )
