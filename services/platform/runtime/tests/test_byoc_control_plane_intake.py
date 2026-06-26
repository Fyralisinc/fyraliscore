from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageSubmissionPayload,
    ByocEvidencePackageSubmissionRequest,
    InMemoryByocEvidencePackageIntakeStore,
    canonical_evidence_package_submission_payload,
    digest_evidence_package,
    evidence_package_submission_payload,
    model_json_schema_bundle,
    signed_evidence_package_submission,
    validate_evidence_package_submission,
)
from services.platform.runtime.byoc_evidence_package import load_byoc_evidence_package


ROOT = Path(__file__).resolve().parents[4]
PACKAGE = load_byoc_evidence_package(ROOT / "deploy/byoc/evidence-package.example.yaml")
SIGNING_SECRET = "local-control-plane-intake-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
AGENT_ID = "agt_intake01"
AGENT_VERSION = "0.1.0"
SUBMITTED_AT = datetime(2026, 6, 26, 12, 30, tzinfo=UTC)


def _payload() -> ByocEvidencePackageSubmissionPayload:
    return evidence_package_submission_payload(
        package=PACKAGE,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce="nonce-intake-contract-001",
        submitted_at=SUBMITTED_AT,
    )


def _request() -> ByocEvidencePackageSubmissionRequest:
    return signed_evidence_package_submission(
        _payload(),
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
    )


def test_signed_submission_verifies_without_serializing_secret() -> None:
    request = _request()

    serialized = request.model_dump_json()
    assert SIGNING_SECRET not in serialized
    assert request.signature.key_ref == SIGNING_KEY_REF
    assert canonical_evidence_package_submission_payload(_payload()) == (
        canonical_evidence_package_submission_payload(_payload())
    )
    assert validate_evidence_package_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []


def test_submission_signature_detects_tampering() -> None:
    request = _request()
    tampered = ByocEvidencePackageSubmissionRequest.model_validate(
        {
            **request.model_dump(mode="json"),
            "agent_version": "0.1.1",
        }
    )

    violations = validate_evidence_package_submission(
        tampered,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert [violation.code for violation in violations] == ["invalid_signature"]


def test_submission_rejects_raw_report_extra_field() -> None:
    data = _request().model_dump(mode="json")
    data["package"]["raw_report"] = {
        "details": "https://gateway.customer.internal token=secret",
    }

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        ByocEvidencePackageSubmissionRequest.model_validate(data)


def test_submission_privacy_scan_rejects_raw_url_or_token_marker() -> None:
    data = _request().model_dump(mode="json")
    data["nonce"] = "nonce-intake-contract-token=secret"
    request = ByocEvidencePackageSubmissionRequest.model_validate(data)

    violations = validate_evidence_package_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert "customer_data_marker_forbidden" in {
        violation.code for violation in violations
    }


@pytest.mark.asyncio
async def test_intake_store_records_only_sanitized_receipt_metadata() -> None:
    store = InMemoryByocEvidencePackageIntakeStore()
    request = _request()

    receipt = await store.put(request, accepted_at=SUBMITTED_AT)
    record = await store.get(receipt.receipt_id)

    assert record is not None
    assert record.receipt.package_digest == digest_evidence_package(PACKAGE)
    assert record.receipt.stored_scope == "sanitized_metadata_only"
    rendered = json.dumps(record.model_dump(mode="json"), sort_keys=True)
    assert "evidence_ledger" not in rendered
    assert "gateway.customer.internal" not in rendered
    assert "postgresql://" not in rendered
    assert "checks" not in rendered


def test_intake_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()

    assert bundle["submission_request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package_submission.v1"
    )
    assert bundle["receipt"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package_receipt.v1"
    )
