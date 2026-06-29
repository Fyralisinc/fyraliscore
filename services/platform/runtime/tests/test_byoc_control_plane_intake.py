from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageReceiptQuery,
    ByocEvidencePackageSubmissionPayload,
    ByocEvidencePackageSubmissionRequest,
    InMemoryByocEvidencePackageIntakeStore,
    PostgresByocEvidencePackageIntakeStore,
    canonical_evidence_package_submission_payload,
    digest_evidence_package,
    evidence_package_submission_payload,
    model_json_schema_bundle,
    signed_evidence_receipt_read_headers,
    signed_evidence_package_submission,
    validate_evidence_receipt_read_auth_headers,
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


def _payload(
    *,
    nonce: str = "nonce-intake-contract-001",
) -> ByocEvidencePackageSubmissionPayload:
    return evidence_package_submission_payload(
        package=PACKAGE,
        agent_id=AGENT_ID,
        agent_version=AGENT_VERSION,
        nonce=nonce,
        submitted_at=SUBMITTED_AT,
    )


def _request(
    *,
    nonce: str = "nonce-intake-contract-001",
) -> ByocEvidencePackageSubmissionRequest:
    return signed_evidence_package_submission(
        _payload(nonce=nonce),
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
    )


class _FakeReceiptPool:
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
                "package_digest": args[8],
                "package_generated_at": args[9],
                "ledger_overall_status": args[10],
                "required_evidence_passed": args[11],
                "live_report_envelope_digest": args[12],
                "submitted_at": args[13],
                "accepted_at": args[14],
                "stored_scope": args[15],
            }
            self.rows[row["receipt_id"]] = row
            return row
        return self.rows.get(args[0])

    async def fetch(self, query: str, *args):
        self.calls.append((query, args))
        rows = list(self.rows.values())
        arg_index = 0
        if "deployment_id = $" in query:
            deployment_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["deployment_id"] == deployment_id]
        if "customer_id = $" in query:
            customer_id = args[arg_index]
            arg_index += 1
            rows = [row for row in rows if row["customer_id"] == customer_id]
        limit = args[-1]
        rows.sort(
            key=lambda row: (row["accepted_at"], row["receipt_id"]),
            reverse=True,
        )
        return rows[:limit]


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


@pytest.mark.asyncio
async def test_intake_store_lists_bounded_sanitized_receipt_metadata() -> None:
    store = InMemoryByocEvidencePackageIntakeStore()
    await store.put(
        _request(nonce="nonce-intake-contract-list-001"),
        accepted_at=datetime(2026, 6, 26, 12, 30, tzinfo=UTC),
    )
    latest_receipt = await store.put(
        _request(nonce="nonce-intake-contract-list-002"),
        accepted_at=datetime(2026, 6, 26, 12, 35, tzinfo=UTC),
    )

    page = await store.list_receipts(
        ByocEvidencePackageReceiptQuery(
            deployment_id=PACKAGE.deployment_id,
            customer_id=PACKAGE.customer_id,
            limit=1,
        )
    )

    assert page.schema_version == "fyralis.byoc.evidence_package_receipt_list.v1"
    assert page.result_count == 1
    assert page.items[0].receipt == latest_receipt
    rendered = page.model_dump_json()
    assert "source_artifacts" not in rendered
    assert "evidence_ledger" not in rendered
    assert "gateway.customer.internal" not in rendered


@pytest.mark.asyncio
async def test_postgres_intake_store_writes_only_scalar_receipt_metadata() -> None:
    pool = _FakeReceiptPool()
    store = PostgresByocEvidencePackageIntakeStore(pool)
    request = _request()

    receipt = await store.put(request, accepted_at=SUBMITTED_AT)
    record = await store.get(receipt.receipt_id)

    assert record is not None
    assert record.receipt == receipt
    assert record.cloud_provider == PACKAGE.cloud_provider
    assert record.region == PACKAGE.region
    flattened_args = json.dumps([str(arg) for _, args in pool.calls for arg in args])
    assert "fyralis.byoc.evidence_package.v1" not in flattened_args
    assert "source_artifacts" not in flattened_args
    assert "evidence_ledger" not in flattened_args
    assert "gateway.customer.internal" not in flattened_args
    assert "postgresql://" not in flattened_args
    assert "raw_report" not in flattened_args


@pytest.mark.asyncio
async def test_postgres_intake_store_lists_only_scalar_receipt_metadata() -> None:
    pool = _FakeReceiptPool()
    store = PostgresByocEvidencePackageIntakeStore(pool)
    await store.put(
        _request(nonce="nonce-intake-contract-pg-list-001"),
        accepted_at=datetime(2026, 6, 26, 12, 30, tzinfo=UTC),
    )
    latest_receipt = await store.put(
        _request(nonce="nonce-intake-contract-pg-list-002"),
        accepted_at=datetime(2026, 6, 26, 12, 35, tzinfo=UTC),
    )

    page = await store.list_receipts(
        ByocEvidencePackageReceiptQuery(
            deployment_id=PACKAGE.deployment_id,
            customer_id=PACKAGE.customer_id,
            limit=1,
        )
    )

    assert page.result_count == 1
    assert page.items[0].receipt == latest_receipt
    flattened_args = json.dumps([str(arg) for _, args in pool.calls for arg in args])
    assert "fyralis.byoc.evidence_package.v1" not in flattened_args
    assert "source_artifacts" not in flattened_args
    assert "raw_report" not in flattened_args


def test_receipt_query_requires_deployment_or_customer_bound() -> None:
    with pytest.raises(ValidationError, match="deployment_id or customer_id"):
        ByocEvidencePackageReceiptQuery()


def test_receipt_read_auth_headers_verify_without_serializing_secret() -> None:
    headers = signed_evidence_receipt_read_headers(
        method="GET",
        path="/byoc/control-plane/evidence-packages",
        query=f"deployment_id={PACKAGE.deployment_id}",
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
        nonce="nonce-intake-read-auth-001",
        timestamp=SUBMITTED_AT,
    )

    assert SIGNING_SECRET not in json.dumps(headers)
    assert validate_evidence_receipt_read_auth_headers(
        headers,
        method="GET",
        path="/byoc/control-plane/evidence-packages",
        query=f"deployment_id={PACKAGE.deployment_id}",
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
        now=SUBMITTED_AT,
    ) == []


def test_receipt_read_auth_rejects_bad_signature_or_stale_timestamp() -> None:
    headers = signed_evidence_receipt_read_headers(
        method="GET",
        path="/byoc/control-plane/evidence-packages",
        query=f"deployment_id={PACKAGE.deployment_id}",
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
        nonce="nonce-intake-read-auth-002",
        timestamp=SUBMITTED_AT,
    )

    bad_signature = {
        **headers,
        "x-fyralis-byoc-read-signature": "0" * 64,
    }
    bad_signature_violations = validate_evidence_receipt_read_auth_headers(
        bad_signature,
        method="GET",
        path="/byoc/control-plane/evidence-packages",
        query=f"deployment_id={PACKAGE.deployment_id}",
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
        now=SUBMITTED_AT,
    )
    assert [violation.code for violation in bad_signature_violations] == [
        "invalid_signature"
    ]

    assert "stale_read_auth" in {
        violation.code
        for violation in validate_evidence_receipt_read_auth_headers(
            headers,
            method="GET",
            path="/byoc/control-plane/evidence-packages",
            query=f"deployment_id={PACKAGE.deployment_id}",
            signing_secret=SIGNING_SECRET,
            expected_key_ref=SIGNING_KEY_REF,
            now=datetime(2026, 6, 26, 12, 40, 1, tzinfo=UTC),
        )
    }


def test_intake_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()

    assert bundle["submission_request"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package_submission.v1"
    )
    assert bundle["receipt"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package_receipt.v1"
    )
    assert bundle["receipt_list"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.evidence_package_receipt_list.v1"
    )
