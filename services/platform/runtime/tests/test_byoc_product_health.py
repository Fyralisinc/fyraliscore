from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from services.platform.runtime.byoc_product_health import (
    ByocProductHealthIssue,
    ByocProductHealthQuery,
    ByocProductModelHealth,
    ByocProductPipelineHealth,
    ByocProductSourceHealth,
    ByocProductThinkHealth,
    ByocProductVectorHealth,
    InMemoryByocProductHealthIntakeStore,
    canonical_product_health_snapshot_payload,
    digest_product_health_snapshot,
    model_json_schema_bundle,
    product_health_snapshot_payload,
    signed_product_health_snapshot,
    unknown_product_health,
    validate_product_health_snapshot_submission,
)


DEPLOYMENT_ID = "dep_product01"
CUSTOMER_ID = "cus_product01"
AGENT_ID = "agt_product01"
SIGNING_SECRET = "local-product-health-secret"
SIGNING_KEY_REF = "control-plane/byoc/product-health-intake"
COLLECTED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def _payload():
    return product_health_snapshot_payload(
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        agent_id=AGENT_ID,
        agent_version="2026.06.27",
        artifact_revision="2026.06.27-1",
        overall_status="degraded",
        collected_at=COLLECTED_AT,
        nonce="nonce-product-health-001",
        sources=(
            ByocProductSourceHealth(
                source="slack",
                status="ready",
                auth_status="ready",
                backfill_status="idle",
                items_ingested_count=100,
                items_failed_count=0,
                queue_depth_count=0,
                lag_seconds=12,
                last_success_at=COLLECTED_AT,
            ),
            ByocProductSourceHealth(
                source="github",
                status="degraded",
                auth_status="ready",
                backfill_status="running",
                items_ingested_count=50,
                items_failed_count=2,
                queue_depth_count=4,
                lag_seconds=45,
                last_success_at=COLLECTED_AT,
            ),
        ),
        pipeline=ByocProductPipelineHealth(
            status="ready",
            queue_lag_count=4,
            dead_letter_count=0,
            retry_backlog_count=2,
            dropped_item_count=0,
        ),
        think=ByocProductThinkHealth(
            status="ready",
            run_count=12,
            failed_run_count=1,
            queued_run_count=0,
            latest_run_at=COLLECTED_AT,
            breaker_status="closed",
        ),
        models=ByocProductModelHealth(
            status="degraded",
            model_count=9,
            model_build_count=3,
            failed_build_count=1,
            model_relation_count=24,
            orphan_model_count=1,
            stale_relation_count=0,
            latest_build_at=COLLECTED_AT,
            graph_status="degraded",
        ),
        vector_index=ByocProductVectorHealth(
            status="ready",
            vector_count=1200,
            backlog_count=3,
            failed_job_count=0,
            latest_job_at=COLLECTED_AT,
            retrieval_status="ready",
        ),
        issues=(
            ByocProductHealthIssue(
                code="model_build_retry",
                severity="warning",
                component="models",
                observed_count=1,
                first_observed_at=COLLECTED_AT,
                latest_observed_at=COLLECTED_AT,
            ),
        ),
    )


def test_product_health_snapshot_signature_and_digest_are_stable() -> None:
    payload = _payload()
    request = signed_product_health_snapshot(
        payload,
        signing_secret=SIGNING_SECRET,
        key_ref=SIGNING_KEY_REF,
    )

    assert validate_product_health_snapshot_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []
    assert digest_product_health_snapshot(payload).startswith("sha256:")
    rendered = canonical_product_health_snapshot_payload(payload)
    assert json.loads(rendered)["stored_scope"] == (
        "sanitized_product_health_metadata_only"
    )
    assert b"raw customer" not in rendered.lower()
    assert b"token=" not in rendered.lower()


def test_product_health_rejects_bad_signature_and_unsafe_codes() -> None:
    request = signed_product_health_snapshot(
        _payload(),
        signing_secret="wrong-secret",
        key_ref=SIGNING_KEY_REF,
    )

    violations = validate_product_health_snapshot_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    )

    assert [violation.code for violation in violations] == ["invalid_signature"]
    with pytest.raises(ValidationError):
        ByocProductSourceHealth(
            source="https://source.example",
            status="ready",
            items_ingested_count=1,
            items_failed_count=0,
        )


@pytest.mark.asyncio
async def test_product_health_store_returns_latest_or_unknown_state() -> None:
    store = InMemoryByocProductHealthIntakeStore()
    query = ByocProductHealthQuery(
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
    )

    unknown = await store.latest(query)
    receipt = await store.put(
        signed_product_health_snapshot(
            _payload(),
            signing_secret=SIGNING_SECRET,
            key_ref=SIGNING_KEY_REF,
        )
    )
    latest = await store.latest(query)

    assert unknown == unknown_product_health(query=query, generated_at=unknown.generated_at)
    assert unknown.observed is False
    assert receipt.stored_scope == "sanitized_product_health_metadata_only"
    assert latest.observed is True
    assert latest.latest_snapshot_id == receipt.snapshot_id
    assert latest.sources[1].source == "github"
    assert latest.models.model_count == 9
    assert latest.privacy_boundary.raw_payloads_included is False


def test_product_health_schema_bundle_is_exportable() -> None:
    bundle = model_json_schema_bundle()

    assert bundle["stored_scope"] == "sanitized_product_health_metadata_only"
    assert bundle["product_health"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health.v1"
    )
    assert bundle["snapshot_payload"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_snapshot.v1"
    )
