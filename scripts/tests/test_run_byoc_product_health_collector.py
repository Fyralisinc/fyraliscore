from __future__ import annotations

import json
from datetime import UTC, datetime

import scripts.run_byoc_product_health_collector as collector_script
from scripts.run_byoc_product_health_collector import (
    DEFAULT_DATABASE_URL_ENV,
    DEFAULT_SIGNING_SECRET_ENV,
    main,
)
from services.platform.runtime.byoc_product_health import (
    ByocProductHealthSnapshotRequest,
    ByocProductModelHealth,
    ByocProductPipelineHealth,
    ByocProductThinkHealth,
    ByocProductVectorHealth,
    product_health_snapshot_payload,
    validate_product_health_snapshot_submission,
)


DEPLOYMENT_ID = "dep_scriptcollector01"
CUSTOMER_ID = "cus_scriptcollector01"
AGENT_ID = "agt_scriptcollector01"
SIGNING_SECRET = "local-product-health-collector-secret"
SIGNING_KEY_REF = "control-plane/byoc/evidence-intake-key"
DATABASE_URL = "postgresql://collector.local/fyralis"
COLLECTED_AT = datetime(2026, 6, 27, 13, 0, tzinfo=UTC)


def test_run_byoc_product_health_collector_prints_signed_snapshot(
    monkeypatch,
    capsys,
) -> None:
    collected: dict[str, object] = {}
    monkeypatch.setenv(DEFAULT_DATABASE_URL_ENV, DATABASE_URL)
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    async def _fake_collect(args, *, database_url: str):
        collected["database_url"] = database_url
        collected["tenant_id"] = args.tenant_id
        return _payload()

    monkeypatch.setattr(collector_script, "_collect_snapshot", _fake_collect)

    code = main(
        [
            "--deployment-id",
            DEPLOYMENT_ID,
            "--customer-id",
            CUSTOMER_ID,
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            "2026.06.27",
            "--artifact-revision",
            "2026.06.27-collector",
            "--tenant-id",
            "00000000-0000-4000-8000-000000000001",
            "--key-ref",
            SIGNING_KEY_REF,
            "--nonce",
            "nonce-product-health-script-001",
        ]
    )

    captured = capsys.readouterr()
    output = json.loads(captured.out)
    rendered = json.dumps(output, sort_keys=True)
    request = ByocProductHealthSnapshotRequest.model_validate(output)
    assert code == 0
    assert collected == {
        "database_url": DATABASE_URL,
        "tenant_id": "00000000-0000-4000-8000-000000000001",
    }
    assert request.schema_version == "fyralis.byoc.product_health_snapshot.v1"
    assert validate_product_health_snapshot_submission(
        request,
        signing_secret=SIGNING_SECRET,
        expected_key_ref=SIGNING_KEY_REF,
    ) == []
    assert SIGNING_SECRET not in rendered
    assert "postgresql://" not in rendered
    assert captured.err == ""


def test_run_byoc_product_health_collector_unsigned_mode(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv(DEFAULT_DATABASE_URL_ENV, DATABASE_URL)

    async def _fake_collect(args, *, database_url: str):
        return _payload()

    monkeypatch.setattr(collector_script, "_collect_snapshot", _fake_collect)

    code = main(
        [
            "--deployment-id",
            DEPLOYMENT_ID,
            "--customer-id",
            CUSTOMER_ID,
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            "2026.06.27",
            "--artifact-revision",
            "2026.06.27-collector",
            "--unsigned",
        ]
    )

    captured = capsys.readouterr()
    output = json.loads(captured.out)
    assert code == 0
    assert output["schema_version"] == "fyralis.byoc.product_health_snapshot.v1"
    assert "signature" not in output
    assert captured.err == ""


def test_run_byoc_product_health_collector_posts_signed_snapshot(
    monkeypatch,
    capsys,
) -> None:
    posted: dict[str, object] = {}
    monkeypatch.setenv(DEFAULT_DATABASE_URL_ENV, DATABASE_URL)
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    async def _fake_collect(args, *, database_url: str):
        return _payload()

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def read(self) -> bytes:
            return json.dumps(
                {
                    "schema_version": "fyralis.byoc.product_health_receipt.v1",
                    "status": "accepted",
                    "snapshot_id": "phs_0123456789abcdef0123456789abcdef",
                    "stored_scope": "sanitized_product_health_metadata_only",
                }
            ).encode("utf-8")

    def _fake_urlopen(request, *, timeout):
        posted["url"] = request.full_url
        posted["timeout"] = timeout
        posted["body"] = json.loads(request.data.decode("utf-8"))
        return _Response()

    monkeypatch.setattr(collector_script, "_collect_snapshot", _fake_collect)
    monkeypatch.setattr(
        "scripts.run_byoc_product_health_collector.urllib.request.urlopen",
        _fake_urlopen,
    )

    code = main(
        [
            "--deployment-id",
            DEPLOYMENT_ID,
            "--customer-id",
            CUSTOMER_ID,
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            "2026.06.27",
            "--artifact-revision",
            "2026.06.27-collector",
            "--key-ref",
            SIGNING_KEY_REF,
            "--submit-url",
            "https://control.example.com/byoc/control-plane/product-health-snapshots",
            "--timeout-seconds",
            "3",
        ]
    )

    captured = capsys.readouterr()
    receipt = json.loads(captured.out)
    posted_body = json.dumps(posted["body"], sort_keys=True)
    assert code == 0
    assert posted["url"] == (
        "https://control.example.com/byoc/control-plane/product-health-snapshots"
    )
    assert posted["timeout"] == 3.0
    assert receipt["snapshot_id"] == "phs_0123456789abcdef0123456789abcdef"
    assert "postgresql://" not in posted_body
    assert SIGNING_SECRET not in posted_body


def test_run_byoc_product_health_collector_requires_database_url(
    monkeypatch,
    capsys,
) -> None:
    missing_database_url_env = "FYRALIS_TEST_MISSING_DATABASE_URL"
    monkeypatch.delenv(missing_database_url_env, raising=False)
    monkeypatch.setenv(DEFAULT_SIGNING_SECRET_ENV, SIGNING_SECRET)

    code = main(
        [
            "--deployment-id",
            DEPLOYMENT_ID,
            "--customer-id",
            CUSTOMER_ID,
            "--agent-id",
            AGENT_ID,
            "--agent-version",
            "2026.06.27",
            "--artifact-revision",
            "2026.06.27-collector",
            "--key-ref",
            SIGNING_KEY_REF,
            "--database-url-env",
            missing_database_url_env,
        ]
    )

    captured = capsys.readouterr()
    assert code == 2
    assert missing_database_url_env in captured.err
    assert captured.out == ""


def test_run_byoc_product_health_collector_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    output = json.loads(captured.out)
    assert code == 0
    assert output["snapshot_payload"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_snapshot.v1"
    )


def _payload():
    return product_health_snapshot_payload(
        deployment_id=DEPLOYMENT_ID,
        customer_id=CUSTOMER_ID,
        agent_id=AGENT_ID,
        agent_version="2026.06.27",
        artifact_revision="2026.06.27-collector",
        overall_status="ready",
        collected_at=COLLECTED_AT,
        nonce="nonce-product-health-script-payload",
        pipeline=ByocProductPipelineHealth(
            status="ready",
            queue_lag_count=0,
            dead_letter_count=0,
            retry_backlog_count=0,
            dropped_item_count=0,
        ),
        think=ByocProductThinkHealth(
            status="ready",
            run_count=1,
            failed_run_count=0,
            queued_run_count=0,
            latest_run_at=COLLECTED_AT,
            breaker_status="closed",
        ),
        models=ByocProductModelHealth(
            status="ready",
            model_count=1,
            model_build_count=1,
            failed_build_count=0,
            model_relation_count=0,
            orphan_model_count=0,
            stale_relation_count=0,
            latest_build_at=COLLECTED_AT,
            graph_status="ready",
        ),
        vector_index=ByocProductVectorHealth(
            status="ready",
            vector_count=1,
            backlog_count=0,
            failed_job_count=0,
            latest_job_at=COLLECTED_AT,
            retrieval_status="ready",
        ),
    )
