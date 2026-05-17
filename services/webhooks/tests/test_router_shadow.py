"""M2.1 shadow-path router tests.

Verifies the shadow-write block added to `services/webhooks/router.py`:
  - S3 PUT + Kafka publish happen when shadow deps are wired and the
    flag is enabled.
  - **Shadow failure does NOT break the inline response.** This is
    the load-bearing safety test from M2's prime directive.
  - The `ingestion.shadow_write_enabled=False` flag disables the
    block (no S3 PUT, no Kafka publish).
  - The published envelope carries `ingress_kind="webhook"`.

These are unit tests — `ingest()` is stubbed so no DB is needed. The
S3 client and Kafka producer are also stubbed. The integration-level
test that exercises real Kafka + moto S3 lives in M2.4
(`test_e2e_shadow.py`).
"""
from __future__ import annotations

import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from lib.shared.ids import uuid7
from services.ingestion import shadow_write as shadow_write_module
from services.webhooks.tenant_resolver import Resolved
from services.webhooks.tests.conftest import slack_sign


_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_INSTALLATION_ROW_ID = UUID("22222222-2222-2222-2222-222222222222")
_SECRET = "router-shadow-test-slack"


class _StubResolver:
    async def resolve(self, provider, payload, headers):
        return Resolved(
            tenant_id=_TENANT,
            installation_row_id=_INSTALLATION_ROW_ID,
            secret_ref=None,
        )


class _StubFlags:
    """In-process tenant-flag stub. Defaults to flag-on; per-tenant
    overrides via `force[tenant_id][flag] = value`.
    """

    def __init__(self) -> None:
        self.force: dict[UUID, dict[str, bool]] = {}

    async def get_bool(self, tenant_id: UUID, flag_name: str, *, default: bool) -> bool:
        return self.force.get(tenant_id, {}).get(flag_name, default)


def _stub_ingest_result(observation_id: UUID | None = None) -> Any:
    """Build an object that quacks like IngestResult enough for the
    router's response shaping. The router only reads
    `result.observation.id`, `result.deduped`, `result.trigger_queue_id`.
    """
    obs = MagicMock()
    obs.id = observation_id or uuid7()
    res = MagicMock()
    res.observation = obs
    res.deduped = False
    res.trigger_queue_id = None
    return res


@pytest.fixture
def _patch_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", _SECRET)


@pytest.fixture
def _stub_ingest(monkeypatch: pytest.MonkeyPatch):
    """Replace `services.webhooks.router.ingest` with an AsyncMock so
    the router's call site succeeds without a real DB. The mock is
    returned for per-test assertions.
    """
    mock_ingest = AsyncMock(return_value=_stub_ingest_result())
    monkeypatch.setattr(
        "services.webhooks.router.ingest",
        mock_ingest,
    )
    return mock_ingest


@pytest.fixture
def _shadow_app(
    _patch_secrets: None,
    _stub_ingest: AsyncMock,
):
    """FastAPI app with the webhook router + shadow-path deps wired.

    Shadow deps:
      - s3_raw_client: AsyncMock with put_if_absent / get.
      - kafka_producer: AsyncMock with produce / flush.
      - tenant_flags: _StubFlags (default on).
    """
    from fastapi import FastAPI

    from services.webhooks.router import build_webhooks_router

    app = FastAPI()
    app.include_router(build_webhooks_router())

    deps = MagicMock()
    deps.pool = MagicMock()
    deps.actor_repo = None
    deps.alias_repo = None
    deps.embedder = None
    app.state.deps = deps
    app.state.tenant_resolver = _StubResolver()

    # --- Shadow deps ---
    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    s3.get = AsyncMock(return_value=b"")
    app.state.s3_raw_client = s3

    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)
    app.state.kafka_producer = kafka

    app.state.tenant_flags = _StubFlags()

    # Reset shadow_write counters so per-test asserts are isolated.
    shadow_write_module.reset_metrics()
    return app


def _sign_slack(body: bytes) -> tuple[str, str]:
    ts = int(time.time())
    return str(ts), slack_sign(_SECRET, body, ts)


def _slack_body() -> bytes:
    return json.dumps({
        "team_id": "T_SHADOW",
        "event": {
            "type": "message",
            "text": "shadow test",
            "ts": str(time.time()),
            "channel": "C_SHADOW",
            "user": "U_SHADOW",
        },
    }).encode("utf-8")


async def _post_slack(app, body: bytes) -> httpx.Response:
    ts, sig = _sign_slack(body)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        return await c.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "X-Slack-Request-Timestamp": ts,
                "X-Slack-Signature": sig,
                "Content-Type": "application/json",
            },
        )


# ---------------------------------------------------------------------
# 1. Happy path — shadow write fires when deps are wired and flag is
# enabled (default).
# ---------------------------------------------------------------------

async def test_webhook_shadow_path_writes_to_s3_and_kafka(_shadow_app, _stub_ingest):
    body = _slack_body()
    r = await _post_slack(_shadow_app, body)
    assert r.status_code in (200, 201), r.text

    s3 = _shadow_app.state.s3_raw_client
    kafka = _shadow_app.state.kafka_producer

    assert s3.put_if_absent.await_count == 1, (
        "shadow path must PUT the raw body to S3 once per webhook"
    )
    assert kafka.produce.await_count == 1, (
        "shadow path must publish to Kafka once per webhook"
    )

    # The inline path still ran.
    assert _stub_ingest.await_count == 1, (
        "inline ingest() must still execute (shadow does not replace it)"
    )

    # And the shadow_write module's success counter recorded it.
    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.success"] == 1


# ---------------------------------------------------------------------
# 2. LOAD-BEARING SAFETY TEST.
# Inject an S3 failure; assert inline response still 200/201, inline
# ingest still ran, no exception propagated. Per M2 work-order:
# "this test failing is a release blocker."
# ---------------------------------------------------------------------

async def test_shadow_path_failure_does_not_break_inline(_shadow_app, _stub_ingest):
    # Make the S3 PUT raise; the shadow block must swallow.
    _shadow_app.state.s3_raw_client.put_if_absent = AsyncMock(
        side_effect=RuntimeError("simulated S3 timeout"),
    )

    body = _slack_body()
    r = await _post_slack(_shadow_app, body)

    # The inline response is unaffected.
    assert r.status_code in (200, 201), r.text
    assert _stub_ingest.await_count == 1, (
        "inline ingest() must run regardless of shadow failure"
    )
    # The Kafka publish must NOT fire (shadow_write_raw raises before
    # the publish step when S3 fails).
    assert _shadow_app.state.kafka_producer.produce.await_count == 0

    # The shadow-failure metric recorded the S3 failure.
    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.failure.s3"] == 1
    assert metrics["shadow_write.success"] == 0


# ---------------------------------------------------------------------
# 3. Same safety property for Kafka publish failures.
# ---------------------------------------------------------------------

async def test_shadow_path_kafka_failure_does_not_break_inline(
    _shadow_app, _stub_ingest,
):
    _shadow_app.state.kafka_producer.produce = AsyncMock(
        side_effect=RuntimeError("simulated Kafka leader unavailable"),
    )

    body = _slack_body()
    r = await _post_slack(_shadow_app, body)

    assert r.status_code in (200, 201), r.text
    assert _stub_ingest.await_count == 1

    # S3 PUT succeeded; only the Kafka publish raised.
    assert _shadow_app.state.s3_raw_client.put_if_absent.await_count == 1

    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.failure.kafka"] == 1
    assert metrics["shadow_write.success"] == 0


# ---------------------------------------------------------------------
# 4. Flag-disabled path — observation written inline, no shadow side
# effects. Per LLD §11 / M2.1 work-order.
# ---------------------------------------------------------------------

async def test_shadow_path_disabled_by_flag(_shadow_app, _stub_ingest):
    flags: _StubFlags = _shadow_app.state.tenant_flags
    flags.force[_TENANT] = {"ingestion.shadow_write_enabled": False}

    body = _slack_body()
    r = await _post_slack(_shadow_app, body)

    assert r.status_code in (200, 201), r.text
    assert _stub_ingest.await_count == 1
    # NO shadow side effects.
    assert _shadow_app.state.s3_raw_client.put_if_absent.await_count == 0
    assert _shadow_app.state.kafka_producer.produce.await_count == 0

    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.success"] == 0
    assert metrics["shadow_write.failure.s3"] == 0
    assert metrics["shadow_write.failure.kafka"] == 0


# ---------------------------------------------------------------------
# 5. The published envelope carries ingress_kind="webhook" and the
# expected source / content_hash. Inspects the bytes handed to the
# Kafka producer.
# ---------------------------------------------------------------------

async def test_envelope_includes_ingress_kind_webhook(_shadow_app, _stub_ingest):
    body = _slack_body()
    r = await _post_slack(_shadow_app, body)
    assert r.status_code in (200, 201)

    produce_calls = _shadow_app.state.kafka_producer.produce.await_args_list
    assert len(produce_calls) == 1

    _, kwargs = produce_calls[0]
    assert kwargs["topic"] == "ingestion.raw"
    # Key is the tenant_id bytes for partition affinity.
    assert kwargs["key"] == str(_TENANT).encode("utf-8")

    envelope = json.loads(kwargs["value"])
    assert envelope["envelope_version"] == 1
    assert envelope["source"] == "slack"
    assert envelope["ingress_kind"] == "webhook"
    assert envelope["tenant_id"] == str(_TENANT)
    assert envelope["content_hash"] and len(envelope["content_hash"]) == 40
    # ingress_metadata is best-effort populated for webhook
    assert envelope["ingress_metadata"].get("event_type") == "message"
