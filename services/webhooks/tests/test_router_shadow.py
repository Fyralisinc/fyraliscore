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

import asyncio
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from lib.shared.ids import uuid7
from services.ingestion import shadow_write as shadow_write_module
from services.ingestion.feature_flags import (
    SHADOW_WRITE_ENABLED,
    FlagCache,
    TenantFlags,
)
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

# ---------------------------------------------------------------------
# 6. TTL cache flip-flop at the router level. Per M2.1 review (c):
# the previous 4 tests use `_StubFlags` which bypasses the real cache.
# This test wires the REAL `TenantFlags` against a controllable
# fetchrow mock and proves end-to-end that:
#   - the cache holds a TRUE value until TTL elapses,
#   - after TTL, the new FALSE value takes effect,
#   - shadow writes follow the cache state.
#
# Uses a 50ms TTL + real sleep, NOT freezegun — freezegun patches
# time.time / datetime but the cache uses time.monotonic, and the
# project's existing freezegun usage doesn't extend to monotonic
# patching. A real-time 50ms window is reliable in unit-test scope
# and keeps the test runtime under 200ms total.
# ---------------------------------------------------------------------

async def test_flag_cache_picks_up_change_within_ttl(_patch_secrets, _stub_ingest):
    """End-to-end flag flip across the cache boundary.

    Step 1: flag value=True (default-on; pool returns no row). Shadow
            fires.
    Step 2: change pool to return flag_value=False. WITHIN TTL: cache
            still says True; shadow STILL fires (this proves the
            cache is actually caching, not pass-through).
    Step 3: sleep past TTL. Cache invalidates; next request re-reads;
            pool returns False; shadow does NOT fire.
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

    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    app.state.s3_raw_client = s3

    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)
    app.state.kafka_producer = kafka

    # REAL TenantFlags + controllable pool. fetchrow returns whatever
    # `pool_state["row"]` says at call time, so the test mutates the
    # state between requests.
    pool_state: dict[str, Any] = {"row": None}  # no row → default True
    flag_pool = AsyncMock()
    async def _fetchrow(_sql, _tenant_id, _flag_name):
        return pool_state["row"]
    flag_pool.fetchrow = AsyncMock(side_effect=_fetchrow)

    # Short TTL so the test runs fast. Cache behaviour is identical
    # to the 30s production TTL — `time.monotonic` comparison is the
    # same logic in both.
    cache = FlagCache(ttl_seconds=0.05)
    app.state.tenant_flags = TenantFlags(flag_pool, cache=cache)

    shadow_write_module.reset_metrics()
    body = _slack_body()

    # M5.3 NOTE: the router now reads TWO flags per request —
    # KAFKA_PATH_ENABLED (cutover) AND SHADOW_WRITE_ENABLED (this
    # test's subject). Both share the same TenantFlags cache but are
    # keyed independently (tenant_id × flag_name), so they hit/miss
    # in lockstep. fetchrow counts reflect both reads.

    # ---- Step 1: default True (no row) → shadow fires ----
    r1 = await _post_slack(app, body)
    assert r1.status_code in (200, 201)
    assert s3.put_if_absent.await_count == 1, "step1: shadow must fire when flag is True"
    assert kafka.produce.await_count == 1
    # Pool was consulted twice (one cache miss per flag).
    assert flag_pool.fetchrow.await_count == 2

    # ---- Step 2: flip pool to False, but stay WITHIN TTL ----
    # Cache should still say True; shadow should STILL fire.
    pool_state["row"] = {"flag_value": False}
    r2 = await _post_slack(app, body)
    assert r2.status_code in (200, 201)
    assert s3.put_if_absent.await_count == 2, (
        "step2: cache must hold the prior True value within TTL; "
        "shadow must still fire. If this fails, the cache isn't "
        "caching (every request hits the DB)."
    )
    # And the pool was NOT consulted again (cache hit for both flags).
    assert flag_pool.fetchrow.await_count == 2, (
        "step2: cache hit must avoid the pool read"
    )

    # ---- Step 3: sleep past TTL; cache invalidates ----
    await asyncio.sleep(0.08)  # 80ms > 50ms TTL
    r3 = await _post_slack(app, body)
    assert r3.status_code in (200, 201)
    # Shadow did NOT fire — flag is now False.
    assert s3.put_if_absent.await_count == 2, (
        "step3: after TTL expires, cache must re-read; the new "
        "False value must suppress the shadow write. If this fails, "
        "the TTL boundary isn't actually expiring."
    )
    assert kafka.produce.await_count == 2  # only step1 + step2
    # And the pool WAS consulted twice more (one re-read per flag
    # after TTL expiry).
    assert flag_pool.fetchrow.await_count == 4

    # Inline path ran for all three requests regardless of flag state.
    assert _stub_ingest.await_count == 3


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
