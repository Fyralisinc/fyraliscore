"""M5.3 — webhook router flag-branched cutover tests.

These tests cover the cutover path added in M5.3:

  - flag=FALSE → router runs inline `ingest()` + M2 shadow write
    (existing M2.1 behaviour preserved).
  - flag=TRUE  → router skips inline; publishes to Kafka via
    `shadow_write_raw`; emits 1% traffic signal; returns 202.
  - Kafka failure under flag=TRUE → graceful degradation: router
    falls back to inline `ingest()` + returns 200; bumps the
    `webhook_router_kafka_path_total{outcome="fallback"}` metric.

The LOAD-BEARING test (`test_double_ingestion_safe_during_cutover`)
is the N1-during-cutover proof: same logical webhook arrives once
via inline (flag=FALSE) and once via Kafka path (flag=TRUE flipped
between requests). The observations UNIQUE catches the race; exactly
one row exists in Postgres after both requests + the writer's
simulated consume.

`test_flag_cache_ttl_governs_cutover_window` uses explicit
`time.monotonic` control (NOT `asyncio.sleep`) so the TTL-boundary
behaviour is deterministic — per the M5.3 sub-block reminder.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import httpx
import orjson
import pytest

from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion import shadow_write as shadow_write_module
from services.ingestion.feature_flags import (
    KAFKA_PATH_ENABLED,
    SHADOW_WRITE_ENABLED,
    FlagCache,
    TenantFlags,
)
from services.ingestion.normalizer.models import NormalizedEnvelope
from services.ingestion.writers import observation_writer as writer_module
from services.webhooks import metrics
from services.webhooks.tenant_resolver import Resolved
from services.webhooks.tests.conftest import slack_sign


_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_INSTALLATION_ROW_ID = UUID("22222222-2222-2222-2222-222222222222")
_SECRET = "router-cutover-test-slack"


# ---------------------------------------------------------------------
# Stubs (mirror test_router_shadow.py patterns).
# ---------------------------------------------------------------------

class _StubResolver:
    def __init__(self, tenant_id: UUID = _TENANT) -> None:
        self._tenant_id = tenant_id

    async def resolve(self, provider, payload, headers):
        return Resolved(
            tenant_id=self._tenant_id,
            installation_row_id=_INSTALLATION_ROW_ID,
            secret_ref=None,
        )


class _StubFlags:
    """In-process tenant-flag stub. Per-tenant + per-flag overrides via
    `force[tenant_id][flag_name] = value`; otherwise returns the
    caller-supplied default. Mirrors the M2 router-shadow stub.
    """

    def __init__(self) -> None:
        self.force: dict[UUID, dict[str, bool]] = {}

    async def get_bool(
        self, tenant_id: UUID, flag_name: str, *, default: bool,
    ) -> bool:
        return self.force.get(tenant_id, {}).get(flag_name, default)


def _stub_ingest_result(observation_id: UUID | None = None) -> Any:
    from lib.shared.ids import uuid7
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
    mock_ingest = AsyncMock(return_value=_stub_ingest_result())
    monkeypatch.setattr(
        "services.webhooks.router.ingest",
        mock_ingest,
    )
    return mock_ingest


@pytest.fixture
def _cutover_app(_patch_secrets: None, _stub_ingest: AsyncMock):
    """FastAPI app with M5.3 cutover deps wired:
      - tenant_flags: _StubFlags (default False → no cutover unless
        explicitly enabled).
      - kafka_producer + s3_raw_client: AsyncMocks.
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
    s3.get = AsyncMock(return_value=b"")
    app.state.s3_raw_client = s3

    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)
    app.state.kafka_producer = kafka

    app.state.tenant_flags = _StubFlags()

    shadow_write_module.reset_metrics()
    return app


def _sign_slack(body: bytes) -> tuple[str, str]:
    ts = int(time.time())
    return str(ts), slack_sign(_SECRET, body, ts)


def _slack_body(message_ts: str | None = None, text: str = "cutover test") -> bytes:
    if message_ts is None:
        message_ts = f"{time.time():.6f}"
    return json.dumps({
        "team_id": "T_CUT",
        "event": {
            "type": "message",
            "text": text,
            "ts": message_ts,
            "channel": "C_CUT",
            "user": "U_CUT",
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


# =====================================================================
# 1. flag=FALSE — router runs inline + M2 shadow (M2.1 behaviour).
# =====================================================================

async def test_router_flag_false_runs_inline_and_shadow(
    _cutover_app, _stub_ingest,
):
    body = _slack_body()
    r = await _post_slack(_cutover_app, body)

    assert r.status_code in (200, 201), r.text
    assert _stub_ingest.await_count == 1, (
        "flag=FALSE: inline ingest() must run (M2 behaviour)"
    )
    # M2 shadow path ran too (default SHADOW_WRITE_ENABLED=True).
    assert _cutover_app.state.s3_raw_client.put_if_absent.await_count == 1
    # Cutover-success metric NOT incremented.
    assert metrics.get_kafka_path_count("slack", "success") == 0
    assert metrics.get_kafka_path_count("slack", "fallback") == 0


# =====================================================================
# 2. flag=TRUE — router publishes to Kafka, returns 202, skips inline.
# =====================================================================

async def test_router_flag_true_publishes_to_kafka_returns_202(
    _cutover_app, _stub_ingest,
):
    flags: _StubFlags = _cutover_app.state.tenant_flags
    flags.force[_TENANT] = {KAFKA_PATH_ENABLED: True}

    body = _slack_body()
    r = await _post_slack(_cutover_app, body)

    assert r.status_code == 202, (
        f"flag=TRUE must return 202; got {r.status_code}: {r.text}"
    )
    # Inline ingest() MUST NOT run on the cutover path.
    assert _stub_ingest.await_count == 0, (
        "flag=TRUE: inline ingest() must be skipped — cutover writes "
        "via Kafka path only. If this fails, N1 cutover-safety is "
        "broken (double-write through both paths)."
    )
    # Kafka publish landed. There are TWO publishes — the raw envelope
    # on ingestion.raw AND the 1% traffic signal on
    # ingestion.tenant_traffic_signal. The signal may or may not fire
    # depending on the content_hash sample decision; the raw publish
    # always fires.
    kafka_calls = _cutover_app.state.kafka_producer.produce.await_args_list
    raw_publishes = [c for c in kafka_calls if c.kwargs.get("topic") == "ingestion.raw"]
    assert len(raw_publishes) == 1, (
        f"flag=TRUE must publish exactly one record to ingestion.raw; "
        f"got {len(raw_publishes)}: {kafka_calls}"
    )
    # S3 PutIfAbsent also fired (shadow_write_raw path).
    assert _cutover_app.state.s3_raw_client.put_if_absent.await_count == 1
    # Cutover-success metric incremented.
    assert metrics.get_kafka_path_count("slack", "success") == 1
    assert metrics.get_kafka_path_count("slack", "fallback") == 0
    # M2 shadow-write-after-inline NOT invoked separately — cutover
    # already published; double-publishing would be wasted load.
    # The single raw_publishes count above proves this.


# =====================================================================
# 3. Kafka-failure under flag=TRUE — fallback to inline, return 200.
# =====================================================================

async def test_cutover_kafka_failure_falls_back_to_inline(
    _cutover_app, _stub_ingest,
):
    flags: _StubFlags = _cutover_app.state.tenant_flags
    flags.force[_TENANT] = {KAFKA_PATH_ENABLED: True}

    # Inject Kafka failure on the raw publish. The signal publish (if
    # it fires) is post-success, so it won't be reached.
    _cutover_app.state.kafka_producer.produce = AsyncMock(
        side_effect=RuntimeError("simulated Kafka leader unavailable"),
    )

    body = _slack_body()
    r = await _post_slack(_cutover_app, body)

    # User-visible behaviour preserved: 200/201, NOT a 5xx.
    assert r.status_code in (200, 201), (
        f"Kafka failure under flag=TRUE must fall back to inline + "
        f"return 200/201, not surface the failure as a 5xx. Got "
        f"{r.status_code}: {r.text}"
    )
    # Inline ingest() DID run (fallback).
    assert _stub_ingest.await_count == 1, (
        "Kafka failure under flag=TRUE must fall back to inline ingest()"
    )
    # Fallback metric incremented; success metric stays at 0.
    assert metrics.get_kafka_path_count("slack", "fallback") == 1
    assert metrics.get_kafka_path_count("slack", "success") == 0
    # M2 shadow-write-after-inline was SKIPPED under fallback (we
    # already tried Kafka and it failed; immediate retry would fail
    # again).
    assert _cutover_app.state.s3_raw_client.put_if_absent.await_count == 1, (
        "Under fallback, S3 PUT fires once via the cutover attempt; the "
        "M2 shadow path is suppressed to avoid retrying Kafka twice."
    )


# =====================================================================
# 4. Cutover deps missing under flag=TRUE — fallback to inline.
# =====================================================================

async def test_cutover_missing_deps_falls_back_to_inline(
    _cutover_app, _stub_ingest,
):
    """Operator misconfiguration: flag=TRUE but kafka_producer / s3
    aren't on app.state. The router must NOT crash; degrade silently
    to inline so the customer experience stays intact. Fallback metric
    is the only signal an operator sees.
    """
    flags: _StubFlags = _cutover_app.state.tenant_flags
    flags.force[_TENANT] = {KAFKA_PATH_ENABLED: True}

    # Strip the kafka producer.
    _cutover_app.state.kafka_producer = None

    body = _slack_body()
    r = await _post_slack(_cutover_app, body)

    assert r.status_code in (200, 201)
    assert _stub_ingest.await_count == 1, (
        "Missing Kafka deps under flag=TRUE must NOT prevent inline "
        "ingest() — graceful degradation is the contract."
    )
    assert metrics.get_kafka_path_count("slack", "fallback") == 1


# =====================================================================
# 5. TTL governs the cutover propagation window.
#    Explicit time control via monkeypatched `time.monotonic` (NOT
#    asyncio.sleep — the M5.3 reminder calls this out specifically).
# =====================================================================

async def test_flag_cache_ttl_governs_cutover_window(
    _patch_secrets, _stub_ingest, monkeypatch: pytest.MonkeyPatch,
):
    """The 30s TTL cache bounds the cutover-propagation latency.

    Sequence with explicit time control (no `asyncio.sleep`):
      [t=0]  pool returns no flag row → cache(False) → inline path.
      [t=1]  flip pool to flag=TRUE.
      [t=2]  cached False still active → inline path again (proves
             cache is actually caching).
      [t=35] past TTL → cache invalidates → pool re-read → flag=TRUE →
             cutover fires; response is 202.

    Time is controlled by patching `time.monotonic` in the
    `feature_flags.client` module so `_CacheEntry.expires_at`
    comparison is deterministic.
    """
    from fastapi import FastAPI
    from services.webhooks.router import build_webhooks_router

    # Mock clock. Cache reads monotonic from the client module.
    clock = {"now": 0.0}

    def _fake_monotonic() -> float:
        return clock["now"]

    monkeypatch.setattr(
        "services.ingestion.feature_flags.client.time.monotonic",
        _fake_monotonic,
    )

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

    pool_state: dict[str, Any] = {"row": None}  # default → False

    async def _fetchrow(_sql, _tenant_id, _flag_name):
        return pool_state["row"]

    flag_pool = AsyncMock()
    flag_pool.fetchrow = AsyncMock(side_effect=_fetchrow)

    cache = FlagCache(ttl_seconds=30.0)
    app.state.tenant_flags = TenantFlags(flag_pool, cache=cache)

    body = _slack_body()

    # ---- [t=0] First request: pool returns None → flag=False ----
    clock["now"] = 0.0
    r1 = await _post_slack(app, body)
    assert r1.status_code in (200, 201), r1.text
    assert _stub_ingest.await_count == 1, "step1: inline path expected"
    # Cutover-success metric: still 0.
    assert metrics.get_kafka_path_count("slack", "success") == 0

    # ---- [t=1] Flip pool to True; clock still within TTL ----
    pool_state["row"] = {"flag_value": True}
    clock["now"] = 1.0
    r2 = await _post_slack(app, body)
    assert r2.status_code in (200, 201), (
        f"step2: within TTL, cache must hold the prior False value → "
        f"inline path. Got status {r2.status_code}: {r2.text}. If this "
        f"is 202, the cache isn't caching."
    )
    assert _stub_ingest.await_count == 2

    # ---- [t=35] Past TTL — cache invalidates; re-read sees TRUE ----
    clock["now"] = 35.0
    r3 = await _post_slack(app, body)
    assert r3.status_code == 202, (
        f"step3: past TTL, cache re-reads and sees flag=True → cutover "
        f"path. Got {r3.status_code}: {r3.text}. If 200/201, the TTL "
        f"boundary isn't expiring."
    )
    # Inline NOT called a third time.
    assert _stub_ingest.await_count == 2
    assert metrics.get_kafka_path_count("slack", "success") == 1


# =====================================================================
# 6. LOAD-BEARING — N1 cutover-safety: double-ingest race is dedup'd.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id, f"cutover-{tenant_id.hex[:8]}",
    )


def _build_normalized_envelope_from_slack(
    tenant_id: UUID, message_ts: str, text: str,
) -> NormalizedEnvelope:
    """Reconstruct the NormalizedEnvelope the M2.3 normalizer would
    emit from the slack handler's output for the test webhook. The
    Slack handler's external_id is `"<channel>:<ts>"`; occurred_at is
    parsed from ts."""
    ts_float = float(message_ts)
    occurred_at = dt.datetime.fromtimestamp(ts_float, tz=dt.timezone.utc)
    content_hash = "c" * 40
    return NormalizedEnvelope(
        envelope_version=1,
        source="slack",
        ingress_kind="webhook",
        tenant_id=tenant_id,
        raw_s3_key=f"dev/slack/{tenant_id}/2026-05/cc/{content_hash}.json",
        content_hash=content_hash,
        raw_ingested_at=occurred_at,
        source_channel="slack:message",
        content_text=text,
        content={
            "channel": "C_CUT",
            "ts": message_ts,
            "text": text,
            "team": "T_CUT",
            "user": "U_CUT",
        },
        occurred_at=occurred_at,
        trust_tier="attested_agent",
        kind="signal",
        source_actor_ref="slack:U_CUT",
        external_id=f"C_CUT:{message_ts}",
        entities_hint=[],
        normalized_at=occurred_at,
        ingress_metadata={},
        idem_hints={},
    )


class _CaptureProducer:
    """IdempotentProducer stand-in for writer simulation."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes, bytes | None]] = []

    async def start(self) -> None:
        return None

    async def stop(self, timeout_seconds: float = 10.0) -> None:
        return None

    async def produce(
        self, topic: str, value: bytes, *,
        key: bytes | None = None, **_kw: Any,
    ) -> None:
        self.published.append((topic, value, key))


async def test_double_ingestion_safe_during_cutover(
    fresh_db: asyncpg.Pool,
    _patch_secrets: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LOAD-BEARING N1-during-cutover property.

    The same logical webhook arrives twice:
      Request A: flag=FALSE → inline `ingest()` writes observation #1.
      (operator flips flag to TRUE)
      Request B: flag=TRUE → cutover path; raw envelope published to
                 Kafka; router returns 202; inline is SKIPPED.
      Writer consume of B: simulated by calling the writer's
                 `_full_mode_write` with the NormalizedEnvelope the
                 normalizer would have produced. `ingest_from_draft`
                 hits the dedup pre-check against the existing row
                 (matching source_channel + external_id) and returns
                 deduped=True. The observations UNIQUE constraint is
                 the correctness backstop even if the pre-check were
                 bypassed.

    Final state: exactly 1 observation row for the tenant.
    """
    from fastapi import FastAPI

    from services.webhooks.router import build_webhooks_router

    tenant_id = uuid4()
    await _seed_tenant(fresh_db, tenant_id)

    app = FastAPI()
    app.include_router(build_webhooks_router())

    deps = MagicMock()
    deps.pool = fresh_db
    deps.actor_repo = ActorRepo(fresh_db)
    deps.alias_repo = EntityAliasRepo(fresh_db)
    deps.embedder = None
    app.state.deps = deps
    app.state.tenant_resolver = _StubResolver(tenant_id)

    # Real TenantFlags + real DB pool so the flag-flip propagates.
    flags = TenantFlags(fresh_db)
    app.state.tenant_flags = flags

    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    app.state.s3_raw_client = s3

    capture = _CaptureProducer()
    app.state.kafka_producer = capture

    # Use a stable message ts so both requests share an external_id.
    # The Slack handler computes external_id = "<channel>:<ts>".
    message_ts = f"{time.time():.6f}"
    body = _slack_body(message_ts=message_ts, text="N1 cutover safety test")

    # ---- Request A: flag=FALSE (no row) → inline writes obs #1 ----
    rA = await _post_slack(app, body)
    assert rA.status_code in (200, 201), rA.text

    count_after_a = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    assert count_after_a == 1, (
        f"After request A (inline), expected 1 observation; got "
        f"{count_after_a}."
    )

    # ---- Operator flips flag to TRUE ----
    await flags.set_bool(
        tenant_id, KAFKA_PATH_ENABLED, True,
        set_by="operator:test-cutover",
    )
    # Invalidate the cache explicitly (production has 30s TTL; tests
    # should not wait).
    flags.cache.invalidate(tenant_id, KAFKA_PATH_ENABLED)

    # ---- Request B: flag=TRUE → 202, cutover path ----
    rB = await _post_slack(app, body)
    assert rB.status_code == 202, (
        f"After flag flip, request B must use cutover path → 202; got "
        f"{rB.status_code}: {rB.text}"
    )

    # Still 1 observation — writer hasn't consumed yet.
    count_before_writer = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    assert count_before_writer == 1

    # ---- Simulate writer consuming the published envelope ----
    # We construct the NormalizedEnvelope the normalizer (M2.3) would
    # have produced from request B's slack body, then call the
    # writer's `_full_mode_write` directly. This is the production
    # code path that runs in M5.2; we just bypass the Kafka broker +
    # normalizer for unit-test scope.
    env = _build_normalized_envelope_from_slack(
        tenant_id, message_ts, "N1 cutover safety test",
    )
    await writer_module._full_mode_write(
        env,
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        embedding_producer=capture,
    )

    # ---- FINAL ASSERTION: still exactly 1 observation row ----
    count_final = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id = $1",
        tenant_id,
    )
    assert count_final == 1, (
        f"N1 cutover-safety FAILED: {count_final} observation rows "
        f"after inline + cutover for the same logical webhook. "
        f"Expected exactly 1 — the UNIQUE (source_channel, external_id, "
        f"occurred_at) constraint should have caught the duplicate. "
        f"If this fails, the cutover transition is unsafe even when "
        f"flag flips happen between in-flight requests."
    )
