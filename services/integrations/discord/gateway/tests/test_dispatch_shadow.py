"""M2.2 — Discord Gateway shadow-write tests.

Verifies the shadow block added to
`services/integrations/discord/gateway/dispatch.py:handle_message_create`:

  - On successful inline ingest(), the frame is shadow-written to
    S3 + Kafka with ingress_kind="gateway".
  - Shadow-write failure does NOT break frame dispatch — the inline
    observation is still written, and the function still returns.
  - The canonical-JSON bytes the shadow path writes round-trip
    losslessly through the Discord message handler — protects N2
    (replay-from-raw) and content_hash dedup determinism.

These exercise the full dispatch → ingest → shadow path against a
real test Postgres (`fresh_db`) — the test mirrors
`test_dispatch_message_create.py` for the inline assertions, then
extends with shadow assertions.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest

from services.ingestion import shadow_write as shadow_write_module
from services.ingestion.handlers.discord import handle_discord_message
from services.integrations.discord.gateway.dispatch import (
    DispatchDeps,
    _maybe_shadow_write_gateway,
    handle_message_create,
)
from services.integrations.discord.gateway.tests.conftest import (
    _TEST_GUILD_ID,
    make_message_create,
)


pytestmark = pytest.mark.integration


@pytest.fixture
def _shadow_mocks():
    """Build (s3_mock, kafka_mock, flags_mock) — the three optional
    DispatchDeps for the shadow path. The M2 shadow path is, by
    definition, the pre-cutover behaviour: SHADOW_WRITE_ENABLED on,
    KAFKA_PATH_ENABLED off. (A per-test override may flip either.)
    """
    from services.ingestion.feature_flags import (
        KAFKA_PATH_ENABLED,
        SHADOW_WRITE_ENABLED,
    )

    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    s3.get = AsyncMock(return_value=b"")

    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)

    async def _get_bool(tenant_id, flag_name, *, default):  # noqa: ANN001
        if flag_name == KAFKA_PATH_ENABLED:
            return False  # shadow path = NOT in cutover mode
        if flag_name == SHADOW_WRITE_ENABLED:
            return True
        return default

    flags = MagicMock()
    flags.get_bool = AsyncMock(side_effect=_get_bool)

    shadow_write_module.reset_metrics()
    return s3, kafka, flags


def _deps_with_shadow(base: DispatchDeps, s3, kafka, flags) -> DispatchDeps:
    """Clone DispatchDeps with shadow deps wired."""
    return DispatchDeps(
        pool=base.pool,
        tenant_resolver=base.tenant_resolver,
        actor_repo=base.actor_repo,
        alias_repo=base.alias_repo,
        embedder=base.embedder,
        application_id=base.application_id,
        s3_raw_client=s3,
        kafka_producer=kafka,
        tenant_flags=flags,
    )


# ---------------------------------------------------------------------
# 1. Happy path — MESSAGE_CREATE → inline observation + shadow write.
# ---------------------------------------------------------------------

async def test_gateway_message_create_writes_shadow(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
    _shadow_mocks,
):
    s3, kafka, flags = _shadow_mocks
    deps = _deps_with_shadow(dispatch_deps, s3, kafka, flags)

    msg = make_message_create(message_id="msg_shadow_001")
    await handle_message_create(msg, deps)

    # Inline path: an observation must exist.
    row = await fresh_db.fetchrow(
        "SELECT external_id, source_channel FROM observations "
        "WHERE external_id = $1",
        f"discord:msg_shadow_001",
    )
    assert row is not None, "inline ingest() did not produce an observation"
    assert row["source_channel"] == "discord:message"

    # Shadow path: exactly one S3 PUT, exactly one Kafka publish.
    assert s3.put_if_absent.await_count == 1
    assert kafka.produce.await_count == 1

    # Inspect the envelope on the Kafka publish.
    _, kafka_kwargs = kafka.produce.await_args
    assert kafka_kwargs["topic"] == "ingestion.raw"
    assert kafka_kwargs["key"] == str(seeded_tenant).encode("utf-8")
    envelope = json.loads(kafka_kwargs["value"])
    assert envelope["source"] == "discord"
    assert envelope["ingress_kind"] == "gateway"
    assert envelope["tenant_id"] == str(seeded_tenant)
    assert envelope["ingress_metadata"]["event_type"] == "MESSAGE_CREATE"
    assert envelope["ingress_metadata"]["message_id"] == "msg_shadow_001"
    # short_guild_hash MUST appear; raw guild_id MUST NOT.
    assert "short_guild_hash" in envelope["ingress_metadata"]
    assert envelope["ingress_metadata"].get("short_guild_hash") != _TEST_GUILD_ID

    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.success"] == 1


# ---------------------------------------------------------------------
# 2. LOAD-BEARING SAFETY TEST.
# S3 failure must NOT break frame dispatch. The inline observation is
# still written; the function returns normally.
# ---------------------------------------------------------------------

async def test_gateway_shadow_failure_does_not_break_dispatch(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
    _shadow_mocks,
):
    s3, kafka, flags = _shadow_mocks
    s3.put_if_absent = AsyncMock(
        side_effect=RuntimeError("simulated S3 timeout"),
    )
    deps = _deps_with_shadow(dispatch_deps, s3, kafka, flags)

    msg = make_message_create(message_id="msg_shadow_fail_001")
    # Must NOT raise — the shadow helper catches everything.
    await handle_message_create(msg, deps)

    # Inline observation still landed.
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE external_id = $1",
        f"discord:msg_shadow_fail_001",
    )
    assert row is not None, (
        "inline ingest() must succeed even when shadow path fails"
    )

    # Kafka publish did NOT fire (shadow_write_raw raised at S3 step).
    assert kafka.produce.await_count == 0
    # Shadow failure metric incremented.
    metrics = shadow_write_module.get_metrics()
    assert metrics["shadow_write.failure.s3"] == 1
    assert metrics["shadow_write.success"] == 0


# ---------------------------------------------------------------------
# 3. Flag-disabled path — inline observation written, no shadow.
# ---------------------------------------------------------------------

async def test_gateway_shadow_disabled_by_flag(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
    _shadow_mocks,
):
    s3, kafka, flags = _shadow_mocks
    flags.get_bool = AsyncMock(return_value=False)
    deps = _deps_with_shadow(dispatch_deps, s3, kafka, flags)

    msg = make_message_create(message_id="msg_shadow_flagged_001")
    await handle_message_create(msg, deps)

    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE external_id = $1",
        f"discord:msg_shadow_flagged_001",
    )
    assert row is not None
    assert s3.put_if_absent.await_count == 0
    assert kafka.produce.await_count == 0


# ---------------------------------------------------------------------
# 4. Pre-M2 worker bootstrap (no shadow deps wired) — must still work
# as before M2.2. Confirms the optional deps default to None and the
# helper silently no-ops.
# ---------------------------------------------------------------------

async def test_gateway_with_no_shadow_deps_wired_is_no_op(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
):
    # dispatch_deps fixture does NOT set s3_raw_client / kafka_producer /
    # tenant_flags. Confirm the default is None.
    assert dispatch_deps.s3_raw_client is None
    assert dispatch_deps.kafka_producer is None
    assert dispatch_deps.tenant_flags is None

    msg = make_message_create(message_id="msg_no_shadow_001")
    await handle_message_create(msg, dispatch_deps)

    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE external_id = $1",
        f"discord:msg_no_shadow_001",
    )
    assert row is not None


# ---------------------------------------------------------------------
# 5. Re-serialisation round-trip property.
#
# The Gateway is a parsed-dict surface — frames arrive as Python dicts
# after the WSS client decodes the JSON envelope. _maybe_shadow_write_
# gateway re-serialises the dict to canonical bytes via
# `orjson.dumps(..., option=OPT_SORT_KEYS)` and writes THOSE bytes to
# S3.
#
# N2 (replay-from-raw, per HLD §"Migration Path") and content_hash
# dedup both depend on a property the LLD does not currently state:
# the canonical bytes the shadow path writes can be replayed through
# the existing Discord message handler and yield an ObservationDraft
# byte-identical to the live in-memory dispatch.
#
# If this property breaks (e.g., orjson silently drops a field, or
# coerces a snowflake string to an int, or alters number escaping),
# the M6 replay path would write divergent observations on replay.
# This test pins the property so a future orjson bump or canonical-
# form change cannot regress it silently. See pyproject.toml note
# alongside the orjson minor pin.
# ---------------------------------------------------------------------

async def test_gateway_shadow_body_round_trips_through_handler():
    """Production code path: invoke `_maybe_shadow_write_gateway`,
    capture the bytes the S3 mock received, replay them through
    `handle_discord_message`, and assert the resulting
    `ObservationDraft` is equal to the live dispatch's draft.

    No DB / fresh_db dependency — this is a pure handler-replay
    property and exercises no observations table.
    """
    msg = make_message_create(
        message_id="msg_replay_001",
        content="hello replay 🚀",
        mentions=[{"id": "user_mention_001", "username": "alice"}],
        attachments=[{"id": "att_001", "filename": "doc.pdf"}],
    )
    tenant_id = UUID("11111111-1111-1111-1111-111111111111")

    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)
    flags = MagicMock()
    flags.get_bool = AsyncMock(return_value=True)

    deps = DispatchDeps(
        pool=MagicMock(),  # shadow path does NOT touch the pool
        tenant_resolver=MagicMock(),
        actor_repo=None,
        alias_repo=None,
        embedder=None,
        application_id=None,
        s3_raw_client=s3,
        kafka_producer=kafka,
        tenant_flags=flags,
    )

    shadow_write_module.reset_metrics()
    await _maybe_shadow_write_gateway(
        deps,
        tenant_id=tenant_id,
        message=msg,
        guild_id=msg["guild_id"],
    )

    # The shadow body is positional arg 1 of put_if_absent(key, body).
    args, _ = s3.put_if_absent.await_args
    shadow_body: bytes = args[1]
    assert isinstance(shadow_body, bytes)

    # Replay: parse the canonical bytes and feed the dict back through
    # the registered Discord MESSAGE_CREATE handler.
    replayed_msg = json.loads(shadow_body)
    live_draft = await handle_discord_message(msg, {})
    replayed_draft = await handle_discord_message(replayed_msg, {})

    # Equality of ObservationDraft (dataclass) covers every field a
    # downstream observation row would carry: source_channel,
    # content_text, content (incl. metadata + short_guild_hash),
    # occurred_at, trust_tier, kind, source_actor_ref, external_id,
    # entities_hint, raw_payload. If orjson or the canonical form ever
    # mutates a value type (str→int, list→tuple, etc.), this assertion
    # fires before the regression reaches production replay.
    assert live_draft == replayed_draft, (
        "canonical-JSON re-serialisation must round-trip losslessly "
        "through the Discord message handler — otherwise M6's "
        "replay-from-raw guarantee is broken"
    )
    # Belt-and-braces: the external_id (dedup key) must match exactly.
    assert live_draft.external_id == replayed_draft.external_id == (
        "discord:msg_replay_001"
    )
