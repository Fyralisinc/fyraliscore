"""Discord Gateway live-via-Kafka cutover tests (M-Validate-Concurrent WS1).

Verifies the cutover branch added to
`services/integrations/discord/gateway/dispatch.py:handle_message_create`,
parallel to the M5.3 webhook-router cutover for slack/github:

  - `ingestion.kafka_path_enabled=TRUE` → the frame is published to
    `ingestion.raw` (ingress_kind="gateway") and inline `ingest()` is
    SKIPPED (no observation is written in-process; the writer pool would
    produce it downstream).
  - Publish failure under the flag → graceful fallback to inline
    `ingest()` (the message is never dropped); the M2 shadow write is
    suppressed (no double-publish).
  - Flag FALSE (default) → inline path unchanged, M2 shadow still runs.

Mirrors `test_dispatch_shadow.py`'s fixtures + real-Postgres approach.
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest

from services.ingestion import shadow_write as shadow_write_module
from services.ingestion.feature_flags import (
    KAFKA_PATH_ENABLED,
    SHADOW_WRITE_ENABLED,
)
from services.integrations.discord.gateway.dispatch import (
    DispatchDeps,
    handle_message_create,
)
from services.integrations.discord.gateway.tests.conftest import (
    make_message_create,
)


pytestmark = pytest.mark.integration


def _flags(*, kafka_path: bool, shadow: bool = True) -> MagicMock:
    """Flags mock returning per-flag values so cutover and shadow can be
    controlled independently."""
    flags = MagicMock()

    async def _get_bool(tenant_id, flag_name, *, default):  # noqa: ANN001
        if flag_name == KAFKA_PATH_ENABLED:
            return kafka_path
        if flag_name == SHADOW_WRITE_ENABLED:
            return shadow
        return default

    flags.get_bool = AsyncMock(side_effect=_get_bool)
    return flags


def _deps(base: DispatchDeps, s3, kafka, flags) -> DispatchDeps:
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


def _s3_kafka(s3_raises: bool = False):
    s3 = MagicMock()
    if s3_raises:
        s3.put_if_absent = AsyncMock(side_effect=RuntimeError("s3 down"))
    else:
        s3.put_if_absent = AsyncMock(return_value=None)
    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    kafka.flush = AsyncMock(return_value=0)
    shadow_write_module.reset_metrics()
    return s3, kafka


# ---------------------------------------------------------------------
# 1. Flag TRUE → publish to Kafka, SKIP inline (no observation written).
# ---------------------------------------------------------------------
async def test_cutover_publishes_and_skips_inline(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
):
    s3, kafka = _s3_kafka()
    deps = _deps(dispatch_deps, s3, kafka, _flags(kafka_path=True))

    msg = make_message_create(message_id="msg_cut_001")
    await handle_message_create(msg, deps)

    # Inline ingest() was SKIPPED — no observation produced in-process.
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE external_id = $1",
        "discord:msg_cut_001",
    )
    assert row is None, "cutover must skip inline ingest()"

    # Exactly one publish to ingestion.raw with ingress_kind=gateway.
    assert s3.put_if_absent.await_count == 1
    assert kafka.produce.await_count == 1
    _, kw = kafka.produce.await_args
    assert kw["topic"] == "ingestion.raw"
    assert kw["key"] == str(seeded_tenant).encode("utf-8")
    env = json.loads(kw["value"])
    assert env["source"] == "discord"
    assert env["ingress_kind"] == "gateway"
    assert env["ingress_metadata"]["message_id"] == "msg_cut_001"
    assert shadow_write_module.get_metrics()["shadow_write.success"] == 1


# ---------------------------------------------------------------------
# 2. Flag TRUE + publish fails → fallback to inline; observation lands;
#    M2 shadow suppressed (no second publish attempt).
# ---------------------------------------------------------------------
async def test_cutover_failure_falls_back_to_inline(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
):
    s3, kafka = _s3_kafka(s3_raises=True)
    deps = _deps(dispatch_deps, s3, kafka, _flags(kafka_path=True))

    msg = make_message_create(message_id="msg_cut_fb_001")
    await handle_message_create(msg, deps)  # must NOT raise

    # Inline observation landed (message not dropped).
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE external_id = $1",
        "discord:msg_cut_fb_001",
    )
    assert row is not None, "fallback must write the inline observation"

    # The cutover publish failed at S3; the suppressed M2 shadow means no
    # Kafka publish ever fired.
    assert kafka.produce.await_count == 0
    assert shadow_write_module.get_metrics()["shadow_write.failure.s3"] == 1


# ---------------------------------------------------------------------
# 3. Flag FALSE (default) → inline path unchanged, M2 shadow still runs.
# ---------------------------------------------------------------------
async def test_flag_false_uses_inline_and_shadow(
    dispatch_deps: DispatchDeps,
    seeded_tenant: UUID,
    fresh_db: asyncpg.Pool,
):
    s3, kafka = _s3_kafka()
    deps = _deps(dispatch_deps, s3, kafka, _flags(kafka_path=False, shadow=True))

    msg = make_message_create(message_id="msg_cut_off_001")
    await handle_message_create(msg, deps)

    # Inline observation present.
    row = await fresh_db.fetchrow(
        "SELECT external_id FROM observations WHERE external_id = $1",
        "discord:msg_cut_off_001",
    )
    assert row is not None

    # M2 shadow ran (post-inline) — one publish with ingress_kind=gateway.
    assert kafka.produce.await_count == 1
    _, kw = kafka.produce.await_args
    assert json.loads(kw["value"])["ingress_kind"] == "gateway"
