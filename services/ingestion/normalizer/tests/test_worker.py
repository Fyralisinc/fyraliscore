"""M2.3 — Normalizer worker happy-path and per-message correctness.

Tests `_normalize_one` directly so we don't need a real Kafka.
The full consumer loop is covered in
`test_worker_cooperative_sticky_rebalance.py` against a testcontainers
broker.
"""
from __future__ import annotations

import datetime as dt
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import orjson
import pytest

from services.ingestion.normalizer import worker as worker_module
from services.ingestion.normalizer.models import NormalizedEnvelope
from services.ingestion.raw_tier.envelope import RawEnvelope


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _slack_payload(channel: str = "C01ALICE", text: str = "hi") -> dict:
    """Minimal valid Slack-webhook payload shape for the registered
    handler at services/ingestion/handlers/slack.py."""
    return {
        "event": {
            "type": "message",
            "channel": channel,
            "user": "U01ALICE",
            "text": text,
            "ts": "1747483200.001000",
            "team": "T01ACME",
        },
    }


def _envelope_for(
    payload: dict,
    *,
    tenant: UUID,
    source: str = "slack",
    ingress_kind: str = "webhook",
) -> tuple[bytes, bytes, str]:
    """Build a (raw_body_bytes, envelope_bytes, s3_key) triple.

    s3_key is deterministic so the test's S3 stub can be pre-loaded
    with the same key the envelope will request.
    """
    raw_body = orjson.dumps(payload)
    content_hash = "a" * 40
    # Prefix segment must equal content_hash[:2] per the M2.4 invariant.
    s3_key = f"dev/{source}/{tenant}/2026-05/{content_hash[:2]}/{content_hash}.json"
    envelope = RawEnvelope(
        source=source,
        tenant_id=tenant,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=_NOW,
        ingress_kind=ingress_kind,
        ingress_metadata={"delivery_id": "deliv-1"},
        idem_hints={"hint": "x"},
    )
    return raw_body, orjson.dumps(envelope.model_dump(mode="json")), s3_key


@pytest.fixture(autouse=True)
def _reset_metrics():
    worker_module.reset_metrics()


@pytest.fixture
def _producer_stub():
    producer = MagicMock()
    producer.produce = AsyncMock(return_value=None)
    producer.flush = AsyncMock(return_value=0)
    return producer


@pytest.fixture
def _s3_stub():
    """A tiny in-memory S3 stub: `put` writes to a dict, `get`
    fetches from it. Async surface matches the real S3Client.get.
    """
    storage: dict[str, bytes] = {}
    stub = MagicMock()

    async def _get(key: str) -> bytes:
        return storage[key]

    stub.get = AsyncMock(side_effect=_get)
    stub._storage = storage
    return stub


# ---------------------------------------------------------------------
# 1. Happy path — Slack webhook envelope → normalized envelope.
# ---------------------------------------------------------------------

async def test_normalize_slack_webhook_produces_normalized_envelope(
    _producer_stub, _s3_stub,
):
    tenant = uuid4()
    raw_body, envelope_bytes, s3_key = _envelope_for(
        _slack_payload(text="hello m2.3"),
        tenant=tenant,
    )
    _s3_stub._storage[s3_key] = raw_body

    produced = await worker_module._normalize_one(
        envelope_bytes, _s3_stub, _producer_stub,
    )

    assert produced is True
    assert _producer_stub.produce.await_count == 1
    _, kwargs = _producer_stub.produce.await_args
    assert kwargs["topic"] == "ingestion.normalized"
    assert kwargs["key"] == str(tenant).encode("utf-8")

    norm = NormalizedEnvelope.model_validate(json.loads(kwargs["value"]))
    assert norm.source == "slack"
    assert norm.ingress_kind == "webhook"
    assert norm.tenant_id == tenant
    assert norm.raw_s3_key == s3_key
    assert norm.source_channel == "slack:message"
    assert norm.content_text == "hello m2.3"
    assert norm.external_id == "C01ALICE:1747483200.001000"
    assert norm.trust_tier == "attested_agent"
    assert norm.ingress_metadata == {"delivery_id": "deliv-1"}
    assert norm.idem_hints == {"hint": "x"}


# ---------------------------------------------------------------------
# 2. Gmail Pub/Sub envelope — out-of-scope for M2 (no handler).
# ---------------------------------------------------------------------

async def test_normalize_gmail_pubsub_is_skipped_with_metric(
    _producer_stub, _s3_stub,
):
    tenant = uuid4()
    # The payload doesn't matter — channel_mapping returns None
    # before the S3 fetch.
    raw_body, envelope_bytes, s3_key = _envelope_for(
        {"emailAddress": "alice@acme.com", "historyId": "12345"},
        tenant=tenant,
        source="gmail",
        ingress_kind="pubsub",
    )

    produced = await worker_module._normalize_one(
        envelope_bytes, _s3_stub, _producer_stub,
    )

    assert produced is False
    assert _producer_stub.produce.await_count == 0
    # S3 was NOT fetched — short-circuit before the network call.
    assert _s3_stub.get.await_count == 0

    metrics = worker_module.get_metrics()
    assert metrics["normalizer.unsupported_combination"] == 1


# ---------------------------------------------------------------------
# 3. Handler raises ValidationError (bad payload) — bubbles up so the
# loop records parse_failure. Confirms the loop-vs-helper contract.
# ---------------------------------------------------------------------

async def test_normalize_handler_validation_error_bubbles(
    _producer_stub, _s3_stub,
):
    tenant = uuid4()
    # Slack handler requires `event.text` to be a string; omit it.
    bad_payload: dict[str, Any] = {
        "event": {
            "type": "message",
            "channel": "C01ALICE",
            "user": "U01ALICE",
            "ts": "1747483200.001000",
        },
    }
    raw_body, envelope_bytes, s3_key = _envelope_for(
        bad_payload, tenant=tenant,
    )
    _s3_stub._storage[s3_key] = raw_body

    with pytest.raises(Exception):
        await worker_module._normalize_one(
            envelope_bytes, _s3_stub, _producer_stub,
        )
    assert _producer_stub.produce.await_count == 0


# ---------------------------------------------------------------------
# 4. Discord MESSAGE_CREATE — confirms the gateway ingress-kind
# routes through the right handler ("discord:message", not
# "discord:interaction").
# ---------------------------------------------------------------------

async def test_normalize_discord_gateway_routes_to_message_handler(
    _producer_stub, _s3_stub,
):
    tenant = uuid4()
    discord_payload = {
        "id": "msg_norm_001",
        "channel_id": "channel_xyz",
        "guild_id": "1504477009927999569",
        "content": "hi from m2.3",
        "timestamp": "2026-05-17T12:00:00.000+00:00",
        "author": {"id": "user_001", "username": "tester", "bot": False},
        "attachments": [],
        "mentions": [],
    }
    raw_body, envelope_bytes, s3_key = _envelope_for(
        discord_payload, tenant=tenant,
        source="discord", ingress_kind="gateway",
    )
    _s3_stub._storage[s3_key] = raw_body

    produced = await worker_module._normalize_one(
        envelope_bytes, _s3_stub, _producer_stub,
    )

    assert produced is True
    _, kwargs = _producer_stub.produce.await_args
    norm = NormalizedEnvelope.model_validate(json.loads(kwargs["value"]))
    assert norm.source_channel == "discord:message"
    assert norm.external_id == "discord:msg_norm_001"


# ---------------------------------------------------------------------
# 5. NormalizedEnvelope is byte-stable for byte-equal logical input.
# (Property the M2.4 invariants test will pin too — included here to
# catch regression at the worker boundary.)
# ---------------------------------------------------------------------

async def test_normalize_envelope_byte_stable_for_equal_input(
    _producer_stub, _s3_stub,
):
    """N2 (replay-from-raw) requires: two normalizations of a
    byte-equal raw envelope produce byte-equal normalized envelopes
    on the wire, modulo the `normalized_at` wall-clock stamp.

    Tested in TWO forms:

      1. Byte-equality of `orjson.dumps(model_dump_after_strip)`
         — the load-bearing N2 contract: the bytes the writer (M2.4+)
         consumes from `ingestion.normalized` are byte-identical
         across replays.

      2. Dict-equality of the parsed envelopes (informational; if (1)
         passes, (2) follows by construction, but the dict form
         localises the failure mode if they ever diverge).
    """
    tenant = uuid4()
    raw_body, envelope_bytes, s3_key = _envelope_for(
        _slack_payload(text="stable"), tenant=tenant,
    )
    _s3_stub._storage[s3_key] = raw_body

    await worker_module._normalize_one(
        envelope_bytes, _s3_stub, _producer_stub,
    )
    await worker_module._normalize_one(
        envelope_bytes, _s3_stub, _producer_stub,
    )

    assert _producer_stub.produce.await_count == 2
    first_bytes = _producer_stub.produce.await_args_list[0][1]["value"]
    second_bytes = _producer_stub.produce.await_args_list[1][1]["value"]
    assert isinstance(first_bytes, bytes)
    assert isinstance(second_bytes, bytes)

    # Strip the wall-clock stamp then re-serialise via the SAME
    # canonical orjson form the writer uses; assert byte equality.
    first_dict = json.loads(first_bytes)
    second_dict = json.loads(second_bytes)
    first_dict.pop("normalized_at")
    second_dict.pop("normalized_at")

    first_canonical = orjson.dumps(first_dict, option=orjson.OPT_SORT_KEYS)
    second_canonical = orjson.dumps(second_dict, option=orjson.OPT_SORT_KEYS)
    assert first_canonical == second_canonical, (
        "BYTE equality required for N2 replay-from-raw: two "
        "normalizations of the same raw envelope must produce "
        "byte-identical canonical JSON (sans normalized_at). "
        "If this fires, a non-deterministic field crept into the "
        "normalization path."
    )
    # Dict-equality follows but is asserted for failure-mode clarity.
    assert first_dict == second_dict
