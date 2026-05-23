"""Gmail live-via-Kafka cutover tests (M-Validate-Concurrent WS2).

The cutover (`_publish_gmail_message_raw`, wired into
`drain_mailbox_history` under `ingestion.kafka_path_enabled`) publishes a
fetched Gmail message to `ingestion.raw` with ingress_kind="poll" instead
of ingesting inline. The load-bearing property is EXTERNAL-ID PARITY: the
published body, replayed through the `gmail:` handler with headers={} (as
the normalizer does for live ingress), must derive the SAME external_id as
the M6.3 backfill `_build_record` path — otherwise cross-path dedup can't
collapse a backfilled message and its live "poll" twin.

Pure unit tests — no DB, no Kafka (mocks at the s3/kafka boundary).
"""
from __future__ import annotations

import base64
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import orjson

from services.ingestion.fetchers.gmail import _build_record
from services.ingestion.handlers.gmail import handle_gmail
from services.integrations.gmail.fetcher import _publish_gmail_message_raw


_INSTALL = UUID("cccccccc-2222-7777-8888-dddddddddddd")
_TENANT = UUID("aaaaaaaa-1111-7777-8888-bbbbbbbbbbbb")


def _resource(message_id: str = "<abc@mail>") -> dict[str, Any]:
    return {
        "id": "msg-1",
        "threadId": "thr-1",
        "labelIds": ["INBOX"],
        "snippet": "hi there",
        "internalDate": "1700000000000",
        "sizeEstimate": 1024,
        "payload": {
            "headers": [
                {"name": "Message-ID", "value": message_id},
                {"name": "From", "value": "Alice <alice@x.com>"},
                {"name": "To", "value": "bob@y.com"},
                {"name": "Subject", "value": "hello"},
            ],
        },
    }


def _mock_s3_kafka():
    s3 = MagicMock()
    s3.put_if_absent = AsyncMock(return_value=None)
    kafka = MagicMock()
    kafka.produce = AsyncMock(return_value=None)
    return s3, kafka


async def test_cutover_publishes_poll_envelope():
    s3, kafka = _mock_s3_kafka()
    ok = await _publish_gmail_message_raw(
        s3_raw_client=s3,
        kafka_producer=kafka,
        tenant_id=_TENANT,
        gmail_installation_id=_INSTALL,
        email_address="alice@x.com",
        scope_alias="gmail.metadata",
        message_resource=_resource(),
        read_path="push",
    )
    assert ok is True
    assert s3.put_if_absent.await_count == 1
    assert kafka.produce.await_count == 1

    _, kw = kafka.produce.await_args
    assert kw["topic"] == "ingestion.raw"
    assert kw["key"] == str(_TENANT).encode("utf-8")
    envelope = orjson.loads(kw["value"])
    assert envelope["source"] == "gmail"
    assert envelope["ingress_kind"] == "poll"
    assert envelope["tenant_id"] == str(_TENANT)


async def test_cutover_failure_returns_false():
    s3, kafka = _mock_s3_kafka()
    s3.put_if_absent = AsyncMock(side_effect=RuntimeError("s3 down"))
    ok = await _publish_gmail_message_raw(
        s3_raw_client=s3,
        kafka_producer=kafka,
        tenant_id=_TENANT,
        gmail_installation_id=_INSTALL,
        email_address="alice@x.com",
        scope_alias="gmail.metadata",
        message_resource=_resource(),
        read_path="push",
    )
    assert ok is False  # caller falls back to inline
    assert kafka.produce.await_count == 0


async def test_cutover_external_id_parity_with_backfill():
    """The published body (replayed via the handler with headers={}) and
    the backfill `_build_record` body must produce the SAME external_id."""
    msg_resource = _resource(message_id="<parity@mail>")
    s3, kafka = _mock_s3_kafka()

    await _publish_gmail_message_raw(
        s3_raw_client=s3,
        kafka_producer=kafka,
        tenant_id=_TENANT,
        gmail_installation_id=_INSTALL,
        email_address="alice@x.com",
        scope_alias="gmail.metadata",
        message_resource=msg_resource,
        read_path="push",
    )
    # The S3 body is the bare record dict (put_if_absent(key, body)).
    args, _ = s3.put_if_absent.await_args
    cutover_body = orjson.loads(args[1])

    # The normalizer passes the bare body with headers={} for live ingress.
    cutover_draft = await handle_gmail(cutover_body, {})

    # Backfill path: _build_record for the same resource, run through the
    # same handler (the normalizer unwraps {record} → payload, headers={}).
    backfill_body = _build_record(
        message_resource=msg_resource,
        mailbox_email="alice@x.com",
        scope_alias="gmail.metadata",
        gmail_installation_id=str(_INSTALL),
        read_path="backfill",  # normalised to "poll" inside _build_record
    )
    backfill_draft = await handle_gmail(backfill_body, {})

    assert cutover_draft.external_id == backfill_draft.external_id
    # external_id is `gmail:{install}:{message_id}` (handler strips the
    # angle brackets from the Message-ID header).
    assert cutover_draft.external_id == f"gmail:{_INSTALL}:parity@mail"
    # And the dedup tuple (source_channel, external_id, occurred_at) matches.
    assert cutover_draft.source_channel == backfill_draft.source_channel
    assert cutover_draft.occurred_at == backfill_draft.occurred_at
