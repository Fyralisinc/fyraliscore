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

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import orjson

from services.ingest.ingestion.fetchers.gmail import _build_record
from services.ingest.ingestion.handlers.gmail import handle_gmail
from services.ingest.integrations.gmail import fetcher
from services.ingest.integrations.gmail.client import GoogleApiError
from services.ingest.integrations.gmail.fetcher import (
    _GmailDrainContext,
    _collect_history_message_ids,
    _drain_message_ids,
    _publish_gmail_message_raw,
)


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


class _FakeDrainGmail:
    def __init__(
        self,
        *,
        history_pages: list[dict[str, Any]] | None = None,
        messages: dict[str, dict[str, Any]] | None = None,
        failing_message_ids: set[str] | None = None,
    ) -> None:
        self.history_pages = list(history_pages or [])
        self.messages = dict(messages or {})
        self.failing_message_ids = set(failing_message_ids or set())
        self.history_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []

    async def history_list(self, **kwargs: Any) -> dict[str, Any]:
        self.history_calls.append(kwargs)
        return self.history_pages.pop(0)

    async def get_message(self, *, user_email: str, scope: str, message_id: str) -> dict[str, Any]:
        self.get_calls.append(
            {"user_email": user_email, "scope": scope, "message_id": message_id}
        )
        if message_id in self.failing_message_ids:
            raise GoogleApiError("not found", status=404)
        return self.messages[message_id]


def _drain_context(fake_gmail: _FakeDrainGmail, *, cutover_enabled: bool) -> _GmailDrainContext:
    return _GmailDrainContext(
        pool=object(),
        gmail=fake_gmail,  # type: ignore[arg-type]
        tenant_id=_TENANT,
        gmail_installation_id=_INSTALL,
        email_address="alice@x.com",
        read_path="push",
        scope_alias="gmail.metadata",
        scope_long="https://www.googleapis.com/auth/gmail.metadata",
        cutover_enabled=cutover_enabled,
        s3_raw_client=object(),
        kafka_producer=object(),
    )


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
    # Per-source raw topic (source-isolation): gmail poll -> gmail lane.
    assert kw["topic"] == "ingestion.raw.gmail"
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


async def test_collect_history_message_ids_pages_until_bookmark():
    fake = _FakeDrainGmail(
        history_pages=[
            {
                "history": [
                    {
                        "messagesAdded": [
                            {"message": {"id": "m1"}},
                            {"message": {}},
                            None,
                        ],
                    },
                ],
                "historyId": "101",
                "nextPageToken": "p2",
            },
            {
                "history": [{"messagesAdded": [{"message": {"id": "m2"}}]}],
                "historyId": 202,
            },
        ],
    )

    ids, history_id = await _collect_history_message_ids(
        gmail=fake,  # type: ignore[arg-type]
        email_address="alice@x.com",
        scope_long="scope",
        start_history_id="100",
    )

    assert ids == ["m1", "m2"]
    assert history_id == "202"
    assert fake.history_calls == [
        {
            "user_email": "alice@x.com",
            "scope": "scope",
            "start_history_id": "100",
            "page_token": None,
        },
        {
            "user_email": "alice@x.com",
            "scope": "scope",
            "start_history_id": "100",
            "page_token": "p2",
        },
    ]


async def test_drain_message_ids_cutover_success_audits_without_inline(monkeypatch):
    fake = _FakeDrainGmail(messages={"m1": {"id": "m1"}})
    published: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []

    async def _publish(**kwargs: Any) -> bool:
        published.append(kwargs)
        return True

    async def _audit(**kwargs: Any) -> None:
        audits.append(kwargs)

    async def _inline(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("cutover success must not dispatch inline")

    monkeypatch.setattr(fetcher, "_publish_gmail_message_raw", _publish)
    monkeypatch.setattr(fetcher, "_write_gmail_read_audit", _audit)
    monkeypatch.setattr(fetcher, "_dispatch_gmail_resource_inline", _inline)

    counters = await _drain_message_ids(_drain_context(fake, cutover_enabled=True), ["m1"])

    assert counters.ingested == 1
    assert counters.deduped == 0
    assert published[0]["message_resource"] == {"id": "m1"}
    assert audits[0]["message_id"] == "m1"


async def test_drain_message_ids_cutover_failure_falls_back_to_inline(monkeypatch):
    fake = _FakeDrainGmail(messages={"m1": {"id": "m1"}})
    audits: list[dict[str, Any]] = []
    inline_resources: list[dict[str, Any]] = []

    async def _publish(**_kwargs: Any) -> bool:
        return False

    async def _audit(**kwargs: Any) -> None:
        audits.append(kwargs)

    async def _inline(_ctx: _GmailDrainContext, resource: dict[str, Any]) -> dict[str, Any]:
        inline_resources.append(resource)
        return {"deduped": True}

    monkeypatch.setattr(fetcher, "_publish_gmail_message_raw", _publish)
    monkeypatch.setattr(fetcher, "_write_gmail_read_audit", _audit)
    monkeypatch.setattr(fetcher, "_dispatch_gmail_resource_inline", _inline)

    counters = await _drain_message_ids(_drain_context(fake, cutover_enabled=True), ["m1"])

    assert counters.ingested == 0
    assert counters.deduped == 1
    assert inline_resources == [{"id": "m1"}]
    assert audits[0]["message_id"] == "m1"


async def test_drain_message_ids_get_failure_skips_message(monkeypatch):
    fake = _FakeDrainGmail(
        messages={"good": {"id": "good"}},
        failing_message_ids={"missing"},
    )
    audits: list[dict[str, Any]] = []

    async def _audit(**kwargs: Any) -> None:
        audits.append(kwargs)

    async def _inline(_ctx: _GmailDrainContext, resource: dict[str, Any]) -> dict[str, Any]:
        return {"deduped": False, "id": resource["id"]}

    monkeypatch.setattr(fetcher, "_write_gmail_read_audit", _audit)
    monkeypatch.setattr(fetcher, "_dispatch_gmail_resource_inline", _inline)

    counters = await _drain_message_ids(
        _drain_context(fake, cutover_enabled=False),
        ["missing", "good"],
    )

    assert counters.ingested == 1
    assert counters.deduped == 0
    assert audits == [
        {
            "tenant_id": _TENANT,
            "gmail_installation_id": _INSTALL,
            "email_address": "alice@x.com",
            "message_id": "good",
            "scope_alias": "gmail.metadata",
            "read_path": "push",
        }
    ]
