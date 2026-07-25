"""Tests for services/ingest/integrations/signal/gateway/dispatch.py.

The live-dispatch bridge: filtering, the kafka-first cutover (shadow-write to
ingestion.raw.signal, ingress_kind=gateway), and the inline fallback. The
shadow-write / inline calls are monkeypatched to capture the routing decision
(the underlying shadow_write_raw / core.ingest are exercised elsewhere).
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.integrations.signal.gateway import dispatch as D
from services.ingest.integrations.signal.gateway.dispatch import (
    DispatchDeps,
    handle_update,
)


pytestmark = pytest.mark.asyncio


class _Flags:
    def __init__(self, enabled: bool):
        self._enabled = enabled

    async def kafka_path_enabled(self, _tenant_id):
        return self._enabled


def _update(mid=1001, *, out=False):
    return {
        "event": "new_message",
        "message": {
            "id": mid, "date": 1781000000, "edit_date": None,
            "message": "live hello", "out": out, "from_id": {"user_id": 7},
        },
        "thread_id": 42, "thread_kind": "group", "thread_title": "Eng",
    }


def _deps(flags, *, with_plane=True):
    return DispatchDeps(
        pool=None, tenant_id=uuid4(), installation_id="inst-uuid",
        s3_raw_client=object() if with_plane else None,
        kafka_producer=object() if with_plane else None,
        tenant_flags=flags,
    )


async def test_cutover_shadow_writes_when_flag_enabled(monkeypatch):
    captured = {}

    async def _fake_shadow(*, tenant_id, source, ingress_kind, raw_body,
                           s3_client, kafka_producer, ingress_metadata=None,
                           **kw):
        captured.update(source=source, ingress_kind=ingress_kind,
                        raw_body=raw_body)
        return "s3://key"

    async def _fake_ingest(*a, **k):  # must NOT be called on the cutover path
        captured["inline"] = True

    monkeypatch.setattr(D, "shadow_write_raw", _fake_shadow)
    monkeypatch.setattr(D, "ingest", _fake_ingest)

    await handle_update(_update(), _deps(_Flags(True)))

    assert captured["source"] == "signal"
    assert captured["ingress_kind"] == "gateway"
    assert b"live hello" in captured["raw_body"]  # canonical record body
    assert "inline" not in captured  # cutover returned; no inline ingest


async def test_inline_when_flag_disabled(monkeypatch):
    called = {}

    async def _fake_ingest(channel, record, **k):
        called.update(channel=channel, record=record)
        class _R:  # noqa: D401 - minimal IngestResult stand-in
            deduped = False
        return _R()

    async def _fake_shadow(**k):
        called["shadow"] = True

    monkeypatch.setattr(D, "ingest", _fake_ingest)
    monkeypatch.setattr(D, "shadow_write_raw", _fake_shadow)

    await handle_update(_update(), _deps(_Flags(False)))
    assert called["channel"] == "signal:message"
    assert called["record"]["id"] == 1001


async def test_outgoing_message_skipped(monkeypatch):
    called = {}

    async def _fake_ingest(*a, **k):
        called["inline"] = True

    async def _fake_shadow(**k):
        called["shadow"] = True

    monkeypatch.setattr(D, "ingest", _fake_ingest)
    monkeypatch.setattr(D, "shadow_write_raw", _fake_shadow)

    # out=True (our own send) → dropped, no ingest / shadow-write.
    await handle_update(_update(out=True), _deps(_Flags(True)))
    assert called == {}


async def test_non_message_event_skipped(monkeypatch):
    called = {}
    monkeypatch.setattr(D, "ingest", lambda *a, **k: called.setdefault("x", 1))
    await handle_update({"event": "typing"}, _deps(_Flags(True)))
    assert called == {}


async def test_inline_failure_reports_not_durable(monkeypatch):
    async def _failed_ingest(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(D, "ingest", _failed_ingest)

    durable = await handle_update(
        _update(),
        _deps(_Flags(False), with_plane=False),
    )

    assert durable is False
