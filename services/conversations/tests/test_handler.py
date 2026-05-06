"""Unit tests for the probe handler — pure-function path that doesn't
require a Postgres connection. The repo is replaced with a fake
in-memory implementation; the QueryHandler is left None so the
free-form Ask path exercises the deterministic fallback.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest

from services.conversations.handler import ProbeHandler, ProbeRequest
from services.conversations.repo import (
    CardConversation,
    CardExchange,
    ConversationArchivedError,
)


class _FakeRepo:
    """In-memory stand-in for ConversationRepo. Mirrors the surface
    the handler uses: get_or_create + append_exchange. No DB, no I/O.
    """

    def __init__(self):
        self._convs: dict[tuple[UUID, UUID, UUID], CardConversation] = {}
        self._exchanges: dict[UUID, list[CardExchange]] = {}

    async def get_or_create(self, *, tenant_id, actor_id, card_id):
        key = (tenant_id, actor_id, card_id)
        if key in self._convs:
            return self._convs[key]
        conv = CardConversation(
            id=uuid4(),
            tenant_id=tenant_id,
            actor_id=actor_id,
            card_id=card_id,
            created_at=datetime.now(timezone.utc),
            last_probed_at=None,
            archived_at=None,
            archive_reason=None,
        )
        self._convs[key] = conv
        self._exchanges[conv.id] = []
        return conv

    async def append_exchange(
        self, *, conversation, probe_kind, probe_id, probe_action,
        probe_text, response_html, follow_ups, latency_ms=None,
    ):
        if conversation.archived_at is not None:
            raise ConversationArchivedError(conversation.id)
        ex = CardExchange(
            id=uuid4(),
            conversation_id=conversation.id,
            probe_kind=probe_kind,
            probe_id=probe_id,
            probe_action=probe_action,
            probe_text=probe_text,
            response_html=response_html,
            follow_ups=list(follow_ups),
            created_at=datetime.now(timezone.utc),
            latency_ms=latency_ms,
        )
        self._exchanges[conversation.id].append(ex)
        if probe_kind == "phrase" and probe_id and probe_id not in conversation.probed_phrase_ids:
            conversation.probed_phrase_ids.append(probe_id)
        if probe_kind == "chip" and probe_id and probe_id not in conversation.used_chip_ids:
            conversation.used_chip_ids.append(probe_id)
        conversation.last_probed_at = ex.created_at
        return ex


class _NullPool:
    """Pool stand-in. _fetch_recommendation will catch the AttributeError
    and return None — which is exactly the fallback the handler is
    designed to tolerate."""

    def acquire(self):  # noqa: D401
        raise RuntimeError("no DB in unit tests")


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


@pytest.fixture
def handler():
    repo = _FakeRepo()
    return ProbeHandler(repo=repo, pool=_NullPool(), query_handler=None), repo


def test_phrase_probe_records_exchange_and_marks_phrase_probed(handler):
    h, repo = handler
    tid, aid, cid = uuid4(), uuid4(), uuid4()
    resp = _run(
        h.probe(
            ProbeRequest(
                tenant_id=tid, actor_id=aid, card_id=cid,
                kind="phrase", probe_id="h-card-three-customers-1",
            )
        )
    )
    assert resp.exchange.probe_kind == "phrase"
    assert resp.exchange.probe_action == "You clicked"
    # The pretty text strips the prefix and slug index out of the id.
    assert "three customers" in resp.exchange.probe_text
    assert "<p>" in resp.exchange.response_html
    # The conversation now records the probed phrase id so the UI can
    # mark it `.probed` across reloads.
    conv = list(repo._convs.values())[0]
    assert "h-card-three-customers-1" in conv.probed_phrase_ids


def test_chip_probe_uses_canned_response_per_suffix(handler):
    h, repo = handler
    tid, aid, cid = uuid4(), uuid4(), uuid4()
    resp = _run(
        h.probe(
            ProbeRequest(
                tenant_id=tid, actor_id=aid, card_id=cid,
                kind="chip", probe_id=f"{cid}:contradicting",
            )
        )
    )
    assert resp.exchange.probe_action == "You probed"
    # Body contains the templated explanation for "contradicting".
    assert "scoped without referencing" in resp.exchange.response_html
    conv = list(repo._convs.values())[0]
    assert f"{cid}:contradicting" in conv.used_chip_ids


def test_ask_probe_falls_back_when_query_handler_absent(handler):
    h, _ = handler
    tid, aid, cid = uuid4(), uuid4(), uuid4()
    resp = _run(
        h.probe(
            ProbeRequest(
                tenant_id=tid, actor_id=aid, card_id=cid,
                kind="ask", query="What if I deferred this for two weeks?",
            )
        )
    )
    assert resp.exchange.probe_action == "You asked"
    assert resp.exchange.probe_text == "What if I deferred this for two weeks?"
    assert "primary tradeoff" in resp.exchange.response_html


def test_phrase_probe_requires_probe_id(handler):
    h, _ = handler
    tid, aid, cid = uuid4(), uuid4(), uuid4()
    with pytest.raises(ValueError):
        _run(
            h.probe(
                ProbeRequest(
                    tenant_id=tid, actor_id=aid, card_id=cid,
                    kind="phrase", probe_id=None,
                )
            )
        )


def test_ask_probe_requires_query(handler):
    h, _ = handler
    tid, aid, cid = uuid4(), uuid4(), uuid4()
    with pytest.raises(ValueError):
        _run(
            h.probe(
                ProbeRequest(
                    tenant_id=tid, actor_id=aid, card_id=cid,
                    kind="ask", query="   ",
                )
            )
        )


def test_repeated_phrase_probe_does_not_duplicate_in_probed_list(handler):
    h, repo = handler
    tid, aid, cid = uuid4(), uuid4(), uuid4()
    pid = "h-card-foo-1"
    _run(h.probe(ProbeRequest(
        tenant_id=tid, actor_id=aid, card_id=cid, kind="phrase", probe_id=pid,
    )))
    _run(h.probe(ProbeRequest(
        tenant_id=tid, actor_id=aid, card_id=cid, kind="phrase", probe_id=pid,
    )))
    conv = list(repo._convs.values())[0]
    assert conv.probed_phrase_ids.count(pid) == 1
