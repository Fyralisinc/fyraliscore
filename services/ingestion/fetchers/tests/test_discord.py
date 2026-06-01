"""Tests for services/ingestion/fetchers/discord.py (M6.6)."""
from __future__ import annotations

import pytest

from services.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingestion.fetchers import discord as dc
from services.ingestion.fetchers.discord import (
    DiscordCursor, SHARD_KIND_CHANNEL_WINDOW, fetch_page_discord,
)


pytestmark = pytest.mark.asyncio


class _FakeDC:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    async def get_messages(self, *, channel_id, before=None, after=None, limit=None):
        self.calls.append({"before": before, "after": after, "limit": limit})
        if not self.pages:
            return []
        return self.pages.pop(0)


class _Inst:
    def __getitem__(self, k):
        return "row"


def _patch(monkeypatch, fake):
    async def fake_open(install):
        async def close(): return None
        return fake, close
    monkeypatch.setattr(dc, "_open_discord_client", fake_open)


async def test_paginate_via_snowflake(monkeypatch):
    fake = _FakeDC([
        [{"id": str(i)} for i in range(2000000, 2000100)],  # 100 → continue
        [{"id": str(1000000)}],                              # < 100 → end
    ])
    _patch(monkeypatch, fake)
    r1 = await fetch_page_discord(
        _Inst(),
        {"shard_kind": SHARD_KIND_CHANNEL_WINDOW, "channel_id": "C",
         "guild_id": "G", "installation_id": "I"},
        cursor=None,
    )
    assert len(r1.records) == 100
    assert r1.end_of_data is False
    # Cursor's before_snowflake = oldest seen, which is "2000000".
    assert r1.next_cursor["before_snowflake"] == "2000000"

    r2 = await fetch_page_discord(
        _Inst(),
        {"channel_id": "C", "guild_id": "G", "installation_id": "I"},
        cursor=r1.next_cursor,
    )
    assert r2.end_of_data is True
    # The fetcher passed before=cursor.before_snowflake.
    assert fake.calls[1]["before"] == "2000000"


async def test_record_envelope_shape(monkeypatch):
    """A27.3 — records are emitted in the MESSAGE_CREATE shape the
    discord:message handler consumes, with guild_id injected, so
    external_id ("discord:{id}") matches the live Gateway message."""
    fake = _FakeDC([[{"id": "100", "content": "hi"}]])
    _patch(monkeypatch, fake)
    r = await fetch_page_discord(
        _Inst(),
        {"channel_id": "C", "guild_id": "G", "installation_id": "I"},
        cursor=None,
    )
    rec = r.records[0]
    assert rec["id"] == "100"
    assert rec["content"] == "hi"
    assert rec["guild_id"] == "G"
    assert rec["channel_id"] == "C"


async def test_cursor_strict():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        DiscordCursor.model_validate({"before_snowflake": None, "x": 1})


async def test_dispatch_wired():
    assert FETCHER_DISPATCH["discord"] is fetch_page_discord
