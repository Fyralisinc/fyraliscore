"""Unit tests for services/app/gateway/slack_router.py synthetic DM generators.

These cover the pure data-shaping functions (no DB): the backfill/live records
must be in the Slack `event_callback` shape the real handler consumes, carry
the right channel_type, and derive stable DM channel ids. The DB-backed
install -> backfill -> live -> status path is exercised end-to-end by the
running stack (see docs/ingestion/slack-dm-demo.md)."""
from __future__ import annotations

from services.app.gateway.slack_router import (
    _dm_channel,
    _slack_dm_backfill_records,
    _slack_dm_live_event,
    build_slack_router,
)
from services.ingest.ingestion.handlers.slack import handle_slack_message


def test_router_has_four_controls():
    r = build_slack_router()
    paths = {route.path for route in r.routes}
    assert paths == {
        "/slack/{user_id}/install",
        "/slack/{user_id}/backfill",
        "/slack/{user_id}/live/emit",
        "/slack/{user_id}/status",
    }


def test_dm_channel_is_stable_and_symmetric():
    a = _dm_channel("U_ALICE", "U_FRIEND")
    b = _dm_channel("U_FRIEND", "U_ALICE")
    assert a == b  # the 1:1 channel is the same regardless of arg order
    assert a.startswith("D")


def test_backfill_records_cover_im_mpim_and_channel():
    recs = _slack_dm_backfill_records("U_ALICE", "T0001", n=4, seed=0)
    types = {r["event"]["channel_type"] for r in recs}
    assert {"im", "mpim", "channel"} <= types
    # Every record is an event_callback wrapping a message event with a ts.
    for r in recs:
        assert r["type"] == "event_callback"
        assert r["event"]["type"] == "message"
        assert isinstance(r["event"]["ts"], str) and "." in r["event"]["ts"]
    # im channels are D-prefixed; the group DM is G-prefixed.
    im = [r for r in recs if r["event"]["channel_type"] == "im"]
    mpim = [r for r in recs if r["event"]["channel_type"] == "mpim"]
    assert all(r["event"]["channel"].startswith("D") for r in im)
    assert all(r["event"]["channel"].startswith("G") for r in mpim)


def test_backfill_records_are_handler_ingestible():
    import asyncio

    recs = _slack_dm_backfill_records("U_ALICE", "T0001", n=2, seed=1)

    async def _drain():
        seen = set()
        for r in recs:
            draft = await handle_slack_message(r, {})
            assert draft.source_channel == "slack:message"
            seen.add(draft.external_id)
        return seen

    # Distinct external_ids (no accidental collisions across the batch).
    seen = asyncio.run(_drain())
    assert len(seen) == len(recs)


def test_live_event_rotation_hits_im_mpim_and_edit():
    types = []
    subtypes = []
    for seq in range(1, 11):
        ev = _slack_dm_live_event("U_ALICE", "T0001", seq)["event"]
        types.append(ev["channel_type"])
        subtypes.append(ev.get("subtype"))
    assert "im" in types
    assert "mpim" in types
    assert "message_changed" in subtypes  # seq % 5 == 4 emits an edit
