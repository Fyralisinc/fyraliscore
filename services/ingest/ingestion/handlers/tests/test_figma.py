"""Tests for services/ingest/ingestion/handlers/figma.py (design)."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.figma import handle_figma_event


pytestmark = pytest.mark.asyncio


_TEAM = "team-1"
_FILE = "file-abc"


def _event(**over):
    base = {
        "id": "e-1001",
        "event_id": "e-1001",
        "event_type": "FILE_VERSION_UPDATE",
        "type": "FILE_VERSION_UPDATE",
        "team_id": _TEAM,
        "file_key": _FILE,
        "version": "v-1",
        "label": "v1.0 checkpoint",
        "user": "ada",
        "createdAt": "2026-05-20T12:30:00.000Z",
        "created_at": "2026-05-20T12:30:00.000Z",
    }
    base.update(over)
    return {"_fyralis_record_type": "event", "_fyralis_team_id": _TEAM,
            "_fyralis_file_key": _FILE, "event": base}


async def test_handler_registered():
    assert get_handler("figma:event") is handle_figma_event
    assert CHANNEL_TRUST_MAP["figma:event"] == "authoritative"


async def test_event_is_signal_with_versioned_external_id():
    draft = await handle_figma_event(_event(), {})
    assert draft.source_channel == "figma:event"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    # external_id namespaced by team_id and versioned by version.
    assert draft.external_id == f"figma:{_TEAM}:event:e-1001:v-1"
    assert draft.content["object_type"] == "event"
    assert draft.content["team_id"] == _TEAM
    assert draft.content["file_key"] == _FILE


async def test_file_delete_is_state_change():
    draft = await handle_figma_event({
        "_fyralis_record_type": "event", "_fyralis_team_id": _TEAM,
        "_fyralis_file_key": _FILE,
        "event": {"id": "e-del", "event_type": "FILE_DELETE",
                  "team_id": _TEAM, "file_key": _FILE, "version": "v-9",
                  "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert draft.kind == "state_change"
    assert draft.external_id == f"figma:{_TEAM}:event:e-del:v-9"


async def test_dev_mode_revert_is_state_change():
    draft = await handle_figma_event({
        "_fyralis_record_type": "event", "_fyralis_team_id": _TEAM,
        "_fyralis_file_key": _FILE,
        "event": {"id": "e-dev", "event_type": "DEV_MODE_STATUS_UPDATE",
                  "team_id": _TEAM, "file_key": _FILE, "version": "v-3",
                  "status": "in_progress",
                  "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert draft.kind == "state_change"


async def test_version_change_produces_distinct_external_id():
    """Mutable-source dedup lesson: a re-publish (new version) must NOT collapse
    onto the earlier observation."""
    v1 = await handle_figma_event(_event(), {})
    v2 = await handle_figma_event({
        "_fyralis_record_type": "event", "_fyralis_team_id": _TEAM,
        "_fyralis_file_key": _FILE,
        "event": {"id": "e-1001", "event_type": "FILE_VERSION_UPDATE",
                  "team_id": _TEAM, "file_key": _FILE, "version": "v-2",
                  "createdAt": "2026-05-21T08:00:00.000Z"},
    }, {})
    assert v1.external_id != v2.external_id


async def test_team_id_namespaces_external_id():
    """Two tenants with the SAME event id but different team_id stay distinct —
    the global UNIQUE has no tenant_id, so team_id namespacing is load-bearing."""
    a = await handle_figma_event(_event(), {})
    b = await handle_figma_event({
        "_fyralis_record_type": "event", "_fyralis_team_id": "team-2",
        "_fyralis_file_key": _FILE,
        "event": {"id": "e-1001", "event_type": "FILE_VERSION_UPDATE",
                  "team_id": "team-2", "file_key": _FILE, "version": "v-1",
                  "createdAt": "2026-05-20T12:30:00.000Z"},
    }, {})
    assert a.external_id != b.external_id
    assert a.external_id.startswith("figma:team-1:")
    assert b.external_id.startswith("figma:team-2:")


# --- live webhook path -----------------------------------------------------

async def test_webhook_event_inline_body():
    payload = {
        "event_type": "FILE_VERSION_UPDATE",
        "team_id": _TEAM,
        "id": "e-1001",
        "file_key": _FILE,
        "version": "v-1",
        "createdAt": "2026-05-20T12:30:00.000Z",
    }
    draft = await handle_figma_event(payload, {})
    assert draft.content["object_type"] == "event"
    # external_id parity with the backfilled event record.
    assert draft.external_id == f"figma:{_TEAM}:event:e-1001:v-1"


async def test_backfill_and_webhook_dedup_to_same_external_id():
    backfill = await handle_figma_event(_event(), {})
    webhook = await handle_figma_event({
        "event_type": "FILE_VERSION_UPDATE", "team_id": _TEAM,
        "id": "e-1001", "file_key": _FILE, "version": "v-1",
        "createdAt": "2026-05-20T12:30:00.000Z",
    }, {})
    assert backfill.external_id == webhook.external_id


async def test_ping_is_not_an_observation():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_figma_event({"event_type": "PING"}, {})


async def test_unknown_payload_raises():
    from lib.shared.errors import ValidationError
    with pytest.raises(ValidationError):
        await handle_figma_event({"foo": "bar"}, {})
