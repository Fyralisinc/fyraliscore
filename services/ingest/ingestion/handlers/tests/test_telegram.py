"""Tests for services/ingest/ingestion/handlers/telegram.py (IN-TELEGRAM)."""
from __future__ import annotations

import pytest

from lib.shared.errors import ValidationError
from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.telegram import handle_telegram
from services.ingest.integrations.telegram.records import build_message_record


pytestmark = pytest.mark.asyncio


def _msg(mid=7, *, edit_date=None, text="hello", sender=99):
    return {
        "id": mid, "date": 1781000000, "edit_date": edit_date,
        "message": text, "out": False, "from_id": {"user_id": sender},
        "sender_username": "alice",
    }


def _record(msg, *, installation_id="inst-uuid", dialog_id=42):
    return build_message_record(
        msg, installation_id=installation_id, dialog_id=dialog_id,
        dialog_kind="channel", dialog_title="Eng",
    )


async def test_dispatch_and_trust_wired():
    assert get_handler("telegram:message") is handle_telegram
    assert CHANNEL_TRUST_MAP["telegram:message"] == "attested_agent"


async def test_draft_fields():
    draft = await handle_telegram(_record(_msg()), {})
    assert draft.source_channel == "telegram:message"
    assert draft.kind == "signal"
    assert draft.trust_tier == "attested_agent"
    assert draft.source_actor_ref == "telegram:user:99"
    assert draft.external_id == "telegram:inst-uuid:42:7:none"
    assert "hello" in draft.content_text
    assert draft.occurred_at.year == 2026
    # entities: the dialog + the sender.
    kinds = {e["type"] for e in draft.entities_hint}
    assert {"telegram_dialog", "telegram_user"} <= kinds


async def test_cross_path_external_id_parity():
    """A backfill record and a live gateway record for the SAME message derive
    the IDENTICAL external_id (both build via build_message_record + the central
    idempotency constructor) → they dedup to one observation."""
    msg = _msg(mid=7, edit_date=None)
    backfill = await handle_telegram(_record(msg), {})
    gateway = await handle_telegram(_record(dict(msg)), {})
    assert backfill.external_id == gateway.external_id


async def test_edit_versions_external_id():
    """An edit (fresh edit_date) lands a NEW observation (distinct external_id)."""
    base = await handle_telegram(_record(_msg(mid=7, edit_date=None)), {})
    edited = await handle_telegram(_record(_msg(mid=7, edit_date=1781000123)), {})
    assert base.external_id != edited.external_id
    assert edited.external_id.endswith(":1781000123")
    assert edited.content["edited"] is True


async def test_malformed_missing_id_raises():
    bad = _record(_msg())
    bad.pop("id")
    with pytest.raises(ValidationError):
        await handle_telegram(bad, {})


async def test_non_dict_payload_raises():
    with pytest.raises(ValidationError):
        await handle_telegram("not-a-dict", {})  # type: ignore[arg-type]
