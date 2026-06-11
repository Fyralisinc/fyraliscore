"""Tests for services/ingest/ingestion/handlers/hibob.py."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.hibob import handle_hibob_object


pytestmark = pytest.mark.asyncio


async def test_handler_registered():
    assert get_handler("hibob:object") is handle_hibob_object
    assert CHANNEL_TRUST_MAP["hibob:object"] == "authoritative"


async def test_people_search_slash_fields_are_supported():
    draft = await handle_hibob_object({
        "_fyralis_record_type": "employee",
        "_fyralis_company_id": "co-1",
        "entity": {
            "/root/id": 123,
            "/root/displayName": "Ada Lovelace",
            "/root/email": "ada@example.com",
            "/work/department": "Engineering",
            "/work/title": "Engineer",
            "modified": "2026-05-01T00:00:00Z",
            "status": "active",
        },
    }, {})
    assert draft.external_id == "hibob:co-1:employee:123:2026-05-01T00:00:00Z"
    assert draft.content["display_name"] == "Ada Lovelace"
    assert draft.content["department"] == "Engineering"
    assert draft.content["title"] == "Engineer"
    assert draft.content["email"] == "ada@example.com"


async def test_numeric_company_id_from_webhook_is_stringified():
    draft = await handle_hibob_object({
        "companyId": 42,
        "type": "employee.updated",
        "entity": {
            "id": "e1",
            "displayName": "Grace Hopper",
            "modified": "2026-05-01T00:00:00Z",
        },
    }, {})
    assert draft.content["company_id"] == "42"
    assert draft.external_id == "hibob:42:employee:e1:2026-05-01T00:00:00Z"
