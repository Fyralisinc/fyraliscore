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


async def test_webhook_redelivery_with_same_modified_dedups():
    payload = {
        "companyId": "co-1",
        "type": "employee.updated",
        "entity": {
            "id": "e1",
            "displayName": "Grace Hopper",
            "modified": "2026-05-01T00:00:00Z",
        },
    }
    first = await handle_hibob_object(payload, {})
    retry = await handle_hibob_object(dict(payload), {})

    assert first.external_id == retry.external_id
    assert first.external_id == "hibob:co-1:employee:e1:2026-05-01T00:00:00Z"


async def test_modified_bump_produces_distinct_external_id():
    first = await handle_hibob_object({
        "companyId": "co-1",
        "type": "employee.updated",
        "entity": {
            "id": "e1",
            "displayName": "Grace Hopper",
            "modified": "2026-05-01T00:00:00Z",
        },
    }, {})
    changed = await handle_hibob_object({
        "companyId": "co-1",
        "type": "employee.updated",
        "entity": {
            "id": "e1",
            "displayName": "Grace Hopper",
            "modified": "2026-05-02T00:00:00Z",
        },
    }, {})

    assert first.external_id != changed.external_id


async def test_backfill_and_webhook_full_body_dedup_to_same_external_id():
    backfill = await handle_hibob_object({
        "_fyralis_record_type": "employee",
        "_fyralis_company_id": "co-1",
        "entity": {
            "id": "e1",
            "displayName": "Grace Hopper",
            "modified": "2026-05-01T00:00:00Z",
        },
    }, {})
    webhook = await handle_hibob_object({
        "companyId": "co-1",
        "type": "employee.updated",
        "entity": {
            "id": "e1",
            "displayName": "Grace Hopper",
            "modified": "2026-05-01T00:00:00Z",
        },
    }, {})

    assert backfill.external_id == webhook.external_id


async def test_thin_webhook_without_modified_has_stable_retry_key():
    payload = {
        "companyId": "co-1",
        "type": "employee.updated",
        "id": "e1",
    }
    first = await handle_hibob_object(payload, {})
    retry = await handle_hibob_object(dict(payload), {})

    assert first.content["thin_change"] is True
    assert first.external_id == retry.external_id
    assert first.external_id == "hibob:co-1:employee:e1:chg:none"
