"""Tests for services/ingest/ingestion/handlers/ashby.py."""
from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers import CHANNEL_TRUST_MAP, get_handler
from services.ingest.ingestion.handlers.ashby import handle_ashby_object


pytestmark = pytest.mark.asyncio


_ORG = "org-acme"
_CANDIDATE_ID = "cand-1001"


def _candidate(**over) -> dict:
    base = {
        "id": _CANDIDATE_ID,
        "name": "Ada Lovelace",
        "stage": "Screening",
        "updatedAt": "2026-05-20T12:30:00Z",
    }
    base.update(over)
    return base


def _tagged(record_type: str, entity: dict) -> dict:
    return {
        "_fyralis_record_type": record_type,
        "_fyralis_org_id": _ORG,
        "entity": entity,
    }


async def test_handler_registered() -> None:
    assert get_handler("ashby:object") is handle_ashby_object
    assert CHANNEL_TRUST_MAP["ashby:object"] == "authoritative"


async def test_candidate_uses_stable_entity_external_id() -> None:
    draft = await handle_ashby_object(_tagged("candidate", _candidate()), {})

    assert draft.source_channel == "ashby:object"
    assert draft.trust_tier == "authoritative"
    assert draft.kind == "signal"
    assert draft.external_id == f"ashby:{_ORG}:candidate:{_CANDIDATE_ID}"
    assert draft.content["object_type"] == "candidate"
    assert draft.content["org_id"] == _ORG


async def test_webhook_retry_dedups_on_entity_id() -> None:
    first = await handle_ashby_object(
        {
            "action": "candidateUpdate",
            "organizationId": _ORG,
            "data": _candidate(stage="Screening"),
        },
        {},
    )
    retry = await handle_ashby_object(
        {
            "action": "candidateUpdate",
            "organizationId": _ORG,
            "data": _candidate(stage="Interviewing"),
        },
        {},
    )

    assert first.external_id == retry.external_id
    assert first.external_id == f"ashby:{_ORG}:candidate:{_CANDIDATE_ID}"


async def test_backfill_and_webhook_dedup_to_same_external_id() -> None:
    backfill = await handle_ashby_object(_tagged("candidate", _candidate()), {})
    webhook = await handle_ashby_object(
        {
            "action": "candidateUpdate",
            "organizationId": _ORG,
            "data": _candidate(),
        },
        {},
    )

    assert backfill.external_id == webhook.external_id


async def test_thin_webhook_dedups_to_entity_external_id() -> None:
    thin = await handle_ashby_object(
        {
            "action": "candidateUpdate",
            "organizationId": _ORG,
            "data": {
                "resourceType": "candidate",
                "entityId": _CANDIDATE_ID,
                "updatedAt": "2026-05-20T12:30:00Z",
            },
        },
        {},
    )

    assert thin.content["thin_change"] is True
    assert thin.external_id == f"ashby:{_ORG}:candidate:{_CANDIDATE_ID}"


async def test_entity_kind_and_org_namespace_external_id() -> None:
    candidate = await handle_ashby_object(_tagged("candidate", _candidate(id="same")), {})
    application = await handle_ashby_object(
        _tagged(
            "application",
            {
                "id": "same",
                "candidateId": _CANDIDATE_ID,
                "status": "submitted",
                "updatedAt": "2026-05-20T12:30:00Z",
            },
        ),
        {},
    )
    other_org = await handle_ashby_object(
        {
            "_fyralis_record_type": "candidate",
            "_fyralis_org_id": "org-other",
            "entity": _candidate(id="same"),
        },
        {},
    )

    assert candidate.external_id != application.external_id
    assert candidate.external_id != other_org.external_id
