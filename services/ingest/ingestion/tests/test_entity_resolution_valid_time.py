"""The shared ingestion path resolves names at source event time."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.ingest.ingestion.core import _resolve_entities
from services.ingest.ingestion.handlers import ObservationDraft


@pytest.mark.asyncio
async def test_entity_resolution_uses_draft_occurred_at() -> None:
    occurred_at = datetime(2024, 3, 1, 12, 0, tzinfo=timezone.utc)
    expected_ref = {"type": "customer", "id": str(uuid7())}

    class _AliasRepo:
        observed_as_of = None

        async def fast_path_resolve_many(
            self,
            phrases,
            tenant_id,
            *,
            as_of=None,
        ):
            self.observed_as_of = as_of
            return {"acme": expected_ref}

    alias_repo = _AliasRepo()
    draft = ObservationDraft(
        source_channel="test:signal",
        content_text="Acme renewed",
        content={},
        occurred_at=occurred_at,
        trust_tier="inferential",
    )

    resolution = await _resolve_entities(
        draft,
        alias_repo,  # type: ignore[arg-type]
        uuid7(),
    )

    assert alias_repo.observed_as_of == occurred_at
    assert expected_ref in resolution.entities_mentioned
