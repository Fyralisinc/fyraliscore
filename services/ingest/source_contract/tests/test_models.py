from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from services.ingest.source_contract.models import (
    CursorState,
    FetchedPage,
    ReconciliationDecision,
    RepairShard,
    ShardPlan,
)


NOW = datetime(2026, 8, 3, tzinfo=timezone.utc)


def test_shard_window_must_be_ordered() -> None:
    with pytest.raises(ValidationError, match="window_end must not precede"):
        ShardPlan(
            kind="events",
            identifier={},
            window_start=NOW,
            window_end=datetime(2026, 8, 2, tzinfo=timezone.utc),
        )


def test_terminal_page_cannot_advance_cursor() -> None:
    with pytest.raises(ValidationError, match="terminal pages cannot provide"):
        FetchedPage(
            end_of_data=True,
            next_cursor=CursorState(schema_version=1, payload={"page": "next"}),
        )


def test_clean_reconciliation_cannot_create_repair_work() -> None:
    repair = RepairShard(
        shard=ShardPlan(kind="events", identifier={}),
        parent_shard_id=UUID("00000000-0000-0000-0000-000000000001"),
    )
    with pytest.raises(ValidationError, match="clean reconciliation"):
        ReconciliationDecision(has_gaps=False, new_shards=(repair,))


def test_contract_models_are_frozen() -> None:
    cursor = CursorState(schema_version=1, payload={})
    with pytest.raises(ValidationError, match="Instance is frozen"):
        cursor.schema_version = 2  # type: ignore[misc]
