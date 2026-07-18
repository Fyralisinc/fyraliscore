from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.evaluation.epistemic_repair.p6_think_runner import (
    _persisted_boundary_entity_refs,
)


class _Conn:
    def __init__(self, rows):
        self.rows = rows
        self.args = None

    async def fetch(self, _sql, *args):
        self.args = args
        return self.rows


@pytest.mark.asyncio
async def test_loads_verified_provisional_and_resolved_entity_refs() -> None:
    tenant_id = uuid4()
    provisional_id, resolved_id = uuid4(), uuid4()
    conn = _Conn([
        {
            "source_observation_id": provisional_id,
            "mention": json.dumps({
                "provisional_canonical_ref": "workstream:delta-handoff",
            }),
            "current_fate": "unresolved",
            "selected_referent": {},
        },
        {
            "source_observation_id": resolved_id,
            "mention": {},
            "current_fate": "resolved_for_consumer",
            "selected_referent": json.dumps({
                "canonical_ref": "workstream:beacon-migration",
            }),
        },
    ])

    refs = await _persisted_boundary_entity_refs(
        conn, tenant_id, [provisional_id, resolved_id],
    )

    assert conn.args == (tenant_id, [provisional_id, resolved_id])
    assert refs == {
        str(provisional_id): ("workstream:delta-handoff",),
        str(resolved_id): ("workstream:beacon-migration",),
    }

