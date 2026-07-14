from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib.shared.ids import uuid7
from services.domain.models.repo import (
    _bulk_record_model_authority_provenance,
    _record_model_authority_provenance,
)


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []

    async def execute(self, query: str, *args):
        self.executed.append((query, args))
        return "INSERT 0 1"


def _model(**overrides):
    tenant = overrides.pop("tenant_id", uuid7())
    return SimpleNamespace(
        id=overrides.pop("id", uuid7()),
        tenant_id=tenant,
        born_from_event_id=overrides.pop("born_from_event_id", uuid7()),
        supporting_event_ids=overrides.pop("supporting_event_ids", []),
        supporting_model_ids=overrides.pop("supporting_model_ids", []),
        contributing_models=overrides.pop("contributing_models", []),
    )


async def test_model_insert_records_authority_provenance_sources():
    born_event = uuid7()
    support_event = uuid7()
    support_model = uuid7()
    contributing_model = uuid7()
    model = _model(
        born_from_event_id=born_event,
        supporting_event_ids=[born_event, support_event, support_event],
        supporting_model_ids=[support_model],
        contributing_models=[contributing_model],
    )
    conn = _FakeConn()

    await _record_model_authority_provenance(conn, model)

    args_by_derivation = {
        args[5]: args
        for _, args in conn.executed
        if len(args) == 7
    }
    assert args_by_derivation["model_born_from_event"][3:6] == (
        "observation",
        born_event,
        "model_born_from_event",
    )
    assert args_by_derivation["model_supporting_event"][3] == "observation"
    assert args_by_derivation["model_supporting_model"][3:6] == (
        "model",
        support_model,
        "model_supporting_model",
    )
    assert args_by_derivation["model_contributing_model"][3] == "model"
    assert len(args_by_derivation) == 4
    assert len(conn.executed) == 6
    label_args = conn.executed[-1][1]
    assert label_args[4] == ["observation", "observation", "model", "model"]
    assert label_args[5] == [
        born_event,
        support_event,
        support_model,
        contributing_model,
    ]


async def test_bulk_model_insert_records_authority_provenance_in_one_statement():
    born_event = uuid7()
    support_event = uuid7()
    model = _model(
        born_from_event_id=born_event,
        supporting_event_ids=[support_event],
    )
    conn = _FakeConn()

    await _bulk_record_model_authority_provenance(conn, [model])

    assert len(conn.executed) == 2
    query, args = conn.executed[0]
    assert "ON CONFLICT" in query
    tenant_ids, derived_ids, source_kinds, source_ids, derivation_kinds, columns = args
    assert tenant_ids == [model.tenant_id, model.tenant_id]
    assert derived_ids == [model.id, model.id]
    assert source_kinds == ["observation", "observation"]
    assert source_ids == [born_event, support_event]
    assert derivation_kinds == ["model_born_from_event", "model_supporting_event"]
    assert columns == ["born_from_event_id", "supporting_event_ids"]
    label_query, label_args = conn.executed[1]
    assert "JOIN object_access_labels" in label_query
    assert label_args[1:4] == ("model", model.id, "model_provenance")
