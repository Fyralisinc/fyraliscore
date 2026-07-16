from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.reasoning.think.applier import _ALLOWED_MODEL_UPDATE_COLUMNS
from services.reasoning.think.deterministic import (
    _grounding_correction_revalidation_ops,
)


pytestmark = pytest.mark.asyncio


class _Connection:
    def __init__(self, *, model_row, surviving_rows, exists_values) -> None:
        self.model_row = model_row
        self.surviving_rows = surviving_rows
        self.exists_values = list(exists_values)

    async def fetchrow(self, _sql, *_args):
        return self.model_row

    async def fetch(self, _sql, *_args):
        return self.surviving_rows

    async def fetchval(self, _sql, *_args):
        return self.exists_values.pop(0)


async def test_grounding_correction_archives_when_last_positive_support_is_lost():
    cause_model_id = uuid7()
    dependent_model_id = uuid7()
    ops = await _grounding_correction_revalidation_ops(
        _Connection(
            model_row={
                "status": "active",
                "visible_to_subjects": False,
                "supporting_model_ids": [cause_model_id],
            },
            surviving_rows=[],
            exists_values=[False],
        ),  # type: ignore[arg-type]
        tenant_id=uuid7(),
        dependent_model_id=dependent_model_id,
        cause_model_id=cause_model_id,
    )

    assert len(ops) == 1
    assert ops[0].op == "archive"
    assert ops[0].reason == "superseded"


async def test_grounding_correction_unfences_with_surviving_positive_support():
    cause_model_id = uuid7()
    surviving_model_id = uuid7()
    dependent_model_id = uuid7()
    ops = await _grounding_correction_revalidation_ops(
        _Connection(
            model_row={
                "status": "active",
                "visible_to_subjects": False,
                "supporting_model_ids": [
                    cause_model_id,
                    surviving_model_id,
                ],
            },
            surviving_rows=[{"id": surviving_model_id}],
            exists_values=[False],
        ),  # type: ignore[arg-type]
        tenant_id=uuid7(),
        dependent_model_id=dependent_model_id,
        cause_model_id=cause_model_id,
    )

    assert len(ops) == 1
    assert ops[0].op == "update"
    assert ops[0].changes == {
        "visible_to_subjects": True,
        "supporting_model_ids": [surviving_model_id],
    }
    assert "visible_to_subjects" in _ALLOWED_MODEL_UPDATE_COLUMNS
