from __future__ import annotations

from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

from services.evaluation.epistemic_repair.p2_hg10_probes import (
    ProjectionIdempotenceProbe,
    probe_derived_writer_rejection,
)


class RejectionConnection:
    def __init__(self) -> None:
        self.row = {
            "id": uuid4(),
            "tenant_id": uuid4(),
            "proposition": {"kind": "belief"},
            "natural": "stable",
            "scope_actors": [],
            "scope_entities": [],
            "status": "active",
        }

    async def fetchrow(self, *_args):
        return dict(self.row)

    async def fetchval(self, *_args):
        return 1

    @asynccontextmanager
    async def transaction(self):
        yield

    async def execute(self, sql, *_args):
        assert "UPDATE models" in sql or "INSERT INTO truth_candidates" in sql
        raise RuntimeError("accepted Model semantics require a truth-kernel command")


@pytest.mark.asyncio
async def test_derived_writer_probe_requires_rejection_and_unchanged_truth() -> None:
    conn = RejectionConnection()
    result = await probe_derived_writer_rejection(
        conn,
        tenant_id=conn.row["tenant_id"],
        model_id=conn.row["id"],
        component="sage",
    )
    assert result.conforms
    assert result.component == "sage"
    assert result.error_type == "RuntimeError+RuntimeError"


def test_projection_probe_conformance_is_continuous_not_exception_based() -> None:
    assert ProjectionIdempotenceProbe(5, 1, 1, True).conforms
    assert not ProjectionIdempotenceProbe(5, 2, 1, True).conforms
    assert not ProjectionIdempotenceProbe(5, 1, 2, True).conforms
    assert not ProjectionIdempotenceProbe(5, 1, 1, False).conforms


def test_runner_integrates_both_sealed_hg10_families() -> None:
    source = open(
        "services/evaluation/epistemic_repair/p2_runner.py", encoding="utf-8"
    ).read()
    assert 'family == "derived_direct_write"' in source
    assert 'family == "projection_idempotence"' in source
    assert "probe_derived_writer_rejection(" in source
    assert "probe_projection_idempotence(" in source
