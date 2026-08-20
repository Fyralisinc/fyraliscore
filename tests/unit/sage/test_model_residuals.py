from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.reasoning.sage.model_residuals import (
    ModelResidualEvidence,
    ModelResidualEvidenceRepo,
)


class _FakeConn:
    def __init__(
        self,
        *,
        fetchrow_results: list[dict | None] | None = None,
        fetch_results: list[list[dict]] | None = None,
    ) -> None:
        self.fetchrow_results = list(fetchrow_results or [])
        self.fetch_results = list(fetch_results or [])
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, sql: str, *args: object) -> dict | None:
        self.fetchrow_calls.append((sql, args))
        if not self.fetchrow_results:
            return None
        return self.fetchrow_results.pop(0)

    async def fetch(self, sql: str, *args: object) -> list[dict]:
        self.fetch_calls.append((sql, args))
        if not self.fetch_results:
            return []
        return self.fetch_results.pop(0)


@pytest.mark.asyncio
async def test_insert_open_returns_inserted_residual() -> None:
    tenant_id = uuid4()
    residual_id = uuid4()
    observation_id = uuid4()
    inserted = _row(
        id=residual_id,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        residual_kind="valuable_unmodeled",
        compact_summary="summary",
        reason="reason",
    )
    conn = _FakeConn(fetchrow_results=[inserted])
    repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)

    result = await repo.insert_open(
        ModelResidualEvidence(
            tenant_id=tenant_id,
            id=residual_id,
            source_observation_id=observation_id,
            residual_kind="valuable_unmodeled",
            compact_summary="summary",
            reason="reason",
            metadata={"source": "test"},
        ),
        conn=conn,  # type: ignore[arg-type]
    )

    assert result.id == residual_id
    assert result.status == "open"
    assert "INSERT INTO model_residual_evidence" in conn.fetchrow_calls[0][0]


@pytest.mark.asyncio
async def test_insert_open_reuses_existing_open_residual_on_conflict() -> None:
    tenant_id = uuid4()
    existing_id = uuid4()
    observation_id = uuid4()
    existing = _row(
        id=existing_id,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        residual_kind="counterevidence_unattached",
        compact_summary="old summary",
        reason="same reason",
    )
    conn = _FakeConn(fetchrow_results=[None, existing])
    repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)

    result = await repo.insert_open(
        ModelResidualEvidence(
            tenant_id=tenant_id,
            source_observation_id=observation_id,
            residual_kind="counterevidence_unattached",
            compact_summary="new summary",
            reason="same reason",
        ),
        conn=conn,  # type: ignore[arg-type]
    )

    assert result.id == existing_id
    assert len(conn.fetchrow_calls) == 2
    assert "ON CONFLICT DO NOTHING" in conn.fetchrow_calls[0][0]
    assert "md5(reason) = md5($4)" in conn.fetchrow_calls[1][0]


@pytest.mark.asyncio
async def test_absorb_closes_open_residual_with_absorbing_object() -> None:
    tenant_id = uuid4()
    residual_id = uuid4()
    model_id = uuid4()
    absorbed = _row(
        id=residual_id,
        tenant_id=tenant_id,
        residual_kind="compression_uncertain",
        compact_summary="summary",
        reason="reason",
        status="absorbed",
        absorption_object_kind="model",
        absorption_object_id=model_id,
        resolved_at=datetime.now(timezone.utc),
    )
    conn = _FakeConn(fetchrow_results=[absorbed])
    repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)

    result = await repo.absorb(
        residual_id,
        object_kind="model",
        object_id=model_id,
        metadata={"absorbed_by": "test"},
        conn=conn,  # type: ignore[arg-type]
    )

    assert result is not None
    assert result.status == "absorbed"
    assert result.absorption_object_kind == "model"
    assert result.absorption_object_id == model_id
    assert "status = $3" in conn.fetchrow_calls[0][0]


@pytest.mark.asyncio
async def test_reject_records_resolution_metadata() -> None:
    tenant_id = uuid4()
    residual_id = uuid4()
    rejected = _row(
        id=residual_id,
        tenant_id=tenant_id,
        residual_kind="validation_dropped_value",
        compact_summary="summary",
        reason="reason",
        status="rejected",
        metadata={"resolution_reason": "noise"},
        resolved_at=datetime.now(timezone.utc),
    )
    conn = _FakeConn(fetchrow_results=[rejected])
    repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)

    result = await repo.reject(
        residual_id,
        reason="noise",
        conn=conn,  # type: ignore[arg-type]
    )

    assert result is not None
    assert result.status == "rejected"
    assert result.metadata["resolution_reason"] == "noise"


def test_insert_open_validates_residual_kind() -> None:
    tenant_id = uuid4()
    repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)

    with pytest.raises(ValueError, match="invalid residual kind"):
        repo._validate_residual(  # noqa: SLF001
            ModelResidualEvidence(
                tenant_id=tenant_id,
                residual_kind="not_a_kind",
                compact_summary="summary",
                reason="reason",
            ),
            require_open=True,
        )


def _row(**overrides: object) -> dict:
    now = datetime.now(timezone.utc)
    base = {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "source_observation_id": None,
        "think_run_id": None,
        "trigger_id": None,
        "model_id": None,
        "residual_kind": "valuable_unmodeled",
        "compact_summary": "summary",
        "reason": "reason",
        "status": "open",
        "absorption_object_kind": None,
        "absorption_object_id": None,
        "metadata": {},
        "created_at": now,
        "updated_at": now,
        "resolved_at": None,
    }
    base.update(overrides)
    return base
