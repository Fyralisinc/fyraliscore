from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.reasoning.sage.latent_gaps import (
    SageLatentGapHypothesis,
    SageLatentGapHypothesisRepo,
    build_latent_gap_hypotheses_from_residuals,
)
from services.reasoning.sage.model_residuals import ModelResidualEvidence


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


def test_build_latent_gap_hypotheses_requires_open_residual_support() -> None:
    tenant_id = uuid4()
    open_residual = _residual(
        tenant_id=tenant_id,
        residual_kind="validation_dropped_value",
    )
    absorbed_residual = _residual(tenant_id=tenant_id, status="absorbed")
    wrong_tenant = _residual(tenant_id=uuid4())

    hypotheses = build_latent_gap_hypotheses_from_residuals(
        [open_residual, absorbed_residual, wrong_tenant],
        tenant_id=tenant_id,
    )

    assert len(hypotheses) == 1
    hypothesis = hypotheses[0]
    assert hypothesis.gap_kind == "validation_dropped_value"
    assert hypothesis.supporting_residual_ids == (open_residual.id,)
    assert hypothesis.supporting_observation_ids == (
        open_residual.source_observation_id,
    )
    assert hypothesis.falsifier
    assert hypothesis.next_evidence_needed
    assert hypothesis.status == "candidate"


def test_build_latent_gap_hypotheses_honors_min_support() -> None:
    tenant_id = uuid4()

    hypotheses = build_latent_gap_hypotheses_from_residuals(
        [_residual(tenant_id=tenant_id)],
        tenant_id=tenant_id,
        min_support=2,
    )

    assert hypotheses == []


def test_build_latent_gap_hypotheses_ignores_t4_repair_validator_artifacts() -> None:
    tenant_id = uuid4()
    repair_residual = _residual(
        tenant_id=tenant_id,
        residual_kind="validation_dropped_value",
        metadata={
            "trigger_kind": "T4",
            "trigger_subkind": "representation_repair",
            "repair_intent": "repair_validation_dropped_value",
            "repair_source": "model_residual_evidence",
        },
    )

    hypotheses = build_latent_gap_hypotheses_from_residuals(
        [repair_residual],
        tenant_id=tenant_id,
    )

    assert hypotheses == []


@pytest.mark.asyncio
async def test_insert_candidate_returns_inserted_row() -> None:
    tenant_id = uuid4()
    hypothesis_id = uuid4()
    candidate = _hypothesis(tenant_id=tenant_id, id=hypothesis_id)
    conn = _FakeConn(fetchrow_results=[_row_from_hypothesis(candidate)])
    repo = SageLatentGapHypothesisRepo(tenant_id=tenant_id)

    result = await repo.insert_candidate(candidate, conn=conn)  # type: ignore[arg-type]

    assert result.id == hypothesis_id
    assert result.status == "candidate"
    assert "INSERT INTO sage_latent_gap_hypotheses" in conn.fetchrow_calls[0][0]


@pytest.mark.asyncio
async def test_insert_candidate_reuses_existing_active_candidate() -> None:
    tenant_id = uuid4()
    existing_id = uuid4()
    candidate = _hypothesis(tenant_id=tenant_id)
    existing = _row_from_hypothesis(candidate, id=existing_id)
    conn = _FakeConn(fetchrow_results=[None, existing])
    repo = SageLatentGapHypothesisRepo(tenant_id=tenant_id)

    result = await repo.insert_candidate(candidate, conn=conn)  # type: ignore[arg-type]

    assert result.id == existing_id
    assert len(conn.fetchrow_calls) == 2
    assert "ON CONFLICT DO NOTHING" in conn.fetchrow_calls[0][0]
    assert "residual_cluster_hash = $2" in conn.fetchrow_calls[1][0]


@pytest.mark.asyncio
async def test_resolve_candidate_records_terminal_status_and_metadata() -> None:
    tenant_id = uuid4()
    hypothesis_id = uuid4()
    model_id = uuid4()
    resolved = _row_from_hypothesis(
        _hypothesis(tenant_id=tenant_id, id=hypothesis_id),
        status="confirmed",
        resolution_object_kind="model",
        resolution_object_id=model_id,
        resolution_reason="later signal confirmed gap",
        resolved_at=datetime.now(timezone.utc),
    )
    conn = _FakeConn(fetchrow_results=[resolved])
    repo = SageLatentGapHypothesisRepo(tenant_id=tenant_id)

    result = await repo.resolve(
        hypothesis_id,
        status="confirmed",
        reason="later signal confirmed gap",
        object_kind="model",
        object_id=model_id,
        metadata={"source": "test"},
        conn=conn,  # type: ignore[arg-type]
    )

    assert result is not None
    assert result.status == "confirmed"
    assert result.resolution_object_kind == "model"
    assert result.resolution_object_id == model_id
    assert result.resolution_reason == "later signal confirmed gap"


def test_insert_candidate_requires_residual_support() -> None:
    tenant_id = uuid4()
    repo = SageLatentGapHypothesisRepo(tenant_id=tenant_id)

    with pytest.raises(ValueError, match="supporting_residual_ids is required"):
        repo._validate(  # noqa: SLF001
            _hypothesis(tenant_id=tenant_id, supporting_residual_ids=()),
            require_candidate=True,
        )


def _residual(**overrides) -> ModelResidualEvidence:
    base = {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "source_observation_id": uuid4(),
        "think_run_id": uuid4(),
        "trigger_id": uuid4(),
        "model_id": None,
        "residual_kind": "valuable_unmodeled",
        "compact_summary": "Residual summary",
        "reason": "Residual reason",
        "status": "open",
    }
    base.update(overrides)
    return ModelResidualEvidence(**base)


def _hypothesis(**overrides) -> SageLatentGapHypothesis:
    residual_id = uuid4()
    base = {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "gap_kind": "valuable_unmodeled",
        "residual_cluster_hash": "cluster-hash",
        "supporting_residual_ids": (residual_id,),
        "supporting_observation_ids": (uuid4(),),
        "missing_evidence_statement": "Missing model update.",
        "falsifier": "A later trace absorbs the residual.",
        "next_evidence_needed": "Rerun metabolism.",
        "hypothesis_text": "Latent gap hypothesis.",
        "confidence": 0.45,
        "metadata": {"source": "test"},
    }
    base.update(overrides)
    return SageLatentGapHypothesis(**base)


def _row_from_hypothesis(
    hypothesis: SageLatentGapHypothesis,
    **overrides,
) -> dict:
    now = datetime.now(timezone.utc)
    row = {
        "id": hypothesis.id,
        "tenant_id": hypothesis.tenant_id,
        "gap_kind": hypothesis.gap_kind,
        "status": hypothesis.status,
        "residual_cluster_hash": hypothesis.residual_cluster_hash,
        "supporting_residual_ids": list(hypothesis.supporting_residual_ids),
        "supporting_observation_ids": list(hypothesis.supporting_observation_ids),
        "missing_evidence_statement": hypothesis.missing_evidence_statement,
        "falsifier": hypothesis.falsifier,
        "next_evidence_needed": hypothesis.next_evidence_needed,
        "confidence": hypothesis.confidence,
        "hypothesis_text": hypothesis.hypothesis_text,
        "metadata": dict(hypothesis.metadata),
        "resolution_object_kind": None,
        "resolution_object_id": None,
        "resolution_reason": None,
        "created_at": now,
        "updated_at": now,
        "resolved_at": None,
    }
    row.update(overrides)
    return row
