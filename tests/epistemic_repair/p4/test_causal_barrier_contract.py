from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from services.domain.company_learning.barrier import (
    ContextDecision,
    HistoricalReopenReason,
)


ROOT = Path(__file__).resolve().parents[3]


def _decision(**changes):
    values = {
        "decision_id": uuid4(),
        "tenant_id": uuid4(),
        "batch_id": "p4-batch-1",
        "route_id": "accepted-memory-first",
        "context_item_kind": "accepted_model",
        "context_item_id": str(uuid4()),
        "context_item_version": "1",
        "retrieved": True,
        "selected": True,
        "included": True,
        "referenced": True,
        "counterevidence_retained": False,
        "confidence_affecting": False,
        "necessary_background": False,
        "historical_reopen_reason": None,
        "decision_fate": "mutation",
        "result_object_kind": "model_version",
        "result_object_id": uuid4(),
        "evidence_lineage": ({"kind": "model_version", "id": str(uuid4())},),
        "decided_at": datetime.now(timezone.utc),
    }
    values.update(changes)
    return ContextDecision(**values)


def test_context_use_chain_is_monotonic():
    _decision().validate()
    with pytest.raises(ValueError, match="included"):
        _decision(included=False, referenced=True).validate()
    with pytest.raises(ValueError, match="selected"):
        _decision(selected=False, included=True).validate()
    with pytest.raises(ValueError, match="retrieved"):
        _decision(retrieved=False, selected=True).validate()


def test_historical_observation_selection_requires_typed_reopen_reason():
    with pytest.raises(ValueError, match="reopen reason"):
        _decision(context_item_kind="historical_observation").validate()
    _decision(
        context_item_kind="historical_observation",
        historical_reopen_reason=HistoricalReopenReason.CONTRADICTION,
    ).validate()


def test_migration_declares_barrier_credit_and_tenant_contracts():
    sql = (ROOT / "db/migrations/0229_company_learning_causal_barrier.sql").read_text()
    for token in (
        "company_learning_barriers",
        "company_learning_context_decisions",
        "company_learning_outcome_links",
        "truth_critical_pending_count",
        "historical_reopen_reason",
        "app.current_tenant",
    ):
        assert token in sql
