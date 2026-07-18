from __future__ import annotations

import inspect
from uuid import uuid4

import pytest

from lib.evaluation.epistemic_repair import p6_postfreeze_evidence
from services.evaluation.epistemic_repair.p6_think_runner import (
    _freeze_zero_seed_preflight,
)


class _PreflightConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def fetchval(self, query: str, _tenant_id):
        self.queries.append(" ".join(query.split()))
        return 0


@pytest.mark.asyncio
async def test_zero_seed_preflight_freezes_both_canonical_counts() -> None:
    conn = _PreflightConnection()

    receipt = await _freeze_zero_seed_preflight(conn, uuid4())

    assert receipt["accepted_model_count"] == 0
    assert receipt["accepted_relation_count"] == 0
    assert receipt["source"] == "database_preflight_before_first_signal"
    assert any("accepted_current_models" in query for query in conn.queries)
    assert any("accepted_current_relations" in query for query in conn.queries)


def test_synthesis_lineage_resolves_model_versions_to_source_signals() -> None:
    claims = [
        {
            "id": "atomic",
            "truth_version_id": "v1",
            "truth_candidate_kind": "atomic_claim",
            "direct_evidence_signal_ids": ["signal-1"],
            "source_model_version_ids": [],
        },
        {
            "id": "middle",
            "truth_version_id": "v2",
            "truth_candidate_kind": "synthesis",
            "direct_evidence_signal_ids": ["signal-2"],
            "source_model_version_ids": ["v1"],
        },
        {
            "id": "thesis",
            "truth_version_id": "v3",
            "truth_candidate_kind": "synthesis",
            "direct_evidence_signal_ids": [],
            "source_model_version_ids": ["v2"],
        },
    ]

    p6_postfreeze_evidence._resolve_claim_lineage(claims)

    assert claims[2]["is_canonical_synthesis"] is True
    assert claims[2]["source_model_ids"] == ["middle"]
    assert claims[2]["evidence_signal_ids"] == ["signal-1", "signal-2"]


def test_postfreeze_query_captures_signed_relation_truth_evidence() -> None:
    source = inspect.getsource(
        p6_postfreeze_evidence.extract_p6_postfreeze_evidence
    )

    assert "relation_truth_evidence" in source
    assert "evidence_reference_id" in source
    assert "evidence_digest" in source
    assert "polarity" in source
    assert "truth_candidate_kind" in source
