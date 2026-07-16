from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib.contracts.source_semantics import (
    GroundedBeliefApplyResult,
    SourceSemanticAdmissionDisposition,
)
from lib.shared.ids import uuid7
from services.domain.source_semantics.processor import GroundedBeliefProcessor


pytestmark = pytest.mark.asyncio


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _Connection:
    def transaction(self):
        return _Transaction()


async def test_duplicate_corrected_admission_still_activates_direct_fence() -> None:
    tenant_id = uuid7()
    predecessor_trace_id = uuid7()
    successor_trace_id = uuid7()
    observation_id = uuid7()
    corrected_model_id = uuid7()
    duplicate = GroundedBeliefApplyResult(
        interpretation_id=uuid7(),
        admission_decision_id=uuid7(),
        disposition=SourceSemanticAdmissionDisposition.BELIEF_APPLIED,
        reason_codes=("asserted_report_with_single_referent_grounding",),
        model_id=corrected_model_id,
        duplicate=True,
    )
    grounding = SimpleNamespace(
        tenant_id=tenant_id,
        trace_id=successor_trace_id,
        source_observation_id=observation_id,
        supersedes_grounding_trace_id=predecessor_trace_id,
    )
    bundle = SimpleNamespace(
        tenant_id=tenant_id,
        grounding_trace_id=successor_trace_id,
        bundle_digest="bundle-digest",
    )

    class _Repo:
        async def load_grounding_trace(self, _conn, **_kwargs):
            return grounding

        async def find_result(self, _conn, **_kwargs):
            return duplicate

    class _Extractor:
        def extract(self, _grounding):
            return bundle

    class _Correction:
        def __init__(self) -> None:
            self.calls = []

        async def propagate_direct_correction(self, _conn, **kwargs):
            self.calls.append(kwargs)

    correction = _Correction()
    processor = GroundedBeliefProcessor(
        source_semantic_repo=_Repo(),  # type: ignore[arg-type]
        epistemic_applier=object(),  # type: ignore[arg-type]
        extractor=_Extractor(),  # type: ignore[arg-type]
        correction_propagation_service=correction,  # type: ignore[arg-type]
    )

    result = await processor.process_trace(
        _Connection(),  # type: ignore[arg-type]
        tenant_id=tenant_id,
        grounding_trace_id=successor_trace_id,
        embedding=[],
    )

    assert result == duplicate
    assert correction.calls == [
        {
            "tenant_id": tenant_id,
            "predecessor_grounding_trace_id": predecessor_trace_id,
            "successor_grounding_trace_id": successor_trace_id,
            "cause_event_id": observation_id,
            "corrected_model_id": corrected_model_id,
        }
    ]
