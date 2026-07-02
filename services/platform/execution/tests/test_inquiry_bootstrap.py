from __future__ import annotations

from uuid import UUID

import pytest

from services.platform.execution import inquiry, inquiry_bootstrap
from services.platform.execution.config import InquiryConfig
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


class _NoPolicyConn:
    async def fetchval(self, *_args: object, **_kwargs: object) -> None:
        return None

    async def fetch(self, *_args: object, **_kwargs: object) -> list[object]:
        return []


def _weak_noop_trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[],
        seed_natural_text=(
            "Workspace chatter: lunch notes, travel plans, and general team "
            "coordination. No blocker, no owner change, no decision."
        ),
        seed_occurred_at=None,
        scope_actors=[],
    )


def test_inquiry_imports_bootstrap_phase_from_canonical_module() -> None:
    assert inquiry._bootstrap_inquiry_run is inquiry_bootstrap._bootstrap_inquiry_run


@pytest.mark.asyncio
async def test_bootstrap_cold_weak_noop_skips_primary_retrieval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _unexpected_primary_retrieve(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("cold weak no-op should not call primary_retrieve")

    monkeypatch.setattr(
        inquiry_bootstrap,
        "primary_retrieve",
        _unexpected_primary_retrieve,
    )

    state = await inquiry_bootstrap._bootstrap_inquiry_run(
        trigger=_weak_noop_trigger(),
        conn=_NoPolicyConn(),
        embedder=None,
        read_pool=None,
        route=None,
        mode="deep",
        top_n=64,
        config=InquiryConfig(
            candidate_model_limit=20,
            result_model_limit=5,
            max_rounds=2,
        ),
    )

    assert state.route == "DEEP_INQUIRY_PATH"
    assert state.candidate_top_n == 20
    assert state.effective_top_n == 5
    assert state.baseline_top_n == 20
    assert state.cold_weak_noop_gate["used"] is True
    assert state.max_rounds == 0
    assert state.baseline.models == []
    assert state.retrieval_results == [state.baseline]
    assert state.question_policy == {}
    assert state.sage_reader_runtime is None
    assert {note["stage"] for note in state.stage_timing_notes} >= {
        "primary_retrieve",
        "baseline_reservoir_seed",
        "question_policy_load",
    }
    primary_note = next(
        note
        for note in state.stage_timing_notes
        if note["stage"] == "primary_retrieve"
    )
    assert primary_note["skipped"] is True


@pytest.mark.asyncio
async def test_bootstrap_passes_company_learning_profile_to_primary_retrieval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, object] = {}

    async def _fake_primary_retrieve(
        trigger: TriggerContext,
        *_args: object,
        **kwargs: object,
    ) -> RetrievalResult:
        seen["company_profile"] = kwargs.get("company_profile")
        return RetrievalResult(trigger=trigger)

    monkeypatch.setattr(
        inquiry_bootstrap,
        "primary_retrieve",
        _fake_primary_retrieve,
    )

    trigger = TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_natural_text=(
            "Customer Acme is blocked on contract approval and needs an owner "
            "decision before renewal closes."
        ),
    )
    state = await inquiry_bootstrap._bootstrap_inquiry_run(
        trigger=trigger,
        conn=_NoPolicyConn(),
        embedder=None,
        read_pool=None,
        route=None,
        mode="deep",
        top_n=16,
        config=InquiryConfig(
            candidate_model_limit=10,
            result_model_limit=5,
            max_rounds=0,
        ),
    )

    assert seen["company_profile"] is state.company_learning_profile
    assert state.company_learning_profile is not None
    assert state.company_learning_profile.notes == ("empty_company_learning_profile",)
