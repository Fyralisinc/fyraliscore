from __future__ import annotations

from uuid import UUID

import pytest

from services.platform.execution import inquiry, inquiry_bootstrap, inquiry_rounds
from services.platform.execution.config import InquiryConfig
from services.reasoning.retrieval.primary import TriggerContext


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


def test_inquiry_imports_round_phase_from_canonical_module() -> None:
    assert inquiry._execute_inquiry_rounds is inquiry_rounds._execute_inquiry_rounds


@pytest.mark.asyncio
async def test_execute_inquiry_rounds_preserves_no_round_default_status() -> None:
    trigger = _weak_noop_trigger()
    state = await inquiry_bootstrap._bootstrap_inquiry_run(
        trigger=trigger,
        conn=_NoPolicyConn(),
        embedder=None,
        read_pool=None,
        route=None,
        mode="deep",
        top_n=64,
        config=InquiryConfig(max_rounds=2),
    )

    status = await inquiry_rounds._execute_inquiry_rounds(
        state,
        trigger=trigger,
        conn=_NoPolicyConn(),
        embedder=None,
        llm_provider=None,
        read_pool=None,
    )

    assert state.max_rounds == 0
    assert status.stop_status == "insufficient_continue"
    assert status.stop_reason == "inquiry has not run"
    assert state.all_questions == []
    assert state.answers == []
