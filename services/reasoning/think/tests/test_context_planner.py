from __future__ import annotations

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.context_planner import (
    _should_emit_missing_transition_triggers,
)


def test_missing_transition_emission_only_runs_for_t1() -> None:
    tenant_id = uuid7()

    assert _should_emit_missing_transition_triggers(
        TriggerContext(kind="T1", tenant_id=tenant_id)
    )
    assert not _should_emit_missing_transition_triggers(
        TriggerContext(kind="T2", tenant_id=tenant_id)
    )
    assert not _should_emit_missing_transition_triggers(
        TriggerContext(kind="T3", tenant_id=tenant_id)
    )
    assert not _should_emit_missing_transition_triggers(
        TriggerContext(kind="T4", tenant_id=tenant_id)
    )
