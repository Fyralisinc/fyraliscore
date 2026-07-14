from __future__ import annotations

from uuid import uuid4

import pytest

import services.reasoning.think.reason as reason_mod
import services.reasoning.think.residuals as residuals_mod
from services.reasoning.think.reason import ThinkRunOutcome
from services.reasoning.think.residuals import (
    ThinkResidualContext,
    absorb_think_residuals,
    absorption_target_for_applied_summary,
    persist_think_residuals,
    residuals_for_think_context,
)


def test_validation_drop_creates_source_backed_residuals() -> None:
    source_a = uuid4()
    source_b = uuid4()
    ctx = _context(
        source_observation_ids=(source_a, source_b),
        validation_dropped_op_count=1,
        validation_dropped_op_errors=("relation_claim_op requires evidence",),
        apply_dropped_op_count=1,
        apply_dropped_op_errors=("edge source model not found",),
    )

    residuals = residuals_for_think_context(ctx)

    assert [residual.source_observation_id for residual in residuals] == [
        source_a,
        source_b,
    ]
    assert {residual.residual_kind for residual in residuals} == {
        "validation_dropped_value"
    }
    assert residuals[0].metadata["validation_dropped_op_count"] == 1
    assert residuals[0].metadata["apply_dropped_op_count"] == 1
    assert residuals[0].compact_summary.startswith("Think dropped 2 operation")


def test_validation_drop_residual_preserves_repair_provenance() -> None:
    repair_residual_id = uuid4()
    ctx = _context(
        trigger_kind="T4",
        trigger_subkind="representation_repair",
        source_observation_ids=(uuid4(),),
        validation_dropped_op_count=1,
        repair_source="model_residual_evidence",
        repair_key=f"residual:{repair_residual_id}",
        repair_residual_id=repair_residual_id,
        repair_residual_kind="validation_dropped_value",
        repair_intent="repair_validation_dropped_value",
        repair_cascade_depth=1,
    )

    residuals = residuals_for_think_context(ctx)

    assert len(residuals) == 1
    assert residuals[0].metadata["repair_source"] == "model_residual_evidence"
    assert residuals[0].metadata["repair_residual_id"] == str(repair_residual_id)
    assert residuals[0].metadata["repair_intent"] == "repair_validation_dropped_value"


def test_success_without_durable_fate_creates_compression_uncertain_residual() -> None:
    source_id = uuid4()
    ctx = _context(
        source_observation_ids=(source_id,),
        reasoning_trace="The signal was considered but no model write was emitted.",
        ops_applied_summary={
            "context_use": {"context_use_grade": "unused_selected_context"}
        },
    )

    residuals = residuals_for_think_context(ctx)

    assert len(residuals) == 1
    assert residuals[0].source_observation_id == source_id
    assert residuals[0].residual_kind == "compression_uncertain"
    assert "without_durable_fate" in residuals[0].reason


def test_durable_outcome_suppresses_compression_uncertain_residual() -> None:
    ctx = _context(
        source_observation_ids=(uuid4(),),
        ops_applied_summary={
            "claim_ops": [{"op": "insert", "model_id": str(uuid4())}],
            "state_changes_emitted": 1,
        },
    )

    assert residuals_for_think_context(ctx) == []


def test_noise_noop_trace_suppresses_residual() -> None:
    ctx = _context(
        source_observation_ids=(uuid4(),),
        reasoning_trace="discard_as_noise: noise-only T1 trigger",
        ops_applied_summary={"reasoning_trace": "discard_as_noise"},
    )

    assert residuals_for_think_context(ctx) == []


def test_justified_noop_context_use_suppresses_residual() -> None:
    ctx = _context(
        source_observation_ids=(uuid4(),),
        ops_applied_summary={
            "context_use": {"context_use_grade": "justified_noop_context_used"}
        },
    )

    assert residuals_for_think_context(ctx) == []


def test_absorption_target_prefers_specific_relation_target() -> None:
    relation_id = uuid4()
    model_id = uuid4()

    target = absorption_target_for_applied_summary(
        {
            "applied_model_ids": [str(model_id)],
            "relation_frame_ops": [
                {"op": "insert", "relation_instance_id": str(relation_id)}
            ],
        }
    )

    assert target is not None
    assert target.object_kind == "relation_instance"
    assert target.object_id == relation_id


def test_absorption_target_ignores_non_durable_skips() -> None:
    target = absorption_target_for_applied_summary(
        {
            "claim_ops": [
                {"op": "skip", "reason": "quality_gate_noise", "model_id": str(uuid4())}
            ]
        }
    )

    assert target is None


@pytest.mark.asyncio
async def test_persist_think_residuals_delegates_to_repo_idempotency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inserted = []

    class FakeRepo:
        def __init__(self, *, tenant_id):
            self.tenant_id = tenant_id

        async def insert_open(self, residual, *, conn=None):
            inserted.append((residual, conn))
            return residual

    class FakeAcquire:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(
        residuals_mod,
        "ModelResidualEvidenceRepo",
        FakeRepo,
    )

    ctx = _context(
        source_observation_ids=(uuid4(),),
        validation_dropped_op_count=1,
        validation_dropped_op_errors=("claim_op references missing model",),
    )

    count = await persist_think_residuals(FakePool(), ctx)  # type: ignore[arg-type]

    assert count == 1
    assert len(inserted) == 1
    assert inserted[0][0].residual_kind == "validation_dropped_value"


@pytest.mark.asyncio
async def test_absorb_think_residuals_closes_open_source_residuals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_id = uuid4()
    residual_id = uuid4()
    model_id = uuid4()
    absorbed = []

    class FakeResidual:
        id = residual_id
        status = "open"

    class FakeRepo:
        def __init__(self, *, tenant_id):
            self.tenant_id = tenant_id

        async def list_for_observations(self, observation_ids, *, conn=None):
            assert observation_ids == [source_id]
            return [FakeResidual()]

        async def absorb(
            self,
            residual_id_arg,
            *,
            object_kind,
            object_id,
            metadata=None,
            conn=None,
        ):
            absorbed.append((residual_id_arg, object_kind, object_id, metadata))
            return object()

    class FakeAcquire:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(
        residuals_mod,
        "ModelResidualEvidenceRepo",
        FakeRepo,
    )

    count = await absorb_think_residuals(
        FakePool(),  # type: ignore[arg-type]
        _context(
            source_observation_ids=(source_id,),
            ops_applied_summary={"applied_model_ids": [str(model_id)]},
        ),
    )

    assert count == 1
    assert absorbed[0][0] == residual_id
    assert absorbed[0][1] == "model"
    assert absorbed[0][2] == model_id
    assert absorbed[0][3]["source"] == "think_success_residual_absorber"


@pytest.mark.asyncio
async def test_success_residual_write_failure_is_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_persist(*_args, **_kwargs):
        raise RuntimeError("model_residual_evidence missing")

    monkeypatch.setattr(reason_mod, "persist_think_residuals", fail_persist)
    outcome = ThinkRunOutcome(
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind="T1",
        status="success",
        residual_context=_context(source_observation_ids=(uuid4(),)),
    )

    await reason_mod._record_success_residuals(object(), outcome)  # noqa: SLF001

    assert outcome.status == "success"


@pytest.mark.asyncio
async def test_success_residual_absorption_updates_ops_observability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates = []

    async def fake_absorb(*_args, **_kwargs):
        return 2

    async def fake_persist(*_args, **_kwargs):
        return 0

    async def fake_update_think_run(_conn, _run_id, *, ops_applied=None, **_kwargs):
        updates.append(ops_applied)

    class FakeAcquire:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(reason_mod, "absorb_think_residuals", fake_absorb)
    monkeypatch.setattr(reason_mod, "persist_think_residuals", fake_persist)
    monkeypatch.setattr(reason_mod, "update_think_run", fake_update_think_run)
    outcome = ThinkRunOutcome(
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind="T1",
        status="success",
        residual_context=_context(
            source_observation_ids=(uuid4(),),
            ops_applied_summary={"claim_ops": [{"op": "insert"}]},
        ),
    )

    await reason_mod._record_success_residuals(FakePool(), outcome)  # noqa: SLF001

    assert updates[0]["residual_absorptions"]["count"] == 2


@pytest.mark.asyncio
async def test_terminal_residual_repair_resolution_skips_generic_residual_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    residual_id = uuid4()
    updates = []

    async def fake_resolve(_pool, context):
        assert context.repair_residual_id == residual_id
        return reason_mod.ResidualRepairResolution(
            residual_id=residual_id,
            status="rejected",
            reason="terminal_invalid_self_edge",
            terminal=True,
            resolved=True,
        )

    async def fail_generic_path(*_args, **_kwargs):
        raise AssertionError("terminal residual repair must not create fresh residual work")

    async def fake_update_think_run(_conn, _run_id, *, ops_applied=None, **_kwargs):
        updates.append(ops_applied)

    class FakeAcquire:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(reason_mod, "resolve_residual_repair_outcome", fake_resolve)
    monkeypatch.setattr(reason_mod, "absorb_think_residuals", fail_generic_path)
    monkeypatch.setattr(reason_mod, "persist_think_residuals", fail_generic_path)
    monkeypatch.setattr(
        reason_mod,
        "enqueue_residual_repair_triggers_for_sources",
        fail_generic_path,
    )
    monkeypatch.setattr(
        reason_mod,
        "create_latent_gap_hypotheses_for_sources",
        fail_generic_path,
    )
    monkeypatch.setattr(reason_mod, "update_think_run", fake_update_think_run)
    outcome = ThinkRunOutcome(
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind="T4:representation_repair",
        status="success",
        residual_context=_context(
            trigger_kind="T4:representation_repair",
            trigger_subkind="representation_repair",
            source_observation_ids=(uuid4(),),
            validation_dropped_op_count=1,
            validation_dropped_op_errors=("self-edge not allowed",),
            repair_residual_id=residual_id,
            repair_residual_kind="validation_dropped_value",
            repair_intent="repair_validation_dropped_value",
            repair_key=f"residual:{residual_id}",
        ),
    )

    await reason_mod._record_success_residuals(FakePool(), outcome)  # noqa: SLF001

    assert updates[0]["residual_repair_resolution"] == {
        "residual_id": str(residual_id),
        "status": "rejected",
        "reason": "terminal_invalid_self_edge",
        "resolved": True,
        "source": "residual_repair_resolution",
    }


@pytest.mark.asyncio
async def test_success_residual_creation_and_latent_gap_updates_ops_observability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates = []

    async def fake_absorb(*_args, **_kwargs):
        return 0

    async def fake_persist(*_args, **_kwargs):
        return 1

    async def fake_create_latent_gaps(*_args, **_kwargs):
        return [object()]

    async def fake_repair_triggers(*_args, **_kwargs):
        return []

    async def fake_update_think_run(_conn, _run_id, *, ops_applied=None, **_kwargs):
        updates.append(ops_applied)

    class FakeAcquire:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *_args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    monkeypatch.setattr(reason_mod, "absorb_think_residuals", fake_absorb)
    monkeypatch.setattr(reason_mod, "persist_think_residuals", fake_persist)
    monkeypatch.setattr(
        reason_mod,
        "create_latent_gap_hypotheses_for_sources",
        fake_create_latent_gaps,
    )
    monkeypatch.setattr(
        reason_mod,
        "enqueue_residual_repair_triggers_for_sources",
        fake_repair_triggers,
    )
    monkeypatch.setattr(reason_mod, "update_think_run", fake_update_think_run)
    outcome = ThinkRunOutcome(
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind="T1",
        status="success",
        residual_context=_context(
            source_observation_ids=(uuid4(),),
            ops_applied_summary={},
        ),
    )

    await reason_mod._record_success_residuals(FakePool(), outcome)  # noqa: SLF001

    assert updates[0]["residual_creations"]["count"] == 1
    assert updates[0]["latent_gap_hypotheses"]["count"] == 1


def _context(**overrides) -> ThinkResidualContext:
    base = {
        "tenant_id": uuid4(),
        "think_run_id": uuid4(),
        "trigger_id": uuid4(),
        "trigger_kind": "T1",
        "source_observation_ids": (uuid4(),),
        "ops_applied_summary": {},
    }
    base.update(overrides)
    return ThinkResidualContext(**base)
