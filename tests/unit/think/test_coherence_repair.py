from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

import services.reasoning.think.coherence_repair as repair_mod
from services.reasoning.sage.model_residuals import ModelResidualEvidence
from services.reasoning.think.coherence_repair import (
    enqueue_residual_repair_triggers,
    repair_payload_for_residual,
    resolve_residual_repair_outcome,
)
from services.reasoning.think.residuals import ThinkResidualContext


def test_repair_payload_for_open_residual_carries_compact_provenance() -> None:
    residual = _residual(residual_kind="counterevidence_unattached")

    payload = repair_payload_for_residual(residual, cascade_depth=2)

    assert payload is not None
    assert payload["repair_key"] == f"residual:{residual.id}"
    assert payload["repair_intent"] == "attach_counterevidence"
    assert payload["repair_source"] == "model_residual_evidence"
    assert payload["residual_id"] == str(residual.id)
    assert payload["source_observation_id"] == str(residual.source_observation_id)
    assert payload["observation_ids"] == [str(residual.source_observation_id)]
    assert payload["model_id"] == str(residual.model_id)
    assert payload["cascade_depth"] == 2
    assert "counterevidence" in payload["success_metric"]


def test_repair_payload_skips_terminal_or_idless_residual() -> None:
    assert repair_payload_for_residual(_residual(status="absorbed")) is None
    assert repair_payload_for_residual(_residual(id=None)) is None


@pytest.mark.asyncio
async def test_enqueue_residual_repair_triggers_dedupes_existing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    existing_id = uuid4()
    residual = _residual(tenant_id=tenant_id)
    conn = _FakeConn(fetchval_results=[existing_id])

    async def fail_enqueue(*_args, **_kwargs):
        raise AssertionError("enqueue_trigger should not run when deduped")

    monkeypatch.setattr(repair_mod, "enqueue_trigger", fail_enqueue)

    queued = await enqueue_residual_repair_triggers(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        residuals=[residual],
    )

    assert len(queued) == 1
    assert queued[0].id == existing_id
    assert queued[0].repair_key == f"residual:{residual.id}"
    assert queued[0].deduped is True


@pytest.mark.asyncio
async def test_enqueue_residual_repair_triggers_bounds_and_skips_invalid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    first = _residual(tenant_id=tenant_id, residual_kind="valuable_unmodeled")
    second = _residual(tenant_id=tenant_id, residual_kind="compression_uncertain")
    terminal = _residual(tenant_id=tenant_id, status="expired")
    wrong_tenant = _residual(tenant_id=uuid4())
    conn = _FakeConn(fetchval_results=[None, None])
    inserted = []

    async def fake_enqueue(conn_arg, **kwargs):
        inserted.append((conn_arg, kwargs))
        return uuid4()

    monkeypatch.setattr(repair_mod, "enqueue_trigger", fake_enqueue)

    queued = await enqueue_residual_repair_triggers(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        residuals=[first, terminal, wrong_tenant, second],
        max_triggers=1,
    )

    assert len(queued) == 1
    assert queued[0].residual_id == first.id
    assert queued[0].residual_kind == "valuable_unmodeled"
    assert queued[0].deduped is False
    assert len(inserted) == 1
    kwargs = inserted[0][1]
    assert kwargs["trigger_kind"] == "T4"
    assert kwargs["trigger_subkind"] == "representation_repair"
    assert kwargs["observation_id"] == first.source_observation_id
    assert kwargs["model_id"] == first.model_id
    assert kwargs["payload"]["repair_intent"] == "absorb_unmodeled_value"


@pytest.mark.asyncio
async def test_resolve_residual_repair_rejects_terminal_self_edge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    residual_id = uuid4()
    rejected = []

    class FakeRepo:
        def __init__(self, _pool, *, tenant_id):
            self.tenant_id = tenant_id

        async def reject(self, residual_id_arg, *, reason, conn=None):
            rejected.append((residual_id_arg, reason, conn))
            return object()

    monkeypatch.setattr(repair_mod, "ModelResidualEvidenceRepo", FakeRepo)

    resolution = await resolve_residual_repair_outcome(
        object(),  # type: ignore[arg-type]
        ThinkResidualContext(
            tenant_id=tenant_id,
            think_run_id=uuid4(),
            trigger_id=uuid4(),
            trigger_kind="T4:representation_repair",
            trigger_subkind="representation_repair",
            source_observation_ids=(uuid4(),),
            apply_dropped_op_count=1,
            apply_dropped_op_errors=("self-edge not allowed: source == target",),
            ops_applied_summary={
                "memory_lifecycle_ops": [
                    {
                        "action": "unchanged",
                        "model_id": str(uuid4()),
                    }
                ]
            },
            repair_residual_id=residual_id,
            repair_residual_kind="validation_dropped_value",
            repair_intent="repair_validation_dropped_value",
            repair_key=f"residual:{residual_id}",
        ),
    )

    assert resolution is not None
    assert resolution.terminal is True
    assert resolution.status == "rejected"
    assert resolution.reason == "terminal_invalid_self_edge"
    assert rejected == [(residual_id, "terminal_invalid_self_edge", None)]


@pytest.mark.asyncio
async def test_resolve_residual_repair_absorbs_parent_on_durable_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    residual_id = uuid4()
    model_id = uuid4()
    absorbed = []

    class FakeRepo:
        def __init__(self, _pool, *, tenant_id):
            self.tenant_id = tenant_id

        async def absorb(
            self,
            residual_id_arg,
            *,
            object_kind,
            object_id,
            metadata=None,
            conn=None,
        ):
            absorbed.append(
                (residual_id_arg, object_kind, object_id, metadata, conn)
            )
            return object()

    monkeypatch.setattr(repair_mod, "ModelResidualEvidenceRepo", FakeRepo)

    resolution = await resolve_residual_repair_outcome(
        object(),  # type: ignore[arg-type]
        ThinkResidualContext(
            tenant_id=tenant_id,
            think_run_id=uuid4(),
            trigger_id=uuid4(),
            trigger_kind="T4:representation_repair",
            trigger_subkind="representation_repair",
            source_observation_ids=(uuid4(),),
            repair_residual_id=residual_id,
            repair_residual_kind="valuable_unmodeled",
            repair_intent="absorb_unmodeled_value",
            repair_key=f"residual:{residual_id}",
            ops_applied_summary={"applied_model_ids": [str(model_id)]},
        ),
    )

    assert resolution is not None
    assert resolution.terminal is True
    assert resolution.status == "absorbed"
    assert absorbed[0][0] == residual_id
    assert absorbed[0][1] == "model"
    assert absorbed[0][2] == model_id
    assert absorbed[0][3]["source"] == "residual_repair_resolution"


class _FakeConn:
    def __init__(self, *, fetchval_results: list[object | None] | None = None) -> None:
        self.fetchval_results = list(fetchval_results or [])
        self.fetchval_calls = []

    async def fetchval(self, sql: str, *args: object) -> object | None:
        self.fetchval_calls.append((sql, args))
        if not self.fetchval_results:
            return None
        return self.fetchval_results.pop(0)


def _residual(**overrides) -> ModelResidualEvidence:
    base = {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "source_observation_id": uuid4(),
        "think_run_id": uuid4(),
        "trigger_id": uuid4(),
        "model_id": uuid4(),
        "residual_kind": "valuable_unmodeled",
        "compact_summary": "Important company signal was not modeled.",
        "reason": "No durable fate was observed.",
        "status": "open",
        "created_at": datetime.now(timezone.utc),
    }
    base.update(overrides)
    return ModelResidualEvidence(**base)
