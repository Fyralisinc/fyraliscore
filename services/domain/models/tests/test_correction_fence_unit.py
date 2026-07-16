from __future__ import annotations

from types import SimpleNamespace

import pytest

from lib.shared.ids import uuid7
from services.domain.models import repo as repo_module
from services.domain.models.repo import ModelsRepo
from services.reasoning.think import audit as audit_module


pytestmark = pytest.mark.asyncio


class _Connection:
    def __init__(self, rows) -> None:
        self.rows = list(rows)
        self.calls = []

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return self.rows.pop(0)


async def test_fence_for_correction_is_tenant_scoped_audited_and_evented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid7()
    model_id = uuid7()
    cause_model_id = uuid7()
    observation_id = uuid7()
    hydrated = SimpleNamespace(id=model_id, tenant_id=tenant_id)
    conn = _Connection(
        [
            {"status": "active", "visible_to_subjects": True},
            {"id": model_id},
        ]
    )
    emitted = []

    async def _noop_codec(_conn):
        return None

    async def _record_state(_conn, **kwargs):
        emitted.append(("state", kwargs))

    async def _record_audit(_conn, **kwargs):
        emitted.append(("audit", kwargs))
        return 1

    async def _record_event(_conn, **kwargs):
        emitted.append(("event", kwargs))
        return uuid7()

    monkeypatch.setattr(repo_module, "_ensure_vector_codec", _noop_codec)
    monkeypatch.setattr(repo_module, "_hydrate_row", lambda _row: hydrated)
    monkeypatch.setattr(repo_module, "emit_state_change", _record_state)
    monkeypatch.setattr(repo_module, "emit_model_event", _record_event)
    monkeypatch.setattr(audit_module, "emit_audit_event", _record_audit)

    changed = await ModelsRepo(
        pool=None,  # type: ignore[arg-type]
        embedder=None,
    ).fence_for_correction(
        model_id,
        tenant_id=tenant_id,
        cause_event_id=observation_id,
        cause_model_id=cause_model_id,
        conn=conn,  # type: ignore[arg-type]
    )

    assert changed is hydrated
    update_sql, update_args = conn.calls[1]
    assert "SET visible_to_subjects = FALSE" in update_sql
    assert "tenant_id = $2" in update_sql
    assert update_args == (model_id, tenant_id)
    assert [kind for kind, _kwargs in emitted] == ["state", "audit", "event"]
    assert emitted[1][1]["changed_fields"] == ["visible_to_subjects"]
    assert emitted[2][1]["changed_fields"] == ["visible_to_subjects"]


async def test_fence_for_correction_is_idempotent_when_already_hidden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _Connection([{"status": "active", "visible_to_subjects": False}])

    async def _noop_codec(_conn):
        return None

    monkeypatch.setattr(repo_module, "_ensure_vector_codec", _noop_codec)
    result = await ModelsRepo(
        pool=None,  # type: ignore[arg-type]
        embedder=None,
    ).fence_for_correction(
        uuid7(),
        tenant_id=uuid7(),
        cause_event_id=uuid7(),
        cause_model_id=uuid7(),
        conn=conn,  # type: ignore[arg-type]
    )

    assert result is None
    assert len(conn.calls) == 1
