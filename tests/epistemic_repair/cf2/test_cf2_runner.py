from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from services.evaluation.epistemic_repair import cf2_runner


pytestmark = pytest.mark.asyncio


class _Connection:
    def __init__(self) -> None:
        self.metadata_rows = []

    async def executemany(self, _query, rows) -> None:
        self.metadata_rows.extend(rows)


async def test_runner_uses_intact_batch_and_prepares_before_think(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    tenant_id = uuid4()
    conn = _Connection()
    events: list[str] = []
    grounded = []

    async def fake_persist(_conn, *, tenant_id, batch):
        events.append("persist")
        assert len(batch.signals) == 25
        return {signal.signal_id: uuid4() for signal in batch.signals}

    async def fake_ground(_conn, signal):
        grounded.append(signal)
        return object()

    async def fake_ensure(_conn, **_kwargs):
        events.append("partition")
        return []

    async def fake_run(*, population, dependencies, **_kwargs):
        assert len(population.batches) == 4
        assert all(len(batch.signals) == 25 for batch in population.batches)
        batch = population.batches[0]
        ids = await dependencies.persist_runtime_batch(conn, tenant_id, batch)
        await dependencies.prepare_persisted_batch(conn, tenant_id, batch, ids)
        events.append("think")
        return {"waves": [{"batch_number": 1, "snapshot": {}}], "complete": True}

    monkeypatch.setattr(cf2_runner, "_persist_runtime_batch", fake_persist)
    monkeypatch.setattr(cf2_runner, "ensure_partitions", fake_ensure)
    monkeypatch.setattr(
        cf2_runner, "persist_source_authenticated_grounding", fake_ground,
    )
    monkeypatch.setattr(cf2_runner, "run_p6_think_with_dependencies", fake_run)

    artifact = await cf2_runner.run_cf2_provider_free(
        database_url="postgresql://unused",
        checkpoint_path=tmp_path / "cf2.json",
        tenant_id=tenant_id,
        max_batches=1,
    )

    assert events == ["partition", "persist", "think"]
    assert len(conn.metadata_rows) == 25
    assert len(grounded) == 25
    assert all(isinstance(item.observation_id, UUID) for item in grounded)
    assert artifact["gold_visible_during_execution"] is False
    assert artifact["zero_seed_requested"] is True
    assert artifact["batch_preparation"] == [{
        "batch_number": 1,
        "signal_count": 25,
        "source_grounded_count": 25,
        "source_grounding_abstained_count": 0,
        "metadata_preserved_count": 25,
    }]
    assert artifact["provider_telemetry"]["call_count"] == 0
    assert (tmp_path / "cf2.json").exists()


async def test_runner_preserves_authoritative_trust_and_source_space(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    conn = _Connection()
    tenant_id = uuid4()

    async def fake_persist(_conn, *, tenant_id, batch):
        return {signal.signal_id: uuid4() for signal in batch.signals}

    async def fake_ground(_conn, _signal):
        return None

    async def fake_ensure(_conn, **_kwargs):
        return []

    async def fake_run(*, population, dependencies, **_kwargs):
        batch = population.batches[3]
        ids = await dependencies.persist_runtime_batch(conn, tenant_id, batch)
        await dependencies.prepare_persisted_batch(conn, tenant_id, batch, ids)
        return {"waves": [], "complete": True}

    monkeypatch.setattr(cf2_runner, "_persist_runtime_batch", fake_persist)
    monkeypatch.setattr(cf2_runner, "ensure_partitions", fake_ensure)
    monkeypatch.setattr(
        cf2_runner, "persist_source_authenticated_grounding", fake_ground,
    )
    monkeypatch.setattr(cf2_runner, "run_p6_think_with_dependencies", fake_run)

    await cf2_runner.run_cf2_provider_free(
        database_url="postgresql://unused",
        checkpoint_path=tmp_path / "cf2.json",
        tenant_id=tenant_id,
    )

    payloads = [(row[2], row[3]) for row in conn.metadata_rows]
    assert sum(trust == "authoritative" for _source, trust in payloads) == 3
    assert all("source_space" in source for source, _trust in payloads)


async def test_returned_artifact_matches_checkpoint_with_nested_runtime_types(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    tenant_id = uuid4()
    nested_id = uuid4()
    captured_at = datetime(2026, 7, 18, 12, 30, tzinfo=timezone.utc)
    checkpoint = tmp_path / "cf2.json"

    async def fake_run(**_kwargs):
        return {
            "waves": [{
                "batch_number": 1,
                "snapshot": {
                    "model_ids": (nested_id,),
                    "captured_at": captured_at,
                },
            }],
            "complete": True,
        }

    monkeypatch.setattr(cf2_runner, "run_p6_think_with_dependencies", fake_run)

    artifact = await cf2_runner.run_cf2_provider_free(
        database_url="postgresql://unused",
        checkpoint_path=checkpoint,
        tenant_id=tenant_id,
        max_batches=1,
    )

    persisted = json.loads(checkpoint.read_text())
    assert artifact == persisted
    assert artifact["waves"][0]["snapshot"] == {
        "model_ids": [str(nested_id)],
        "captured_at": captured_at.isoformat(),
    }
