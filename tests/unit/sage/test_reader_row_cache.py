from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.reasoning.sage import reader as reader_mod
from services.reasoning.sage.reader import ReaderBudget, SynthesisReader


@pytest.mark.asyncio
async def test_synthesis_reader_row_cache_fetches_only_misses(monkeypatch) -> None:
    tenant_id = uuid4()
    model_a = uuid4()
    model_b = uuid4()
    model_c = uuid4()
    edge_a = uuid4()
    obs_a = uuid4()
    calls: dict[str, list[list[object]]] = {
        "models": [],
        "model_features": [],
        "edge_features": [],
        "observations": [],
    }

    async def fake_load_models(conn, tenant, model_ids):
        calls["models"].append(list(model_ids))
        return {mid: SimpleNamespace(id=mid, kind="model") for mid in model_ids}

    async def fake_load_model_features(conn, tenant, model_ids):
        calls["model_features"].append(list(model_ids))
        return {mid: SimpleNamespace(model_id=mid) for mid in model_ids[:1]}

    async def fake_load_edge_features(conn, tenant, edge_ids):
        calls["edge_features"].append(list(edge_ids))
        return {eid: SimpleNamespace(edge_id=eid) for eid in edge_ids}

    async def fake_load_observations(conn, tenant, observation_ids):
        calls["observations"].append(list(observation_ids))
        return [SimpleNamespace(id=oid) for oid in observation_ids]

    monkeypatch.setattr(reader_mod, "_load_models", fake_load_models)
    monkeypatch.setattr(reader_mod, "_load_model_features", fake_load_model_features)
    monkeypatch.setattr(reader_mod, "_load_edge_features", fake_load_edge_features)
    monkeypatch.setattr(reader_mod, "_load_observations", fake_load_observations)

    reader = SynthesisReader(budget=ReaderBudget(row_cache_enabled=True))

    assert set(
        await reader._load_models_cached(None, tenant_id, [model_a, model_b])
    ) == {model_a, model_b}
    assert set(
        await reader._load_models_cached(None, tenant_id, [model_b, model_c])
    ) == {model_b, model_c}
    assert calls["models"] == [[model_a, model_b], [model_c]]

    features = await reader._load_model_features_cached(
        None, tenant_id, [model_a, model_b]
    )
    assert set(features) == {model_a}
    features = await reader._load_model_features_cached(
        None, tenant_id, [model_a, model_b]
    )
    assert set(features) == {model_a}
    assert calls["model_features"] == [[model_a, model_b]]

    assert set(await reader._load_edge_features_cached(None, tenant_id, [edge_a])) == {
        edge_a
    }
    assert set(await reader._load_edge_features_cached(None, tenant_id, [edge_a])) == {
        edge_a
    }
    assert calls["edge_features"] == [[edge_a]]

    observations = await reader._load_observations_cached(None, tenant_id, [obs_a])
    assert [obs.id for obs in observations] == [obs_a]
    observations = await reader._load_observations_cached(None, tenant_id, [obs_a])
    assert [obs.id for obs in observations] == [obs_a]
    assert calls["observations"] == [[obs_a]]

    stats = reader.cache_stats_snapshot()
    assert stats["model_hits"] == 1
    assert stats["model_misses"] == 3
    assert stats["model_feature_hits"] == 2
    assert stats["model_feature_misses"] == 2
    assert stats["edge_feature_hits"] == 1
    assert stats["edge_feature_misses"] == 1
    assert stats["observation_hits"] == 1
    assert stats["observation_misses"] == 1


@pytest.mark.asyncio
async def test_synthesis_reader_row_cache_can_be_disabled(monkeypatch) -> None:
    tenant_id = uuid4()
    model_id = uuid4()
    calls: list[list[object]] = []

    async def fake_load_models(conn, tenant, model_ids):
        calls.append(list(model_ids))
        return {mid: SimpleNamespace(id=mid) for mid in model_ids}

    monkeypatch.setattr(reader_mod, "_load_models", fake_load_models)

    reader = SynthesisReader(budget=ReaderBudget(row_cache_enabled=False))

    await reader._load_models_cached(None, tenant_id, [model_id])
    await reader._load_models_cached(None, tenant_id, [model_id])

    assert calls == [[model_id], [model_id]]
    assert reader.cache_stats_snapshot() == {}
