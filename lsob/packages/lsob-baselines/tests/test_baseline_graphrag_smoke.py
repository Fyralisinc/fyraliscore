"""Smoke test for the GraphRAGBaseline."""

from __future__ import annotations

from lsob_baselines import REGISTRY


async def test_graphrag_smoke(smoke_runner, sut_config):
    await smoke_runner(lambda: REGISTRY.construct("graphrag", sut_config))
