"""Smoke test for the LlamaIndexKGBaseline (in-memory fallback)."""

from __future__ import annotations

from lsob_baselines import REGISTRY


async def test_llamaindex_kg_smoke(smoke_runner, sut_config):
    await smoke_runner(lambda: REGISTRY.construct("llamaindex-kg", sut_config))
