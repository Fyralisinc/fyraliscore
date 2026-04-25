"""Smoke test for the VanillaRAGBaseline (in-memory path)."""

from __future__ import annotations

from lsob_baselines import REGISTRY


async def test_vanilla_rag_smoke(smoke_runner, sut_config):
    await smoke_runner(lambda: REGISTRY.construct("vanilla-rag", sut_config))
