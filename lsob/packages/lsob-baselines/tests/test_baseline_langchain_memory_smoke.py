"""Smoke test for the LangChainMemoryBaseline (in-memory fallback)."""

from __future__ import annotations

from lsob_baselines import REGISTRY


async def test_langchain_memory_smoke(smoke_runner, sut_config):
    await smoke_runner(lambda: REGISTRY.construct("langchain-memory", sut_config))
