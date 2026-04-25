"""Smoke test for the MemGPTStyleBaseline."""

from __future__ import annotations

from lsob_baselines import REGISTRY


async def test_memgpt_style_smoke(smoke_runner, sut_config):
    await smoke_runner(lambda: REGISTRY.construct("memgpt-style", sut_config))
