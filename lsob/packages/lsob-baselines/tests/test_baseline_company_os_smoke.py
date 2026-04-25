"""Smoke test for the CompanyOSBaseline (mock client path, Phase 1)."""

from __future__ import annotations

from lsob_baselines import REGISTRY


async def test_company_os_smoke(smoke_runner, sut_config):
    await smoke_runner(lambda: REGISTRY.construct("company-os", sut_config))
