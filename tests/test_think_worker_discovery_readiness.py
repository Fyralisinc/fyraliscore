from __future__ import annotations

from dataclasses import dataclass

import pytest

import scripts.run_think_worker as launcher
from services.domain.entity_grounding.learned_discovery import (
    DiscoveryProviderPreflightError,
)


@dataclass(frozen=True)
class _Config:
    provider: str = "codex"
    model: str = "gpt-unsupported"
    circuit_breaker_name: str | None = None


class _Provider:
    def __init__(self, model: str = "gpt-unsupported") -> None:
        self.config = _Config(model=model)


class _Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def error(self, event: str, **_fields) -> None:
        self.events.append(("error", event))

    def warning(self, event: str, **_fields) -> None:
        self.events.append(("warning", event))


@pytest.mark.asyncio
async def test_startup_refuses_failed_primary_without_explicit_fallback(
    monkeypatch,
) -> None:
    monkeypatch.delenv("ENTITY_DISCOVERY_FALLBACK_MODELS", raising=False)

    async def fail(_provider) -> None:
        raise DiscoveryProviderPreflightError(
            "outdated", code="unsupported_or_outdated_model", retryable=False
        )

    monkeypatch.setattr(launcher, "preflight_structured_discovery", fail)

    with pytest.raises(RuntimeError, match="worker startup refused"):
        await launcher._ready_discovery_provider(_Provider(), log=_Log())


@pytest.mark.asyncio
async def test_only_explicitly_allowlisted_fallback_can_replace_primary(
    monkeypatch,
) -> None:
    monkeypatch.setenv("ENTITY_DISCOVERY_FALLBACK_MODELS", "gpt-approved")
    built: list[str] = []

    def build(config):
        built.append(config.model)
        return _Provider(config.model)

    async def preflight(provider) -> None:
        if provider.config.model != "gpt-approved":
            raise DiscoveryProviderPreflightError(
                "unsupported", code="unsupported_or_outdated_model", retryable=False
            )

    log = _Log()
    monkeypatch.setattr(launcher, "build_provider", build)
    monkeypatch.setattr(launcher, "preflight_structured_discovery", preflight)

    selected = await launcher._ready_discovery_provider(_Provider(), log=log)

    assert selected.config.model == "gpt-approved"
    assert built == ["gpt-approved"]
    assert ("warning", "think_worker.entity_discovery_explicit_fallback_selected") in log.events
