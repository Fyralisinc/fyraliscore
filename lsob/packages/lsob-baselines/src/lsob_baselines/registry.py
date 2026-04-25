"""Registry of baseline factories.

All six Phase 1 baselines auto-register themselves on import of the
``lsob_baselines`` package.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from lsob_contracts import SUTConfig, SystemUnderTest

BaselineFactory = Callable[[SUTConfig], SystemUnderTest]


@dataclass
class BaselineRegistry:
    """Thread-unsafe process-local registry of SUT factories."""

    _factories: dict[str, BaselineFactory] = field(default_factory=dict)

    def register(self, name: str, factory: BaselineFactory) -> None:
        if not name:
            raise ValueError("baseline name must be non-empty")
        self._factories[name] = factory

    def construct(self, name: str, config: SUTConfig) -> SystemUnderTest:
        if name not in self._factories:
            known = ", ".join(sorted(self._factories)) or "<none>"
            raise KeyError(f"unknown baseline {name!r} (registered: {known})")
        return self._factories[name](config)

    def list(self) -> list[str]:
        return sorted(self._factories)

    def __contains__(self, name: str) -> bool:  # pragma: no cover - convenience
        return name in self._factories


# Process-wide singleton used by the harness.
REGISTRY = BaselineRegistry()
