"""Contract-derived customer-cloud browser-agent recipes.

The UI intentionally stays small: source cards and a Connect button. The
immutable automation metadata lives in each source's onboarding contract;
this module retains the legacy runtime view consumed by the gateway, CLI, and
browser-agent workflow.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.source_contract.models import SourceDefinition


@dataclass(frozen=True)
class BrowserAgentRecipe:
    """Legacy runtime view assembled from one ``SourceDefinition``."""

    source: str
    provider_console_url: str
    settings_targets: tuple[str, ...]
    agent_collects: tuple[str, ...]
    agent_generates: tuple[str, ...]
    human_gates: tuple[str, ...]
    completion_checks: tuple[str, ...]

    @classmethod
    def from_source_definition(
        cls,
        source: SourceDefinition,
    ) -> BrowserAgentRecipe:
        """Build the public view without owning source-specific metadata."""

        onboarding = source.onboarding
        provider_console_url = onboarding.provider_console_url
        if provider_console_url is None:
            raise ValueError(
                f"source {source.source_id!r} has no provider console URL"
            )
        definition = onboarding.browser_agent
        return cls(
            source=source.source_id,
            provider_console_url=provider_console_url,
            settings_targets=definition.settings_targets,
            agent_collects=definition.agent_collects,
            agent_generates=definition.agent_generates,
            human_gates=definition.human_gates,
            completion_checks=definition.completion_checks,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "provider_console_url": self.provider_console_url,
            "settings_targets": list(self.settings_targets),
            "agent_collects": list(self.agent_collects),
            "agent_generates": list(self.agent_generates),
            "human_gates": list(self.human_gates),
            "completion_checks": list(self.completion_checks),
        }


# Preserve the historical alphabetical iteration order while deriving every
# value from the canonical source contract. This is intentionally a
# comprehension, not another literal source registry.
BROWSER_AGENT_RECIPES: dict[str, BrowserAgentRecipe] = {
    source.source_id: BrowserAgentRecipe.from_source_definition(source)
    for source in sorted(SOURCE_DEFINITIONS, key=lambda item: item.source_id)
}


def browser_agent_recipe_for_source(source: str) -> dict[str, Any]:
    recipe = BROWSER_AGENT_RECIPES.get(source)
    if recipe is None:
        raise KeyError(source)
    return recipe.as_dict()


def missing_browser_agent_recipe_sources(sources: set[str]) -> set[str]:
    return set(sources) - set(BROWSER_AGENT_RECIPES)


__all__ = [
    "BROWSER_AGENT_RECIPES",
    "BrowserAgentRecipe",
    "browser_agent_recipe_for_source",
    "missing_browser_agent_recipe_sources",
]
