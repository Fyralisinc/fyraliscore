from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from services.ingest.source_contract import (
    BrowserAgentDefinition,
    SOURCE_DEFINITIONS,
    source_definition,
)
from services.platform.runtime.source_browser_agent_recipes import (
    BROWSER_AGENT_RECIPES,
    browser_agent_recipe_for_source,
)


_ORDERED_PAYLOAD_SHA256 = (
    "18dc132434b5ec9a310aedd84f344006cf9fb8b51c6bddbdd005e11702393422"
)
_RECIPE_ORDER = (
    "ashby",
    "aws",
    "brex",
    "carta",
    "deel",
    "discord",
    "facebook_pages",
    "figma",
    "fireflies",
    "github",
    "gmail",
    "google_calendar",
    "google_drive",
    "grafana",
    "gusto",
    "hibob",
    "jira",
    "linkedin",
    "mercury",
    "miro",
    "notion",
    "quickbooks",
    "ramp",
    "signal",
    "slack",
    "telegram",
    "whatsapp",
)


def _ordered_browser_payload() -> dict[str, dict[str, object]]:
    return {
        source_id: recipe.as_dict()
        for source_id, recipe in BROWSER_AGENT_RECIPES.items()
    }


def test_browser_recipe_view_preserves_exact_27_source_payload_and_order() -> None:
    payload = _ordered_browser_payload()
    wire_payload = json.dumps(
        payload,
        sort_keys=False,
        separators=(",", ":"),
    ).encode()

    assert tuple(payload) == _RECIPE_ORDER
    assert len(payload) == len(SOURCE_DEFINITIONS) == 27
    assert hashlib.sha256(wire_payload).hexdigest() == (
        _ORDERED_PAYLOAD_SHA256
    )


def test_every_runtime_recipe_is_derived_from_its_source_contract() -> None:
    for source in SOURCE_DEFINITIONS:
        onboarding = source.onboarding
        assert onboarding.provider_console_url is not None
        assert browser_agent_recipe_for_source(source.source_id) == (
            onboarding.browser_agent.as_payload(
                source=source.source_id,
                provider_console_url=onboarding.provider_console_url,
            )
        )


def test_browser_recipe_payload_returns_fresh_mutable_lists() -> None:
    payload = browser_agent_recipe_for_source("slack")
    payload["settings_targets"].append("mutated")  # type: ignore[union-attr]

    assert "mutated" not in browser_agent_recipe_for_source("slack")[
        "settings_targets"
    ]
    assert "mutated" not in (
        source_definition("slack").onboarding.browser_agent.settings_targets
    )


def test_browser_agent_contract_rejects_empty_or_duplicate_steps() -> None:
    slack = source_definition("slack").onboarding.browser_agent

    with pytest.raises(ValueError, match="must not be empty"):
        replace(slack, settings_targets=())
    with pytest.raises(ValueError, match="duplicate"):
        replace(
            slack,
            human_gates=("admin approval", "admin approval"),
        )


def test_browser_agent_payload_rejects_untrusted_console_url() -> None:
    definition = BrowserAgentDefinition(
        settings_targets=("settings",),
        agent_collects=("tenant id",),
        agent_generates=("secret ref",),
        human_gates=("admin approval",),
        completion_checks=("connection proof",),
    )

    with pytest.raises(ValueError, match="must use HTTPS"):
        definition.as_payload(
            source="slack",
            provider_console_url="http://api.slack.test/apps",
        )
