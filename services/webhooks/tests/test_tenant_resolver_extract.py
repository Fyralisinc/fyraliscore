"""Unit tests for the per-provider id extractors in
services/webhooks/tenant_resolver.py.

Pure functions — no DB, no async, no cache. The lookup integration
test (test_tenant_resolver_lookup.py) exercises the full pipeline.

Covers FR-003 (extraction rule per provider), FR-006 (PayloadMissing
on absent/empty/malformed id), and US-1 / US-4 acceptance scenarios.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

import pytest

from services.webhooks.tenant_resolver import (
    PROVIDER_EXTRACTORS,
    _str_or_none,
)


SAMPLES = Path(__file__).parent / "samples"


def _load(name: str) -> Mapping[str, object]:
    with open(SAMPLES / name) as fh:
        return json.load(fh)


# =====================================================================
# Slack
# =====================================================================

def test_slack_extracts_team_id() -> None:
    payload = _load("slack_event_callback.json")
    assert PROVIDER_EXTRACTORS["slack"](payload, {}) == "T_ACME_FIXTURE"


def test_slack_missing_team_id_returns_none() -> None:
    assert PROVIDER_EXTRACTORS["slack"]({}, {}) is None
    assert PROVIDER_EXTRACTORS["slack"]({"team_id": ""}, {}) is None
    assert PROVIDER_EXTRACTORS["slack"]({"team_id": None}, {}) is None


# =====================================================================
# GitHub
# =====================================================================

def test_github_extracts_installation_id() -> None:
    payload = _load("github_webhook.json")
    # GitHub stores installation.id as a number; we stringify.
    assert PROVIDER_EXTRACTORS["github"](payload, {}) == "4567890"


def test_github_missing_installation_block_returns_none() -> None:
    assert PROVIDER_EXTRACTORS["github"]({}, {}) is None
    assert PROVIDER_EXTRACTORS["github"]({"installation": None}, {}) is None
    assert PROVIDER_EXTRACTORS["github"]({"installation": {}}, {}) is None
    assert (
        PROVIDER_EXTRACTORS["github"]({"installation": "not-a-dict"}, {}) is None
    )


# =====================================================================
# Linear
# =====================================================================

def test_linear_extracts_organization_id() -> None:
    payload = _load("linear_webhook.json")
    assert PROVIDER_EXTRACTORS["linear"](payload, {}) == "ORG_FIXTURE_UUID"


def test_linear_missing_organization_id_returns_none() -> None:
    assert PROVIDER_EXTRACTORS["linear"]({}, {}) is None


# =====================================================================
# Stripe
# =====================================================================

def test_stripe_extracts_account_header() -> None:
    headers = {"Stripe-Account": "acct_STRIPE_FIXTURE"}
    assert PROVIDER_EXTRACTORS["stripe"]({}, headers) == "acct_STRIPE_FIXTURE"


def test_stripe_extracts_account_header_case_insensitive() -> None:
    # Some HTTP stacks lowercase header keys; the extractor accepts both.
    headers = {"stripe-account": "acct_STRIPE_FIXTURE_LOWER"}
    assert (
        PROVIDER_EXTRACTORS["stripe"]({}, headers)
        == "acct_STRIPE_FIXTURE_LOWER"
    )


def test_stripe_missing_header_returns_none() -> None:
    assert PROVIDER_EXTRACTORS["stripe"]({}, {}) is None
    assert PROVIDER_EXTRACTORS["stripe"]({}, {"Stripe-Account": ""}) is None


# =====================================================================
# Discord
# =====================================================================

def test_discord_prefers_guild_id() -> None:
    payload = _load("discord_interaction.json")
    # Both guild_id and application_id present — guild wins.
    assert PROVIDER_EXTRACTORS["discord"](payload, {}) == "GUILD_FIXTURE"


def test_discord_falls_back_to_application_id() -> None:
    payload = _load("discord_global_command.json")
    # Only application_id present — falls back.
    assert PROVIDER_EXTRACTORS["discord"](payload, {}) == "APP_FIXTURE"


def test_discord_missing_both_returns_none() -> None:
    assert PROVIDER_EXTRACTORS["discord"]({}, {}) is None


# =====================================================================
# _str_or_none corner cases
# =====================================================================

@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("foo", "foo"),
        ("  foo ", "foo"),
        (123, "123"),
        (True, None),       # bools are NOT installation ids
        (False, None),
        ([], None),
        ({}, None),
        (0, "0"),
    ],
)
def test_str_or_none(value: object, expected: str | None) -> None:
    assert _str_or_none(value) == expected
