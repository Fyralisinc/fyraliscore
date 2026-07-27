from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from services.ingest.source_contract import (
    PROVIDER_DEFINITIONS,
    SOURCE_CONNECTION_CATALOG,
    SOURCE_CONNECTION_SLUGS,
    SOURCE_DEFINITIONS,
    CatalogValidationError,
    source_connection_definition,
    source_connection_profile,
    source_local_rehearsal_profile,
    validate_catalog,
)


_REVIEWED_SOURCE_PROFILE_SHA256 = (
    "4ee0ffb1f39c66fa4cc6f125aa56fc7ee95ac56b39de1c7b8c66f4173a68b036"
)
_LEGACY_REHEARSAL_PROFILE_SHA256 = (
    "0251500def0f45cdc46654842bacc1c259a4b8bf5bf4c4afc21088191c28a204"
)


def _wire_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_cli_source_profiles_match_reviewed_payloads_and_order_exactly() -> None:
    assert SOURCE_CONNECTION_SLUGS == (
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
        "google-calendar",
        "google-drive",
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
    payload = {
        slug: source_connection_profile(slug)
        for slug in SOURCE_CONNECTION_SLUGS
    }
    assert _wire_hash(payload) == _REVIEWED_SOURCE_PROFILE_SHA256


def test_explicit_rehearsal_profiles_preserve_legacy_payloads_exactly() -> None:
    payload = {
        slug: profile
        for slug in SOURCE_CONNECTION_SLUGS
        if (profile := source_local_rehearsal_profile(slug)) is not None
    }
    assert tuple(payload) == (
        "discord",
        "facebook_pages",
        "figma",
        "github",
        "jira",
        "notion",
        "slack",
        "telegram",
    )
    assert _wire_hash(payload) == _LEGACY_REHEARSAL_PROFILE_SHA256


def test_cli_views_are_read_only_and_payloads_are_defensive_copies() -> None:
    with pytest.raises(TypeError):
        SOURCE_CONNECTION_CATALOG["new-source"] = source_connection_definition(
            "slack"
        )

    profile = source_connection_profile("slack")
    profile["method"] = "mutated"
    profile["default_scopes"].append("mutated")
    profile["native_connect"]["payload_fields"].append("mutated")

    fresh = source_connection_profile("slack")
    assert fresh["method"] == "oauth"
    assert "mutated" not in fresh["default_scopes"]
    assert "mutated" not in fresh["native_connect"]["payload_fields"]


def test_catalog_validation_rejects_missing_cli_contract_metadata() -> None:
    slack = source_connection_definition("slack")
    invalid_sources = tuple(
        replace(
            source,
            onboarding=replace(source.onboarding, default_scopes=()),
        )
        if source is slack
        else source
        for source in SOURCE_DEFINITIONS
    )

    with pytest.raises(CatalogValidationError, match="default_scopes"):
        validate_catalog(PROVIDER_DEFINITIONS, invalid_sources)


def test_ui_slugs_are_contract_owned_and_resolve_exact_cli_names() -> None:
    assert source_connection_definition("google-calendar").source_id == (
        "google_calendar"
    )
    assert source_connection_definition("google-drive").source_id == (
        "google_drive"
    )
    assert source_connection_definition("facebook_pages").source_id == (
        "facebook_pages"
    )
    with pytest.raises(KeyError, match="unknown source connection slug"):
        source_connection_definition("google_calendar")
