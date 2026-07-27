"""Dependency and value invariants for outbound provider endpoints."""

from __future__ import annotations

from pathlib import Path

import pytest

from lib.integrations.endpoint_contract import (
    PROVIDER_ENDPOINT_CATALOG,
    PROVIDER_ENDPOINT_DEFINITIONS,
    ProviderEndpointDefinition,
)


def test_endpoint_contract_has_unique_names_and_override_environment_keys() -> None:
    assert len(PROVIDER_ENDPOINT_DEFINITIONS) == 27
    assert tuple(PROVIDER_ENDPOINT_CATALOG) == tuple(
        definition.endpoint_name for definition in PROVIDER_ENDPOINT_DEFINITIONS
    )
    assert len(
        {definition.override_env for definition in PROVIDER_ENDPOINT_DEFINITIONS}
    ) == len(PROVIDER_ENDPOINT_DEFINITIONS)


def test_exact_production_defaults_and_installation_scoped_exceptions() -> None:
    assert (
        PROVIDER_ENDPOINT_CATALOG["gmail_pubsub_api"].production_base_url
        == "https://pubsub.googleapis.com/v1"
    )
    assert (
        PROVIDER_ENDPOINT_CATALOG["linkedin_api"].override_env
        == "LINKEDIN_API_BASE_URL"
    )
    assert PROVIDER_ENDPOINT_CATALOG["jira_api"].production_base_url == ""
    assert PROVIDER_ENDPOINT_CATALOG["grafana_api"].production_base_url == ""


@pytest.mark.parametrize(
    "definition",
    (
        ProviderEndpointDefinition(
            "valid_name",
            "https://provider.example/api",
            "VALID_NAME_BASE_URL",
        ),
        ProviderEndpointDefinition(
            "installation_scoped",
            "",
            "INSTALLATION_SCOPED_BASE_URL",
        ),
    ),
)
def test_valid_endpoint_definitions_are_immutable_contract_values(
    definition: ProviderEndpointDefinition,
) -> None:
    assert definition.endpoint_name


@pytest.mark.parametrize(
    "args",
    (
        ("BadName", "https://provider.example", "BAD_NAME_URL"),
        ("valid", "http://provider.example", "VALID_URL"),
        ("valid", "https://user@provider.example", "VALID_URL"),
        ("valid", "https://provider.example/", "VALID_URL"),
        ("valid", "https://provider.example", "not_an_env"),
    ),
)
def test_endpoint_definition_rejects_ambiguous_or_unsafe_values(
    args: tuple[str, str, str],
) -> None:
    with pytest.raises(ValueError):
        ProviderEndpointDefinition(*args)


def test_low_level_resolver_does_not_reverse_import_ingestion_services() -> None:
    source = Path("lib/integrations/endpoints.py").read_text(encoding="utf-8")
    assert "services.ingest" not in source
