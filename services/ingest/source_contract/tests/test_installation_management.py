from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from types import MappingProxyType

import pytest

from services.ingest.source_contract import (
    INSTALLATION_MANAGEMENT_CATALOG,
    InstallationManagementDefinition,
    source_definition,
)


_GOLDEN_MANAGEMENT_ROWS = {
    "gmail": (
        "gmail_installations",
        "workspace_domain",
        (),
        "gmail_mailbox_watches",
        "gmail_installation_id",
        None,
        (),
        None,
        None,
        None,
        None,
        False,
    ),
    "google_calendar": (
        "google_calendar_installations",
        "workspace_domain",
        (),
        "google_calendar_calendars",
        "google_calendar_installation_id",
        None,
        (),
        None,
        None,
        None,
        None,
        True,
    ),
    "google_drive": (
        "google_drive_installations",
        "workspace_domain",
        (),
        "google_drive_targets",
        "google_drive_installation_id",
        None,
        (),
        None,
        None,
        None,
        None,
        True,
    ),
    "whatsapp": (
        "whatsapp_installations",
        "phone_number_id",
        ("app_secret_ref", "verify_token_ref", "access_token_ref"),
        None,
        None,
        None,
        (),
        None,
        None,
        "enabled",
        "updated_at",
        False,
    ),
    "quickbooks": (
        "quickbooks_installations",
        "realm_id",
        ("secret_ref", "refresh_secret_ref", "webhook_secret_ref"),
        "quickbooks_entities",
        "quickbooks_installation_id",
        "base_url",
        (),
        "realm_id",
        None,
        None,
        None,
        False,
    ),
    "gusto": (
        "gusto_installations",
        "company_uuid",
        ("secret_ref", "refresh_secret_ref", "webhook_secret_ref"),
        "gusto_entities",
        "gusto_installation_id",
        "base_url",
        (),
        "company_uuid",
        None,
        None,
        None,
        False,
    ),
    "ramp": (
        "ramp_installations",
        "business_id",
        ("secret_ref", "refresh_secret_ref", "webhook_secret_ref"),
        "ramp_entities",
        "ramp_installation_id",
        "base_url",
        (),
        "business_id",
        None,
        None,
        None,
        False,
    ),
    "carta": (
        "carta_installations",
        "firm_id",
        ("secret_ref", "refresh_secret_ref"),
        "carta_entities",
        "carta_installation_id",
        "base_url",
        (),
        None,
        None,
        None,
        None,
        False,
    ),
    "linkedin": (
        "linkedin_installations",
        "organization_urn",
        ("secret_ref", "refresh_secret_ref"),
        "linkedin_entities",
        "linkedin_installation_id",
        "base_url",
        (),
        None,
        None,
        None,
        None,
        False,
    ),
    "jira": (
        "jira_installations",
        "base_url",
        ("secret_ref", "webhook_secret_ref"),
        "jira_projects",
        "jira_installation_id",
        "base_url",
        (),
        "base_url",
        "host",
        None,
        None,
        False,
    ),
    "mercury": (
        "mercury_installations",
        "organization_id",
        ("secret_ref", "webhook_secret_ref"),
        "mercury_accounts",
        "mercury_installation_id",
        "base_url",
        (),
        "organization_id",
        None,
        None,
        None,
        False,
    ),
    "brex": (
        "brex_installations",
        "organization_id",
        ("secret_ref", "webhook_secret_ref"),
        "brex_accounts",
        "brex_installation_id",
        "base_url",
        (),
        "organization_id",
        None,
        None,
        None,
        False,
    ),
    "deel": (
        "deel_installations",
        "organization_id",
        ("secret_ref", "webhook_secret_ref"),
        "deel_contracts",
        "deel_installation_id",
        "base_url",
        (),
        "organization_id",
        None,
        None,
        None,
        False,
    ),
    "fireflies": (
        "fireflies_installations",
        "workspace_id",
        ("secret_ref", "webhook_secret_ref"),
        None,
        None,
        "base_url",
        (),
        "workspace_id",
        None,
        None,
        None,
        False,
    ),
    "miro": (
        "miro_installations",
        "org_id",
        ("secret_ref", "webhook_secret_ref"),
        "miro_boards",
        "miro_installation_id",
        "base_url",
        (),
        "org_id",
        None,
        None,
        None,
        False,
    ),
    "grafana": (
        "grafana_installations",
        "base_url",
        ("secret_ref", "webhook_secret_ref"),
        None,
        None,
        "base_url",
        (),
        "base_url",
        "host",
        None,
        None,
        False,
    ),
    "figma": (
        "figma_installations",
        "team_id",
        ("secret_ref", "webhook_secret_ref"),
        "figma_files",
        "figma_installation_id",
        "base_url",
        (),
        "team_id",
        None,
        None,
        None,
        False,
    ),
    "hibob": (
        "hibob_installations",
        "company_id",
        ("secret_ref", "webhook_secret_ref"),
        "hibob_entities",
        "hibob_installation_id",
        "base_url",
        (),
        "company_id",
        None,
        None,
        None,
        False,
    ),
    "ashby": (
        "ashby_installations",
        "org_id",
        ("secret_ref", "webhook_secret_ref"),
        "ashby_entities",
        "ashby_installation_id",
        "base_url",
        (),
        "org_id",
        None,
        None,
        None,
        False,
    ),
    "aws": (
        "aws_installations",
        "account_id",
        ("secret_ref",),
        None,
        None,
        None,
        ("region", "credential_kind"),
        None,
        None,
        None,
        None,
        False,
    ),
    "telegram": (
        "telegram_installations",
        "account_label",
        (
            "api_hash_secret_ref",
            "session_secret_ref",
            "backfill_session_secret_ref",
        ),
        "telegram_dialogs",
        "telegram_installation_id",
        None,
        (),
        None,
        None,
        None,
        None,
        False,
    ),
    "signal": (
        "signal_installations",
        "account_label",
        ("session_secret_ref", "backfill_session_secret_ref"),
        "signal_threads",
        "signal_installation_id",
        None,
        (),
        None,
        None,
        None,
        None,
        False,
    ),
}


def _management_row(
    definition: InstallationManagementDefinition,
) -> tuple[object, ...]:
    return (
        definition.table,
        definition.scope_column,
        definition.ref_columns,
        definition.entity_table,
        definition.entity_install_column,
        definition.base_url_column,
        definition.extra_output_columns,
        definition.webhook_installation_id_column,
        definition.webhook_installation_id_transform,
        definition.enabled_column,
        definition.updated_at_column,
        definition.native_google_watch_table,
    )


def test_dedicated_installation_management_has_exact_22_source_parity() -> None:
    assert isinstance(INSTALLATION_MANAGEMENT_CATALOG, MappingProxyType)
    assert set(INSTALLATION_MANAGEMENT_CATALOG) == set(_GOLDEN_MANAGEMENT_ROWS)
    assert len(INSTALLATION_MANAGEMENT_CATALOG) == 22
    assert {
        source: _management_row(definition)
        for source, definition in INSTALLATION_MANAGEMENT_CATALOG.items()
    } == _GOLDEN_MANAGEMENT_ROWS


def test_management_view_is_derived_from_each_source_definition() -> None:
    for source, management in INSTALLATION_MANAGEMENT_CATALOG.items():
        adapter = source_definition(source).installation_adapter
        assert adapter is not None
        assert adapter.management is management
        assert management.source == source

    assert source_definition("whatsapp").history is None
    assert source_definition("whatsapp").installation_adapter is not None


def test_management_contract_and_catalog_are_immutable() -> None:
    with pytest.raises(TypeError):
        INSTALLATION_MANAGEMENT_CATALOG["gmail"] = (  # type: ignore[index]
            INSTALLATION_MANAGEMENT_CATALOG["gmail"]
        )
    with pytest.raises(FrozenInstanceError):
        INSTALLATION_MANAGEMENT_CATALOG["gmail"].table = "other"  # type: ignore[misc]


def test_management_contract_rejects_unsafe_or_incomplete_sql_metadata() -> None:
    gmail = INSTALLATION_MANAGEMENT_CATALOG["gmail"]
    with pytest.raises(ValueError, match="lowercase snake-case"):
        replace(gmail, table="gmail-installations")
    with pytest.raises(ValueError, match="declared together"):
        replace(gmail, entity_install_column=None)

    quickbooks = INSTALLATION_MANAGEMENT_CATALOG["quickbooks"]
    with pytest.raises(ValueError, match="webhook_secret_ref"):
        replace(quickbooks, webhook_installation_id_column=None)
    with pytest.raises(ValueError, match="unknown.*transform"):
        replace(quickbooks, webhook_installation_id_transform="path")


def test_source_rejects_foreign_installation_management_metadata() -> None:
    gmail = source_definition("gmail")
    assert gmail.installation_adapter is not None
    assert gmail.installation_adapter.management is not None
    foreign_management = replace(
        gmail.installation_adapter.management,
        source="jira",
    )

    with pytest.raises(ValueError, match="must match SourceDefinition"):
        replace(
            gmail,
            installation_adapter=replace(
                gmail.installation_adapter,
                management=foreign_management,
            ),
        )


def test_live_only_management_does_not_create_a_historical_loader() -> None:
    whatsapp = source_definition("whatsapp")
    assert whatsapp.installation_adapter is not None
    assert whatsapp.installation_adapter.loader_binding is None

    with pytest.raises(ValueError, match="history=None"):
        replace(
            whatsapp,
            installation_adapter=replace(
                whatsapp.installation_adapter,
                loader_binding=(
                    "services.ingest.ingestion.installations:"
                    "load_slack_installation"
                ),
            ),
        )
