"""Dependency-light contract for named outbound provider endpoints.

The ingestion source catalog binds provider operations to these stable endpoint
names.  This module intentionally lives below ``services.ingest`` so both the
validated source contract and low-level integration clients can import the same
definitions without reversing the dependency direction.

An empty production base is intentional for providers whose real URL is owned
by an exact installation.  Their named endpoint remains useful as an explicit
Provider Lab override; production client construction still uses the
installation URL.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlsplit


_ENDPOINT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ProviderEndpointDefinition:
    """One stable resolver name, production default, and explicit override."""

    endpoint_name: str
    production_base_url: str
    override_env: str

    def __post_init__(self) -> None:
        if _ENDPOINT_NAME_RE.fullmatch(self.endpoint_name) is None:
            raise ValueError(
                "endpoint_name must be a lowercase identifier; got "
                f"{self.endpoint_name!r}"
            )
        if _ENV_NAME_RE.fullmatch(self.override_env) is None:
            raise ValueError(
                "override_env must be an uppercase environment key; got "
                f"{self.override_env!r}"
            )
        if not self.production_base_url:
            return
        parsed = urlsplit(self.production_base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "production_base_url must be an HTTPS URL without credentials, "
                f"query, or fragment; got {self.production_base_url!r}"
            )
        if self.production_base_url.endswith("/"):
            raise ValueError("production_base_url must not end with '/'")


PROVIDER_ENDPOINT_DEFINITIONS: tuple[ProviderEndpointDefinition, ...] = (
    ProviderEndpointDefinition(
        "gmail_api",
        "https://gmail.googleapis.com/gmail/v1",
        "GMAIL_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "gmail_pubsub_api",
        "https://pubsub.googleapis.com/v1",
        "GMAIL_PUBSUB_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "google_directory",
        "https://admin.googleapis.com/admin/directory/v1",
        "GOOGLE_DIRECTORY_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "google_token",
        "https://oauth2.googleapis.com/token",
        "GOOGLE_TOKEN_URI",
    ),
    ProviderEndpointDefinition(
        "github_api",
        "https://api.github.com",
        "GITHUB_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "slack_api",
        "https://slack.com/api",
        "SLACK_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "discord_api",
        "https://discord.com/api/v10",
        "DISCORD_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "discord_gateway_bot",
        "https://discord.com/api/v10/gateway/bot",
        "DISCORD_GATEWAY_BOT_URL",
    ),
    ProviderEndpointDefinition(
        "notion_api",
        "https://api.notion.com",
        "NOTION_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "google_calendar_api",
        "https://www.googleapis.com/calendar/v3",
        "GOOGLE_CALENDAR_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "google_drive_api",
        "https://www.googleapis.com/drive/v3",
        "GOOGLE_DRIVE_API_BASE_URL",
    ),
    ProviderEndpointDefinition("jira_api", "", "JIRA_API_BASE_URL"),
    ProviderEndpointDefinition(
        "mercury_api",
        "https://api.mercury.com/api/v1",
        "MERCURY_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "quickbooks_api",
        "https://quickbooks.api.intuit.com",
        "QUICKBOOKS_API_BASE_URL",
    ),
    ProviderEndpointDefinition("grafana_api", "", "GRAFANA_API_BASE_URL"),
    ProviderEndpointDefinition(
        "brex_api",
        "https://platform.brexapis.com",
        "BREX_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "ramp_api",
        "https://api.ramp.com/developer/v1",
        "RAMP_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "gusto_api",
        "https://api.gusto.com",
        "GUSTO_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "deel_api",
        "https://api.letsdeel.com/rest/v2",
        "DEEL_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "fireflies_api",
        "https://api.fireflies.ai",
        "FIREFLIES_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "miro_api",
        "https://api.miro.com/v2",
        "MIRO_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "figma_api",
        "https://api.figma.com",
        "FIGMA_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "carta_api",
        "https://api.carta.com",
        "CARTA_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "hibob_api",
        "https://api.hibob.com",
        "HIBOB_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "ashby_api",
        "https://api.ashbyhq.com",
        "ASHBY_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "linkedin_api",
        "https://api.linkedin.com/rest",
        "LINKEDIN_API_BASE_URL",
    ),
    ProviderEndpointDefinition(
        "facebook_graph_api",
        "https://graph.facebook.com",
        "FACEBOOK_GRAPH_API_BASE_URL",
    ),
)


def _build_endpoint_catalog(
    definitions: tuple[ProviderEndpointDefinition, ...],
) -> Mapping[str, ProviderEndpointDefinition]:
    by_name: dict[str, ProviderEndpointDefinition] = {}
    env_owners: dict[str, str] = {}
    for definition in definitions:
        if definition.endpoint_name in by_name:
            raise ValueError(
                f"duplicate provider endpoint name {definition.endpoint_name!r}"
            )
        existing_env_owner = env_owners.get(definition.override_env)
        if existing_env_owner is not None:
            raise ValueError(
                f"provider endpoints {existing_env_owner!r} and "
                f"{definition.endpoint_name!r} share override env "
                f"{definition.override_env!r}"
            )
        by_name[definition.endpoint_name] = definition
        env_owners[definition.override_env] = definition.endpoint_name
    return MappingProxyType(by_name)


PROVIDER_ENDPOINT_CATALOG: Mapping[str, ProviderEndpointDefinition] = (
    _build_endpoint_catalog(PROVIDER_ENDPOINT_DEFINITIONS)
)


def provider_endpoint_definition(endpoint_name: str) -> ProviderEndpointDefinition:
    """Return one exact endpoint definition."""

    try:
        return PROVIDER_ENDPOINT_CATALOG[endpoint_name]
    except KeyError as exc:
        raise KeyError(f"unknown endpoint name: {endpoint_name!r}") from exc


__all__ = [
    "PROVIDER_ENDPOINT_CATALOG",
    "PROVIDER_ENDPOINT_DEFINITIONS",
    "ProviderEndpointDefinition",
    "provider_endpoint_definition",
]
