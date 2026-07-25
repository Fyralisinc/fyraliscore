"""Test-only URL contract for the loopback Provider Lab.

Production endpoint resolution deliberately does not consume
``PROVIDER_LAB_URL``. Callers that opt into the lab use this module to derive
provider-shaped URLs, then pass those URLs through an explicit client
constructor or per-source endpoint environment variable.
"""
from __future__ import annotations

import ipaddress
import os
from urllib.parse import urlsplit

from lib.shared.env import is_prod


PROVIDER_LAB_URL_ENV = "PROVIDER_LAB_URL"

# Provider Lab preserves the provider-shaped bases used by the real clients.
# Keys are endpoint-resolver names rather than source IDs, so multi-endpoint
# providers (Google and Discord) remain explicit.
_ENDPOINT_PATHS: dict[str, str] = {
    "gmail_api": "/gmail/gmail/v1",
    "google_directory": "/gmail/admin/directory/v1",
    "google_token": "/gmail/token",
    "github_api": "/github",
    "slack_api": "/slack/api",
    "discord_api": "/discord/api/v10",
    "discord_gateway_bot": "/discord/api/v10/gateway/bot",
    "notion_api": "/notion",
    "google_calendar_api": "/gcal/calendar/v3",
    "google_drive_api": "/gdrive/drive/v3",
    "jira_api": "/jira",
    "mercury_api": "/mercury",
    "quickbooks_api": "/quickbooks",
    "grafana_api": "/grafana",
    "brex_api": "/brex",
    "ramp_api": "/ramp",
    "gusto_api": "/gusto",
    "deel_api": "/deel",
    "fireflies_api": "/fireflies",
    "miro_api": "/miro",
    "figma_api": "/figma",
    "carta_api": "/carta",
    "hibob_api": "/hibob",
    "ashby_api": "/ashby",
    "linkedin_api": "/linkedin",
    "facebook_graph_api": "/facebook",
}

_ENDPOINT_ENV: dict[str, str] = {
    "gmail_api": "GMAIL_API_BASE_URL",
    "google_directory": "GOOGLE_DIRECTORY_BASE_URL",
    "google_token": "GOOGLE_TOKEN_URI",
    "github_api": "GITHUB_API_BASE_URL",
    "slack_api": "SLACK_API_BASE_URL",
    "discord_api": "DISCORD_API_BASE_URL",
    "discord_gateway_bot": "DISCORD_GATEWAY_BOT_URL",
    "notion_api": "NOTION_API_BASE_URL",
    "google_calendar_api": "GOOGLE_CALENDAR_API_BASE_URL",
    "google_drive_api": "GOOGLE_DRIVE_API_BASE_URL",
    "jira_api": "JIRA_API_BASE_URL",
    "mercury_api": "MERCURY_API_BASE_URL",
    "quickbooks_api": "QUICKBOOKS_API_BASE_URL",
    "grafana_api": "GRAFANA_API_BASE_URL",
    "brex_api": "BREX_API_BASE_URL",
    "ramp_api": "RAMP_API_BASE_URL",
    "gusto_api": "GUSTO_API_BASE_URL",
    "deel_api": "DEEL_API_BASE_URL",
    "fireflies_api": "FIREFLIES_API_BASE_URL",
    "miro_api": "MIRO_API_BASE_URL",
    "figma_api": "FIGMA_API_BASE_URL",
    "carta_api": "CARTA_API_BASE_URL",
    "hibob_api": "HIBOB_API_BASE_URL",
    "ashby_api": "ASHBY_API_BASE_URL",
    "linkedin_api": "LINKEDIN_API_BASE_URL",
    "facebook_graph_api": "FACEBOOK_GRAPH_API_BASE_URL",
}


def _validated_root(value: str) -> str:
    root = value.strip().rstrip("/")
    parsed = urlsplit(root)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise RuntimeError(
            f"{PROVIDER_LAB_URL_ENV} must be an HTTP(S) origin without "
            "credentials, a path, query, or fragment",
        )
    try:
        loopback = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = parsed.hostname.lower() == "localhost"
    if not loopback:
        raise RuntimeError(f"{PROVIDER_LAB_URL_ENV} must target a loopback host")
    return root


def provider_lab_root_url() -> str | None:
    """Return the configured loopback lab origin, rejecting production use."""
    value = os.environ.get(PROVIDER_LAB_URL_ENV)
    if not value:
        return None
    if is_prod():
        raise RuntimeError(
            f"{PROVIDER_LAB_URL_ENV} is test-only and must be unset in production",
        )
    return _validated_root(value)


def provider_lab_enabled() -> bool:
    """Whether this non-production process explicitly opted into Provider Lab."""
    return provider_lab_root_url() is not None


def provider_lab_endpoint_url(endpoint_name: str) -> str:
    """Return one exact provider-shaped endpoint base below the lab origin."""
    try:
        path = _ENDPOINT_PATHS[endpoint_name]
    except KeyError as exc:
        raise KeyError(f"unknown Provider Lab endpoint: {endpoint_name!r}") from exc
    root = provider_lab_root_url()
    if root is None:
        raise RuntimeError(f"{PROVIDER_LAB_URL_ENV} is unset")
    return root + path


def provider_lab_endpoint_overrides(root_url: str | None = None) -> dict[str, str]:
    """Return explicit per-endpoint environment overrides for subprocesses."""
    if root_url is None:
        root = provider_lab_root_url()
        if root is None:
            raise RuntimeError(f"{PROVIDER_LAB_URL_ENV} is unset")
    else:
        if is_prod():
            raise RuntimeError(
                f"{PROVIDER_LAB_URL_ENV} is test-only and must be unset in production",
            )
        root = _validated_root(root_url)
    return {
        _ENDPOINT_ENV[name]: root + path
        for name, path in _ENDPOINT_PATHS.items()
    }


__all__ = [
    "PROVIDER_LAB_URL_ENV",
    "provider_lab_enabled",
    "provider_lab_endpoint_overrides",
    "provider_lab_endpoint_url",
    "provider_lab_root_url",
]
