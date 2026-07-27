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

from lib.integrations.endpoint_contract import PROVIDER_ENDPOINT_CATALOG
from lib.shared.env import is_prod


PROVIDER_LAB_URL_ENV = "PROVIDER_LAB_URL"

# Provider Lab preserves the provider-shaped bases used by the real clients.
# Keys are endpoint-resolver names rather than source IDs, so multi-endpoint
# providers (Google and Discord) remain explicit.
_ENDPOINT_PATHS: dict[str, str] = {
    "gmail_api": "/gmail/gmail/v1",
    "gmail_pubsub_api": "/gmail/v1",
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
    "signal_jsonrpc": "/signal/jsonrpc",
    "signal_sse": "/signal/events/{subscription_id}",
    "slack_oauth_token": "/slack/api/oauth.v2.access",
    "discord_oauth_token": "/discord/api/v10/oauth2/token",
    "notion_oauth_token": "/notion/v1/oauth/token",
    "figma_oauth_token": "/figma/v1/oauth/token",
    "figma_oauth_refresh": "/figma/v1/oauth/token",
    "quickbooks_token": "/quickbooks/oauth2/v1/tokens/bearer",
    "ramp_token": "/ramp/token",
    "gusto_token": "/gusto/oauth/token",
    "carta_token": "/carta/o/access_token/",
    "linkedin_token": "/linkedin/oauth/v2/accessToken",
}

_LAB_ONLY_ENDPOINT_ENV: dict[str, str] = {
    "signal_jsonrpc": "SIGNAL_JSONRPC_ENDPOINT",
    "signal_sse": "SIGNAL_SSE_ENDPOINT",
    "slack_oauth_token": "SLACK_OAUTH_TOKEN_URL",
    "discord_oauth_token": "DISCORD_OAUTH_TOKEN_URL",
    "notion_oauth_token": "NOTION_OAUTH_TOKEN_URL",
    "figma_oauth_token": "FIGMA_OAUTH_TOKEN_URL",
    "figma_oauth_refresh": "FIGMA_OAUTH_REFRESH_URL",
    "quickbooks_token": "QUICKBOOKS_TOKEN_URL",
    "ramp_token": "RAMP_TOKEN_URL",
    "gusto_token": "GUSTO_TOKEN_URL",
    "carta_token": "CARTA_TOKEN_URL",
    "linkedin_token": "LINKEDIN_TOKEN_URL",
}
_ENDPOINT_ENV: dict[str, str] = {
    name: PROVIDER_ENDPOINT_CATALOG[name].override_env
    for name in _ENDPOINT_PATHS
    if name in PROVIDER_ENDPOINT_CATALOG
}
_ENDPOINT_ENV.update(_LAB_ONLY_ENDPOINT_ENV)


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
    return {_ENDPOINT_ENV[name]: root + path for name, path in _ENDPOINT_PATHS.items()}


__all__ = [
    "PROVIDER_LAB_URL_ENV",
    "provider_lab_enabled",
    "provider_lab_endpoint_overrides",
    "provider_lab_endpoint_url",
    "provider_lab_root_url",
]
