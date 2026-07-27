"""Central resolver for production outbound source-API base URLs.

Every outbound integration client resolves its base URL through
``endpoint(name)`` instead of a hardcoded module constant. Production defaults
are the real provider URLs, and each endpoint can be overridden through its
own explicit environment variable.

Resolution happens at client-construction time so tests can configure a
per-source endpoint before constructing the client. The loopback-only Provider
Lab uses ``lib.integrations.provider_lab``; its single origin is intentionally
not a fallback in this production resolver.

Auth/token endpoints that matter for outbound:
  - `google_token` — the DWD token-exchange URL. (Also data-driven via the
    service-account JSON's `token_uri`; this is the code-level default.)
  - github App-JWT / installation-token calls use `github_api` (same host).
"""

from __future__ import annotations

import os

from lib.integrations.endpoint_contract import (
    PROVIDER_ENDPOINT_CATALOG,
    provider_endpoint_definition,
)
from lib.shared.env import is_prod


def endpoint(name: str) -> str:
    """Resolve ``name`` from its explicit override or production default."""
    definition = provider_endpoint_definition(name)
    if os.environ.get("PROVIDER_LAB_URL") and is_prod():
        raise RuntimeError(
            "PROVIDER_LAB_URL is test-only and must be unset in production",
        )
    explicit = os.environ.get(definition.override_env)
    if explicit:
        return explicit.rstrip("/")
    return definition.production_base_url


def all_endpoints() -> dict[str, str]:
    """Snapshot of all resolved endpoints — for startup logging / diagnostics."""
    return {name: endpoint(name) for name in PROVIDER_ENDPOINT_CATALOG}


__all__ = ["all_endpoints", "endpoint"]
