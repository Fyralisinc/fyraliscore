"""services/webhooks/tenant_resolution.py — provider-specific tenant lookup.

Verified webhooks MUST resolve to a tenant before ingestion, because
the existing ingestion pipeline writes `tenant_id` on every row and
downstream RLS depends on the value being correct (Constitution
Principle III).

The Bearer path's `DEFAULT_TENANT_ID` fallback is NOT acceptable here
— a webhook that cannot be tied to a tenant MUST be rejected with the
`tenant_not_resolved` reason rather than silently land under a fallback.

The MVP resolver reads env-var mappings keyed by provider-specific
identifiers:

    WEBHOOK_TENANT_SLACK_<TEAM_ID>=<tenant_uuid>
    WEBHOOK_TENANT_GITHUB_<INSTALLATION_ID>=<tenant_uuid>
    WEBHOOK_TENANT_LINEAR_<ORG_ID>=<tenant_uuid>
    WEBHOOK_TENANT_STRIPE_<ACCOUNT>=<tenant_uuid>
    WEBHOOK_TENANT_DISCORD_<APPLICATION_ID>=<tenant_uuid>

A single-tenant deployment can set the catch-all:

    WEBHOOK_TENANT_DEFAULT=<tenant_uuid>

The catch-all is consulted ONLY when the provider-specific key is
absent AND the deployment has explicitly opted in via
`WEBHOOK_TENANT_DEFAULT_ALLOW=1`. This keeps the default-deny posture
of FR-014 — the operator must take an explicit action to enable a
fallback.

Returns None when no tenant can be resolved. The router treats None
as `tenant_not_resolved` and returns 401.
"""
from __future__ import annotations

import json
import os
from typing import Any, Callable
from uuid import UUID


def _env_uuid(key: str) -> UUID | None:
    raw = os.environ.get(key)
    if not raw:
        return None
    try:
        return UUID(raw)
    except (ValueError, TypeError):
        return None


def _default_tenant() -> UUID | None:
    """Catch-all tenant for single-tenant deployments, gated by env."""
    if os.environ.get("WEBHOOK_TENANT_DEFAULT_ALLOW", "") != "1":
        return None
    return _env_uuid("WEBHOOK_TENANT_DEFAULT")


def _payload_dict(body: bytes) -> dict[str, Any]:
    """Best-effort JSON decode. Returns {} when the body is not JSON
    (Stripe and Discord both send JSON; Slack does too)."""
    try:
        v = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {}
    return v if isinstance(v, dict) else {}


# ---------------------------------------------------------------------
# Per-provider resolvers
# ---------------------------------------------------------------------


def resolve_slack(body: bytes, hint: dict[str, Any]) -> UUID | None:
    team_id = hint.get("team_id") or _payload_dict(body).get("team_id")
    if not team_id:
        # Slack Events API wraps `team_id` at the top level; some
        # interactions put it under `team.id`. Check the nested form.
        d = _payload_dict(body)
        team = d.get("team")
        if isinstance(team, dict):
            team_id = team.get("id")
    if team_id:
        v = _env_uuid(f"WEBHOOK_TENANT_SLACK_{str(team_id).upper()}")
        if v is not None:
            return v
    return _default_tenant()


def resolve_github(body: bytes, hint: dict[str, Any]) -> UUID | None:
    d = _payload_dict(body)
    inst = d.get("installation") if isinstance(d, dict) else None
    inst_id = None
    if isinstance(inst, dict):
        inst_id = inst.get("id")
    if inst_id is not None:
        v = _env_uuid(f"WEBHOOK_TENANT_GITHUB_{inst_id}")
        if v is not None:
            return v
    return _default_tenant()


def resolve_linear(body: bytes, hint: dict[str, Any]) -> UUID | None:
    d = _payload_dict(body)
    org_id = d.get("organizationId") or d.get("organization_id")
    if not org_id:
        org = d.get("organization")
        if isinstance(org, dict):
            org_id = org.get("id")
    if org_id:
        v = _env_uuid(f"WEBHOOK_TENANT_LINEAR_{str(org_id).upper()}")
        if v is not None:
            return v
    return _default_tenant()


def resolve_stripe(body: bytes, hint: dict[str, Any]) -> UUID | None:
    d = _payload_dict(body)
    # Stripe Connect events name the connected account at the top
    # level (`account`); non-Connect events do not.
    account = d.get("account")
    if account:
        v = _env_uuid(f"WEBHOOK_TENANT_STRIPE_{str(account).upper()}")
        if v is not None:
            return v
    return _default_tenant()


def resolve_discord(body: bytes, hint: dict[str, Any]) -> UUID | None:
    d = _payload_dict(body)
    app_id = d.get("application_id") or d.get("guild_id")
    if app_id:
        v = _env_uuid(f"WEBHOOK_TENANT_DISCORD_{str(app_id).upper()}")
        if v is not None:
            return v
    return _default_tenant()


RESOLVERS: dict[str, Callable[[bytes, dict[str, Any]], UUID | None]] = {
    "slack": resolve_slack,
    "github": resolve_github,
    "linear": resolve_linear,
    "stripe": resolve_stripe,
    "discord": resolve_discord,
}


def resolve_tenant(
    provider: str,
    body: bytes,
    hint: dict[str, Any] | None = None,
) -> UUID | None:
    """Top-level entry point. Dispatches to the per-provider resolver
    and returns the resolved tenant or None.
    """
    resolver = RESOLVERS.get(provider)
    if resolver is None:
        return None
    return resolver(body, hint or {})


__all__ = [
    "RESOLVERS",
    "resolve_tenant",
]
