"""services/webhooks/secrets.py — per-(provider, tenant) secret resolution.

Plan R3 selected env-var-based storage for the MVP — no new migration,
no new table. Rotation overlap is supported by allowing multiple
comma-separated secrets in a single env var. Each entry may be tagged
with an optional `label` for observability so rotation tests can
confirm which secret matched.

Env var layout:

    WEBHOOK_SECRET_<PROVIDER>=<value>[,<value>,...]
    WEBHOOK_SECRET_<PROVIDER>__<TENANT_HEX>=<value>[,<value>,...]

Where `<PROVIDER>` is one of `SLACK`, `GITHUB`, `LINEAR`, `STRIPE`,
`DISCORD` and `<TENANT_HEX>` is a tenant UUID with dashes stripped and
uppercased. Per-tenant overrides take precedence; the global key is
the fallback used during dev/dogfood.

A secret value may be prefixed with `LABEL=` to tag it for rotation
observability:

    WEBHOOK_SECRET_SLACK=old=oldhex,new=newhex

In this case `old` and `new` are labels; both secrets are tried.

The resolver is intentionally stateless so a new process can pick up
a rotation without restart simply by being launched with the new env.
Long-running processes that need live rotation MUST be paired with a
secrets-manager front end that mutates the env at runtime, OR with a
follow-up that replaces this resolver with a DB-backed one. The
Protocol below is identical in either case.
"""
from __future__ import annotations

import os
from typing import Sequence
from uuid import UUID

from services.webhooks.verifier import Secret


def _env_value(provider: str, tenant_id: UUID | None) -> str | None:
    """Pull the raw env value for (provider, tenant), with the
    per-tenant key checked first.
    """
    up = provider.upper()
    if tenant_id is not None:
        per_tenant_key = f"WEBHOOK_SECRET_{up}__{tenant_id.hex.upper()}"
        v = os.environ.get(per_tenant_key)
        if v is not None:
            return v
    return os.environ.get(f"WEBHOOK_SECRET_{up}")


def _parse_value(provider: str, raw: str, tenant_id: UUID | None) -> list[Secret]:
    """Parse a possibly-multi-secret env value into Secret records.

    Each comma-separated entry is either `<value>` or `<label>=<value>`.
    Whitespace around commas is stripped. Empty entries are skipped.
    """
    out: list[Secret] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        label: str | None = None
        value = entry
        if "=" in entry:
            maybe_label, maybe_value = entry.split("=", 1)
            # Treat `label=value` only when the label side is short and
            # ASCII-identifier-ish; anything else is a value that
            # happens to contain `=` (some HMAC hex strings can, no —
            # hex won't, but Stripe-style headers can in other paths).
            if (
                maybe_label
                and len(maybe_label) <= 32
                and maybe_label.replace("_", "").replace("-", "").isalnum()
            ):
                label = maybe_label
                value = maybe_value
        out.append(
            Secret(
                provider=provider,
                value=value,
                tenant_id=str(tenant_id) if tenant_id is not None else None,
                label=label,
            )
        )
    return out


def load_secrets(provider: str, tenant_id: UUID | None = None) -> Sequence[Secret]:
    """Load every active secret for (provider, tenant) from env.

    Returns an empty sequence when no secret is configured — the
    Verifier will raise `secret_not_configured` in that case so the
    operator sees a distinct dashboard signal vs. signature mismatch.

    Per-tenant secrets override the global key; if a per-tenant key
    is present (even if its value is empty), the global fallback is
    NOT consulted. This makes "disable this tenant" expressible by
    setting the per-tenant key to an empty string.
    """
    raw = _env_value(provider, tenant_id)
    if raw is None:
        return []
    return _parse_value(provider, raw, tenant_id)


__all__ = [
    "Secret",
    "load_secrets",
]
