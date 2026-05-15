"""services/webhooks/secrets.py — per-(provider, tenant) secret resolution.

IN-08 cutover: the canonical source of truth for webhook signing
secrets is now `provider_installations.secret_ref` resolved via the
envelope-encrypted `lib.shared.secrets` store. The legacy env-var path
is retained as a development-only fallback, gated by
`WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`.

Public surface
--------------
* `load_secrets(provider, tenant_id, *, app_state=None)` — async.
  Resolves the active signing secret(s) for a given (provider, tenant)
  pair. Returns a list of `Secret` records compatible with the
  verifier Protocol. Empty list ⇒ caller emits the same
  `secret_not_configured` shape as before this feature shipped.

* `assert_prod_safety_invariants()` — startup helper. Raises
  `RuntimeError` when `FYRALIS_ENV=prod` and
  `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` are both set, so production
  cannot accidentally fall back to plaintext env-var secrets.

Resolution order
----------------
When `app_state` is provided and `tenant_id` is non-None:
  1. Query `provider_installations` for the enabled row keyed by
     `(provider, tenant_id)`. (The router has the tenant already by
     this point — IN-07 / IN-08 path.)
  2. If `secret_ref` is populated, decrypt via
     `app_state.secret_store.get(ref, tenant_id=...)` and return.
  3. Otherwise, if `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` is set, fall
     through to the legacy env-var path.
  4. Otherwise, return `[]`.

When `app_state` is omitted (legacy callers, tests, or the
`tenant_id=None` URL-verification handshake path), the env-var path is
the only mechanism. This keeps current callers working unchanged.

Env-var layout (legacy)
-----------------------
    WEBHOOK_SECRET_<PROVIDER>=<value>[,<value>,...]
    WEBHOOK_SECRET_<PROVIDER>__<TENANT_HEX>=<value>[,<value>,...]

Where `<PROVIDER>` is one of `SLACK`, `GITHUB`, `LINEAR`, `STRIPE`,
`DISCORD` and `<TENANT_HEX>` is a tenant UUID with dashes stripped and
uppercased. Per-tenant overrides take precedence; the global key is
the fallback used during dev/dogfood.

A secret value may be prefixed with `LABEL=` to tag it for rotation
observability.
"""
from __future__ import annotations

import os
from typing import Any, Sequence
from uuid import UUID

from lib.shared.errors import SecretNotFoundError, SecretStoreError
from services.webhooks.verifier import Secret


# ---------------------------------------------------------------------
# Startup safety
# ---------------------------------------------------------------------

def assert_prod_safety_invariants() -> None:
    """Fail fast at gateway startup if a production environment has the
    env-var fallback flag enabled. Called once during
    `services.gateway.main::build_app` before any request is served.

    Reasoning: the env-var path stores tenant signing secrets in
    plaintext in process environment, which is unacceptable for
    multi-tenant prod (SC-002). A deployment-time misconfiguration
    that left the flag on would silently downgrade security; failing
    startup is the loud, observable response.
    """
    env = os.environ.get("FYRALIS_ENV", "").lower()
    fallback = _env_fallback_allowed()
    if env == "prod" and fallback:
        raise RuntimeError(
            "WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1 is set in a production "
            "environment (FYRALIS_ENV=prod). The env-var fallback for "
            "webhook signing secrets is dev-only and must not be enabled "
            "in prod — refusing to start so tenant secrets are not "
            "silently sourced from process environment."
        )


def _env_fallback_allowed() -> bool:
    return os.environ.get("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "") == "1"


# ---------------------------------------------------------------------
# Env-var legacy path (unchanged from IN-06)
# ---------------------------------------------------------------------

def _env_value(provider: str, tenant_id: UUID | None) -> str | None:
    """Pull the raw env value for (provider, tenant), with the
    per-tenant key checked first."""
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


def _load_from_env(provider: str, tenant_id: UUID | None) -> list[Secret]:
    raw = _env_value(provider, tenant_id)
    if raw is None:
        return []
    return _parse_value(provider, raw, tenant_id)


# ---------------------------------------------------------------------
# DB-backed path (IN-08)
# ---------------------------------------------------------------------

async def _load_from_db(
    provider: str,
    tenant_id: UUID,
    app_state: Any,
) -> list[Secret]:
    """Read `provider_installations.secret_ref` for the enabled row
    matching `(provider, tenant_id)` and resolve it via the secret
    store. Returns [] when no enabled row or no secret_ref."""
    pool = getattr(app_state, "pool", None)
    secret_store = getattr(app_state, "secret_store", None)
    if pool is None or secret_store is None:
        return []

    row = await pool.fetchrow(
        """
        SELECT secret_ref
          FROM provider_installations
         WHERE provider = $1
           AND tenant_id = $2
           AND enabled = TRUE
           AND secret_ref IS NOT NULL
         ORDER BY installed_at DESC
         LIMIT 1
        """,
        provider,
        tenant_id,
    )
    if row is None:
        return []
    ref = row["secret_ref"]

    try:
        plaintext = await secret_store.get(ref, tenant_id=tenant_id)
    except SecretNotFoundError:
        # Dangling ref — installation row points at a deleted secret.
        # Treat as "no secret configured" so the verifier emits the
        # standard secret_not_configured / signature_mismatch error.
        return []
    except SecretStoreError:
        # Backend trouble. Surfacing as "no secret" is conservative —
        # the caller will return 401, which is safer than 500 for
        # transient backend hiccups.
        return []

    return [
        Secret(
            provider=provider,
            value=plaintext.decode("utf-8") if isinstance(plaintext, (bytes, bytearray)) else str(plaintext),
            tenant_id=str(tenant_id),
            label=f"installation:{ref}",
        )
    ]


# ---------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------

async def load_secrets(
    provider: str,
    tenant_id: UUID | None = None,
    *,
    app_state: Any | None = None,
) -> Sequence[Secret]:
    """Return the active signing secret(s) for `(provider, tenant_id)`.

    Resolution order (see module docstring):
      1. GitHub special-case (IN-13): App-level secret, never
         per-tenant. Reads from `WEBHOOK_SECRET_GITHUB` env var
         (operator-supplied; matches the App's developer-settings
         webhook secret). Optional rotation overlap via
         `WEBHOOK_SECRET_GITHUB_PREV`. The env-var path is allowed for
         GitHub in prod WITHOUT the `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW`
         flag because the secret is App-level (single value across the
         whole deployment), not tenant-scoped — see Clarifications Q1.
      2. DB ref via secret store (when `app_state` and `tenant_id`
         are provided; for slack / discord / linear / stripe).
      3. Env-var fallback (when DB lookup yielded nothing AND the
         fallback flag is on, OR when `app_state` is absent).

    Returns an empty sequence when no secret is configured — the
    verifier raises `secret_not_configured` in that case so the
    operator sees a distinct dashboard signal vs. signature mismatch.
    """
    if provider == "github":
        return _load_github_app_secrets()

    if app_state is not None and tenant_id is not None:
        db_secrets = await _load_from_db(provider, tenant_id, app_state)
        if db_secrets:
            return db_secrets
        # Fall through to env path only when explicitly allowed.
        if not _env_fallback_allowed():
            return []
    # Legacy / fallback path.
    return _load_from_env(provider, tenant_id)


def _load_github_app_secrets() -> list[Secret]:
    """IN-13: load the GitHub App-level webhook secret + optional
    previous secret (rotation overlap window).

    Env vars:
      - WEBHOOK_SECRET_GITHUB       — current App-level secret (required)
      - WEBHOOK_SECRET_GITHUB_PREV  — previous secret during rotation
                                      (optional)
    """
    current = os.environ.get("WEBHOOK_SECRET_GITHUB", "").strip()
    previous = os.environ.get("WEBHOOK_SECRET_GITHUB_PREV", "").strip()
    out: list[Secret] = []
    if current:
        out.append(
            Secret(
                provider="github",
                value=current,
                tenant_id=None,
                label="app:current",
            )
        )
    if previous:
        out.append(
            Secret(
                provider="github",
                value=previous,
                tenant_id=None,
                label="app:previous",
            )
        )
    return out


__all__ = [
    "Secret",
    "load_secrets",
    "assert_prod_safety_invariants",
]
