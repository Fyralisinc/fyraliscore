"""Exact webhook-installation secret resolution.

IN-08 cutover: the canonical source of truth for webhook signing
secrets is now `provider_installations.secret_ref` resolved via the
envelope-encrypted `lib.shared.secrets` store. The legacy env-var path
is retained as a development-only fallback, gated by
`WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`.

Public surface
--------------
* `load_secrets(provider, tenant_id, *, installation_row_id, app_state=None)`
  — async. Resolves the active signing secret for the exact installation
  selected by tenant resolution. Returns a list of `Secret` records compatible
  with the
  verifier Protocol. Empty list ⇒ caller emits the same
  `secret_not_configured` shape as before this feature shipped.

* `assert_prod_safety_invariants()` — startup helper. Raises
  `RuntimeError` when `FYRALIS_ENV=prod` and
  `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1` are both set, so production
  cannot accidentally fall back to plaintext env-var secrets.

Resolution order
----------------
When `app_state` is provided and `tenant_id` is non-None:
  1. Query `provider_installations` for the exact enabled row keyed by
     `(id, provider, tenant_id)`. The resolver has already selected that row
     from the provider-native installation identifier.
  2. If `secret_ref` is populated, decrypt via
     `app_state.integration_runtime.secret_store.get(ref, tenant_id=...)`
     and return. Legacy `app_state.secret_store` remains supported as a
     compatibility alias.
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
from lib.shared.secrets import load_app_secret_text_from_env
from services.app.webhooks.verifier import Secret


# ---------------------------------------------------------------------
# Startup safety
# ---------------------------------------------------------------------

def assert_prod_safety_invariants() -> None:
    """Fail fast at gateway startup if a production environment has the
    env-var fallback flag enabled. Called once during
    `services.app.gateway.main::build_app` before any request is served.

    Reasoning: the env-var path stores tenant signing secrets in
    plaintext in process environment, which is unacceptable for
    multi-tenant prod (SC-002). A deployment-time misconfiguration
    that left the flag on would silently downgrade security; failing
    startup is the loud, observable response.
    """
    from lib.shared.env import is_prod

    fallback = _env_fallback_allowed()
    if is_prod() and fallback:
        raise RuntimeError(
            "WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1 is set in a production "
            "environment (FYRALIS_ENV/COMPANY_OS_ENV=prod). The env-var "
            "fallback for "
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


def _app_state_attr(app_state: Any, name: str) -> Any | None:
    integration_runtime = getattr(app_state, "integration_runtime", None)
    if integration_runtime is not None:
        value = getattr(integration_runtime, name, None)
        if value is not None:
            return value
    return getattr(app_state, name, None)


# ---------------------------------------------------------------------
# DB-backed path (IN-08)
# ---------------------------------------------------------------------

async def _load_from_db(
    provider: str,
    tenant_id: UUID,
    installation_row_id: UUID,
    app_state: Any,
) -> list[Secret]:
    """Resolve the secret for one exact enabled installation.

    The row UUID is carried forward from tenant resolution.  Falling back to
    every row for a tenant/provider would allow one installation's delivery to
    verify with another installation's secret.
    """
    pool = _app_state_attr(app_state, "pool")
    secret_store = _app_state_attr(app_state, "secret_store")
    if pool is None or secret_store is None:
        return []

    row = await pool.fetchrow(
        """
        SELECT secret_ref
          FROM provider_installations
         WHERE id = $1
           AND provider = $2
           AND tenant_id = $3
           AND enabled = TRUE
           AND secret_ref IS NOT NULL
        """,
        installation_row_id,
        provider,
        tenant_id,
    )
    if row is None:
        return []

    ref = row["secret_ref"]
    try:
        plaintext = await secret_store.get(ref, tenant_id=tenant_id)
    except (SecretNotFoundError, SecretStoreError):
        return []
    return [
        Secret(
            provider=provider,
            value=(
                plaintext.decode("utf-8")
                if isinstance(plaintext, (bytes, bytearray))
                else str(plaintext)
            ),
            tenant_id=str(tenant_id),
            label=f"installation:{installation_row_id}:{ref}",
        )
    ]


# ---------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------

async def load_secrets(
    provider: str,
    tenant_id: UUID | None = None,
    *,
    installation_row_id: UUID | None = None,
    app_state: Any | None = None,
) -> Sequence[Secret]:
    """Return signing secrets for one resolved installation.

    Resolution order (see module docstring):
      1. GitHub special-case (IN-13): App-level secret, never
         per-tenant. Reads from `WEBHOOK_SECRET_GITHUB` env var
         (operator-supplied; matches the App's developer-settings
         webhook secret). Optional rotation overlap via
         `WEBHOOK_SECRET_GITHUB_PREV`. The env-var path is allowed for
         GitHub in prod WITHOUT the `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW`
         flag because the secret is App-level (single value across the
         whole deployment), not tenant-scoped — see Clarifications Q1.
      2. Exact DB ref via secret store (when `app_state`, `tenant_id`, and
         `installation_row_id` are provided).
      3. Env-var fallback (when DB lookup yielded nothing AND the
         fallback flag is on, OR when `app_state` is absent).

    Returns an empty sequence when no secret is configured — the
    verifier raises `secret_not_configured` in that case so the
    operator sees a distinct dashboard signal vs. signature mismatch.
    """
    if provider == "github":
        return _load_github_app_secrets()

    if provider == "notion":
        return _load_notion_app_secrets()

    if (
        app_state is not None
        and tenant_id is not None
        and installation_row_id is not None
    ):
        db_secrets = await _load_from_db(
            provider,
            tenant_id,
            installation_row_id,
            app_state,
        )
        if db_secrets:
            return db_secrets
        # Fall through to env path only when explicitly allowed.
        if not _env_fallback_allowed():
            return []
    elif app_state is not None and tenant_id is not None:
        # A runtime caller that resolved a tenant but lost the installation
        # identity must never broaden the lookup to sibling installations.
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
    current = load_app_secret_text_from_env("WEBHOOK_SECRET_GITHUB").strip()
    previous = load_app_secret_text_from_env("WEBHOOK_SECRET_GITHUB_PREV").strip()
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


def _load_notion_app_secrets() -> list[Secret]:
    """IN-14: load the Notion webhook verification token + optional
    previous token (rotation overlap window).

    Notion subscriptions are App-level (one subscription per integration;
    every workspace's events arrive on the one endpoint signed with the
    one `verification_token`), so the token is a single deployment-wide
    value — same shape as the GitHub App webhook secret. The
    `provider_installations.secret_ref` column is NOT used here; it holds
    the per-workspace bot token (outbound API), a different secret.

    Env vars:
      - NOTION_WEBHOOK_VERIFICATION_TOKEN       — current token (required)
      - NOTION_WEBHOOK_VERIFICATION_TOKEN_PREV  — previous (optional, rotation)

    The token is delivered once by Notion's verification POST (logged by
    services/ingest/integrations/notion/webhook.py for the operator to copy here).
    """
    current = load_app_secret_text_from_env(
        "NOTION_WEBHOOK_VERIFICATION_TOKEN",
    ).strip()
    previous = load_app_secret_text_from_env(
        "NOTION_WEBHOOK_VERIFICATION_TOKEN_PREV",
    ).strip()
    out: list[Secret] = []
    if current:
        out.append(
            Secret(
                provider="notion", value=current,
                tenant_id=None, label="app:current",
            )
        )
    if previous:
        out.append(
            Secret(
                provider="notion", value=previous,
                tenant_id=None, label="app:previous",
            )
        )
    return out


__all__ = [
    "Secret",
    "load_secrets",
    "assert_prod_safety_invariants",
]
