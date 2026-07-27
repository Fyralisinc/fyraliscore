"""GitHub App-scoped webhook-secret loading.

GitHub signs every installation delivery for one App with the App's webhook
secret.  The secret is therefore deployment-scoped rather than stored on a
tenant installation row.  The canonical webhook ingress contract binds this
loader directly and the shared router invokes it without provider branching.
"""

from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID

from lib.integrations.webhook_verifier import Secret
from lib.shared.secrets import load_app_secret_text_from_env


async def load_app_webhook_secrets(
    provider: str,
    tenant_id: UUID | None = None,
    *,
    installation_row_id: UUID | None = None,
    app_state: Any | None = None,
) -> Sequence[Secret]:
    """Load current and previous GitHub App secrets during rotation overlap."""

    del tenant_id, installation_row_id, app_state
    if provider != "github":
        raise ValueError("GitHub webhook secret loader requires provider='github'")

    current = load_app_secret_text_from_env("WEBHOOK_SECRET_GITHUB").strip()
    previous = load_app_secret_text_from_env("WEBHOOK_SECRET_GITHUB_PREV").strip()
    secrets: list[Secret] = []
    if current:
        secrets.append(
            Secret(
                provider=provider,
                value=current,
                tenant_id=None,
                label="app:current",
            )
        )
    if previous:
        secrets.append(
            Secret(
                provider=provider,
                value=previous,
                tenant_id=None,
                label="app:previous",
            )
        )
    return secrets


__all__ = ["load_app_webhook_secrets"]
