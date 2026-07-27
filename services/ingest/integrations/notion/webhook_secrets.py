"""Notion integration-scoped webhook verification-token loading.

A Notion webhook subscription belongs to one integration, so its verification
token is deployment-scoped.  Installation ``secret_ref`` values hold outbound
workspace bot tokens and must never be used to verify inbound webhooks.  The
canonical webhook ingress contract binds this loader directly.
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
    """Load current and previous Notion tokens during rotation overlap."""

    del tenant_id, installation_row_id, app_state
    if provider != "notion":
        raise ValueError("Notion webhook secret loader requires provider='notion'")

    current = load_app_secret_text_from_env(
        "NOTION_WEBHOOK_VERIFICATION_TOKEN"
    ).strip()
    previous = load_app_secret_text_from_env(
        "NOTION_WEBHOOK_VERIFICATION_TOKEN_PREV"
    ).strip()
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
