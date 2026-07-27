"""Contract-owned webhook signing-secret loader tests."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest

from services.ingest.source_contract import resolve_webhook_secret_loader


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("route_id", "current_name", "previous_name"),
    (
        ("github", "WEBHOOK_SECRET_GITHUB", "WEBHOOK_SECRET_GITHUB_PREV"),
        (
            "notion",
            "NOTION_WEBHOOK_VERIFICATION_TOKEN",
            "NOTION_WEBHOOK_VERIFICATION_TOKEN_PREV",
        ),
    ),
)
async def test_app_scoped_loaders_preserve_rotation_overlap(
    route_id: str,
    current_name: str,
    previous_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(current_name, f"{route_id}-current")
    monkeypatch.setenv(previous_name, f"{route_id}-previous")

    loader = resolve_webhook_secret_loader(route_id)
    secrets = await loader(
        route_id,
        UUID("11111111-1111-1111-1111-111111111111"),
        installation_row_id=UUID("22222222-2222-2222-2222-222222222222"),
        app_state=SimpleNamespace(
            pool=object(),
            secret_store=object(),
        ),
    )

    assert [(secret.label, secret.value) for secret in secrets] == [
        ("app:current", f"{route_id}-current"),
        ("app:previous", f"{route_id}-previous"),
    ]
    assert all(secret.provider == route_id for secret in secrets)
    assert all(secret.tenant_id is None for secret in secrets)


@pytest.mark.asyncio
@pytest.mark.parametrize("route_id", ("github", "notion"))
async def test_app_scoped_loader_rejects_miswired_route(route_id: str) -> None:
    loader = resolve_webhook_secret_loader(route_id)

    with pytest.raises(ValueError, match="requires provider"):
        await loader("slack")
