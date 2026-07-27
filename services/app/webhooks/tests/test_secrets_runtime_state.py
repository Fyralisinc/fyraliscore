from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest

from services.app.webhooks.secrets import load_installation_secrets


class _FakePool:
    async def fetchrow(self, sql, installation_row_id, provider, tenant_id):
        assert installation_row_id == UUID(
            "22222222-2222-2222-2222-222222222222"
        )
        return {"secret_ref": "secret/ref"}


class _FakeSecretStore:
    async def get(self, ref, *, tenant_id):
        assert ref == "secret/ref"
        return b"runtime-secret"


@pytest.mark.asyncio
async def test_installation_loader_reads_runtime_before_legacy_state_aliases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", raising=False)
    app_state = SimpleNamespace(
        pool=None,
        secret_store=None,
        integration_runtime=SimpleNamespace(
            pool=_FakePool(),
            secret_store=_FakeSecretStore(),
        ),
    )

    secrets = await load_installation_secrets(
        "slack",
        UUID("11111111-1111-1111-1111-111111111111"),
        installation_row_id=UUID(
            "22222222-2222-2222-2222-222222222222"
        ),
        app_state=app_state,
    )

    assert len(secrets) == 1
    assert secrets[0].provider == "slack"
    assert secrets[0].value == "runtime-secret"
    assert secrets[0].label == (
        "installation:22222222-2222-2222-2222-222222222222:secret/ref"
    )
