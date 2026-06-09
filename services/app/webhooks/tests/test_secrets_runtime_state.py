from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest

from services.app.webhooks.secrets import load_secrets


class _FakePool:
    async def fetch(self, sql, provider, tenant_id):
        # _load_from_db now fetches ALL active secret_refs (rotation overlap),
        # not a single LIMIT-1 row — return a one-row list.
        return [{"secret_ref": "secret/ref"}]


class _FakeSecretStore:
    async def get(self, ref, *, tenant_id):
        assert ref == "secret/ref"
        return b"runtime-secret"


@pytest.mark.asyncio
async def test_load_secrets_reads_integration_runtime_before_legacy_aliases(
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

    secrets = await load_secrets(
        "slack",
        UUID("11111111-1111-1111-1111-111111111111"),
        app_state=app_state,
    )

    assert len(secrets) == 1
    assert secrets[0].provider == "slack"
    assert secrets[0].value == "runtime-secret"
    assert secrets[0].label == "installation:secret/ref"
