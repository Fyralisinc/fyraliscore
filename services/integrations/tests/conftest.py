"""services.integrations.tests.conftest — shared fixtures for IN-08 tests.

The parent project conftest provides `db_pool` / `fresh_db`; this file
adds environment-variable setup for the OAuth flow tests so that
`build_app()` and `build_secret_store()` don't fire the dev-mode
warning under the project's `filterwarnings = error` policy.
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _stable_master_kek(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stable test Fernet key + env-fallback flag so `build_secret_store`
    constructs a deterministic envelope and `assert_prod_safety_invariants`
    is happy. Individual tests override `MASTER_KEK` when they need
    distinct keys.
    """
    monkeypatch.setenv(
        "MASTER_KEK", "KuT6Cixjs4991zhixcpj1QAFbiQj3b9N8meZV2AJJyw=",
    )
    monkeypatch.setenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", "1")
