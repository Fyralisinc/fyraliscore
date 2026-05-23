"""Unit tests for lib.shared.env canonical prod detection."""
from __future__ import annotations

import pytest

from lib.shared.env import env_name, is_prod


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("FYRALIS_ENV", raising=False)
    monkeypatch.delenv("COMPANY_OS_ENV", raising=False)


def test_unset_is_not_prod() -> None:
    assert is_prod() is False
    assert env_name() == "dev"
    assert env_name(default="local") == "local"


@pytest.mark.parametrize("var", ["FYRALIS_ENV", "COMPANY_OS_ENV"])
@pytest.mark.parametrize("val", ["prod", "production", "PROD", " Prod "])
def test_either_var_marks_prod(
    monkeypatch: pytest.MonkeyPatch, var: str, val: str
) -> None:
    monkeypatch.setenv(var, val)
    assert is_prod() is True
    assert env_name() == "prod"


def test_company_os_env_prod_with_fyralis_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact shipped-compose footgun: only COMPANY_OS_ENV=prod set."""
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")
    assert is_prod() is True


def test_non_prod_label_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COMPANY_OS_ENV", "staging")
    assert is_prod() is False
    assert env_name() == "staging"


def test_master_kek_fail_fast_in_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    """build_secret_store must refuse to boot with no MASTER_KEK in prod,
    via either env var — not silently mint an ephemeral key."""
    from lib.shared.errors import SecretStoreError
    from lib.shared.secrets import build_secret_store

    monkeypatch.delenv("MASTER_KEK", raising=False)
    monkeypatch.setenv("COMPANY_OS_ENV", "prod")
    with pytest.raises(SecretStoreError, match="MASTER_KEK"):
        build_secret_store(pool=None)  # type: ignore[arg-type]
