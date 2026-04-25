"""Tests for ``CompanyOSBaseline`` client selection and mock fallback."""

from __future__ import annotations

import os

import pytest

from lsob_baselines.company_os import (
    CompanyOSBaseline,
    MockCompanyOSClient,
)


def test_default_client_is_mock() -> None:
    b = CompanyOSBaseline()
    assert isinstance(b._client, MockCompanyOSClient)


def test_explicit_mock_client_string() -> None:
    b = CompanyOSBaseline(client="mock")
    assert isinstance(b._client, MockCompanyOSClient)


def test_unknown_client_string_raises() -> None:
    with pytest.raises(ValueError):
        CompanyOSBaseline(client="totally-made-up")


def test_env_var_selects_mock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LSOB_COMPANY_OS_CLIENT", "mock")
    b = CompanyOSBaseline()
    assert isinstance(b._client, MockCompanyOSClient)


def test_registry_factory_honours_params(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure env var does not leak in.
    monkeypatch.delenv("LSOB_COMPANY_OS_CLIENT", raising=False)
    from lsob_contracts import SUTConfig

    from lsob_baselines import REGISTRY

    sut = REGISTRY.construct(
        "company-os",
        SUTConfig(sut_name="company-os", tenant_id="t", params={"client": "mock"}),
    )
    # Baseline adapter exposes ``_client`` privately for introspection.
    assert isinstance(sut._client, MockCompanyOSClient)  # type: ignore[attr-defined]


def test_local_client_import_gating_without_parent() -> None:
    """``LocalCompanyOSClient`` must fail loudly when the parent cannot be imported.

    We don't assert the success path here because availability depends on
    whether the parent ``company-os`` package is installed in the active
    environment. Instead we verify the error path is plumbed correctly:
    if the parent *is* importable, construction succeeds; if not, it
    raises ``CompanyOSUnavailableError``.
    """
    from lsob_baselines.company_os import (
        CompanyOSUnavailableError,
        LocalCompanyOSClient,
        _parent_is_importable,
    )

    if _parent_is_importable():
        client = LocalCompanyOSClient()
        assert client is not None
    else:
        with pytest.raises(CompanyOSUnavailableError):
            LocalCompanyOSClient()
