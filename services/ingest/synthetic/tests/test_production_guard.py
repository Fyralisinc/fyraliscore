from __future__ import annotations

import importlib

import pytest


def test_synthetic_package_refuses_production_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPANY_OS_ENV", "test")
    synthetic = importlib.import_module("services.ingest.synthetic")

    monkeypatch.setenv("COMPANY_OS_ENV", "prod")

    with pytest.raises(RuntimeError, match="cannot run in production"):
        synthetic._check_env_guard()


def test_synthetic_package_allows_test_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("COMPANY_OS_ENV", "test")
    synthetic = importlib.import_module("services.ingest.synthetic")

    synthetic._check_env_guard()
