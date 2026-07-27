"""Named endpoint defaults for admin/native-connect source routers."""

from __future__ import annotations

import importlib
from typing import Any

import pytest
from fastapi import HTTPException


_CASES = (
    pytest.param(
        "ashby",
        "_inputs",
        {"api_token": "token"},
        "ASHBY_API_BASE_URL",
        1,
        id="ashby",
    ),
    pytest.param(
        "ramp",
        "_require_creds",
        {"access_token": "token"},
        "RAMP_API_BASE_URL",
        "base_url",
        id="ramp",
    ),
    pytest.param(
        "deel",
        "_require_token",
        {"api_token": "token"},
        "DEEL_API_BASE_URL",
        1,
        id="deel",
    ),
    pytest.param(
        "hibob",
        "_inputs",
        {
            "company_id": "company-1",
            "service_user_id": "service-user-1",
            "service_user_token": "token",
        },
        "HIBOB_API_BASE_URL",
        3,
        id="hibob",
    ),
    pytest.param(
        "miro",
        "_require_token",
        {"api_token": "token"},
        "MIRO_API_BASE_URL",
        1,
        id="miro",
    ),
    pytest.param(
        "linkedin",
        "_require_creds",
        {
            "organization_urn": "urn:li:organization:1",
            "access_token": "token",
        },
        "LINKEDIN_API_BASE_URL",
        2,
        id="linkedin",
    ),
    pytest.param(
        "gusto",
        "_require_creds",
        {"company_uuid": "company-1", "access_token": "token"},
        "GUSTO_API_BASE_URL",
        2,
        id="gusto",
    ),
    pytest.param(
        "mercury",
        "_require_token",
        {"api_token": "token"},
        "MERCURY_API_BASE_URL",
        1,
        id="mercury",
    ),
    pytest.param(
        "brex",
        "_require_token",
        {"api_token": "token"},
        "BREX_API_BASE_URL",
        1,
        id="brex",
    ),
    pytest.param(
        "quickbooks",
        "_require_creds",
        {"realm_id": "realm-1", "access_token": "token"},
        "QUICKBOOKS_API_BASE_URL",
        2,
        id="quickbooks",
    ),
    pytest.param(
        "carta",
        "_require_creds",
        {"access_token": "token"},
        "CARTA_API_BASE_URL",
        1,
        id="carta",
    ),
    pytest.param(
        "figma",
        "_require_token",
        {"api_token": "token"},
        "FIGMA_API_BASE_URL",
        1,
        id="figma-pat",
    ),
    pytest.param(
        "fireflies",
        "_require_token",
        {"api_token": "token"},
        "FIREFLIES_API_BASE_URL",
        1,
        id="fireflies",
    ),
)


@pytest.fixture(autouse=True)
def _non_production_endpoint_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "FYRALIS_ENV",
        "COMPANY_OS_ENV",
        "APP_ENV",
        "ENVIRONMENT",
        "PROVIDER_LAB_URL",
    ):
        monkeypatch.delenv(name, raising=False)


def _resolved_base_url(
    source: str,
    parser_name: str,
    body: dict[str, object],
    selector: str | int,
) -> str:
    module = importlib.import_module(
        f"services.ingest.integrations.{source}.oauth",
    )
    parser = getattr(module, parser_name)
    parsed: Any = parser(dict(body))
    return parsed[selector]


@pytest.mark.parametrize(
    ("source", "parser_name", "body", "override_env", "selector"),
    _CASES,
)
def test_default_base_url_resolves_named_endpoint_at_call_time(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    parser_name: str,
    body: dict[str, object],
    override_env: str,
    selector: str | int,
) -> None:
    # Import before changing the environment: endpoint selection belongs to
    # request/client construction, not module import.
    importlib.import_module(f"services.ingest.integrations.{source}.oauth")
    expected = f"http://127.0.0.1:8787/{source}"
    monkeypatch.setenv(override_env, f"{expected}/")

    assert _resolved_base_url(source, parser_name, body, selector) == expected


@pytest.mark.parametrize(
    ("source", "parser_name", "body", "override_env", "selector"),
    _CASES,
)
def test_explicit_installation_base_url_wins_over_named_endpoint(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    parser_name: str,
    body: dict[str, object],
    override_env: str,
    selector: str | int,
) -> None:
    monkeypatch.setenv(override_env, f"http://127.0.0.1:8787/{source}")
    explicit = f"https://customer-endpoint.example/{source}"

    assert (
        _resolved_base_url(
            source,
            parser_name,
            {**body, "base_url": f"{explicit}/"},
            selector,
        )
        == explicit
    )


@pytest.mark.parametrize(
    ("source", "parser_name", "body", "override_env", "selector"),
    _CASES,
)
def test_production_request_cannot_redirect_fixed_provider_credentials(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    parser_name: str,
    body: dict[str, object],
    override_env: str,
    selector: str | int,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    monkeypatch.delenv(override_env, raising=False)

    with pytest.raises(HTTPException) as exc_info:
        _resolved_base_url(
            source,
            parser_name,
            {**body, "base_url": "https://credential-capture.example"},
            selector,
        )

    assert exc_info.value.status_code == 400
    assert "catalog-owned provider endpoint" in str(exc_info.value.detail)
