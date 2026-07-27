"""Security contract for credential-bearing native-connect provider URLs."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException

from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.grafana.oauth import _inputs as grafana_inputs
from services.ingest.integrations.jira.oauth import (
    _require_credentials as jira_credentials,
)


_ENVIRONMENT_KEYS = (
    "FYRALIS_ENV",
    "COMPANY_OS_ENV",
    "APP_ENV",
    "ENVIRONMENT",
    "PROVIDER_LAB_URL",
    "BREX_API_BASE_URL",
    "GRAFANA_API_BASE_URL",
    "JIRA_API_BASE_URL",
)
_INTEGRATIONS_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENVIRONMENT_KEYS:
        monkeypatch.delenv(name, raising=False)


def _detail(exc_info: pytest.ExceptionInfo[HTTPException]) -> str:
    assert exc_info.value.status_code == 400
    return str(exc_info.value.detail)


def test_every_request_base_url_connect_router_uses_the_common_policy() -> None:
    """Prevent a future source router from restoring an ad-hoc URL check."""

    matched: list[str] = []
    for path in sorted(_INTEGRATIONS_ROOT.glob("*/oauth.py")):
        source = path.read_text(encoding="utf-8")
        if 'body.get("base_url")' not in source and 'body.get("instance_url")' not in source:
            continue
        matched.append(path.parent.name)
        assert (
            "from services.ingest.integrations.base_url_policy import "
            "native_connect_base_url"
        ) in source
        assert "native_connect_base_url(" in source
        assert 'startswith(("https://", "http://"))' not in source

    assert matched


@pytest.mark.parametrize(
    "candidate",
    (
        "https://user:password@api.brex.example",
        "https://api.brex.example/path?token=leak",
        "https://api.brex.example/path#fragment",
        "https://api.brex.example/a/../metadata",
        "https://api.brex.example\\@credential-capture.example",
    ),
)
def test_ambiguous_or_secret_bearing_urls_are_rejected(candidate: str) -> None:
    with pytest.raises(HTTPException):
        native_connect_base_url(candidate, endpoint_name="brex_api")


def test_non_loopback_http_is_rejected_even_when_env_overridden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = "http://provider-lab.example/brex"
    monkeypatch.setenv("BREX_API_BASE_URL", candidate)

    with pytest.raises(HTTPException) as exc_info:
        native_connect_base_url(None, endpoint_name="brex_api")

    assert "must use HTTPS" in _detail(exc_info)


def test_loopback_http_requires_explicit_lab_test_or_endpoint_configuration() -> None:
    with pytest.raises(HTTPException) as exc_info:
        native_connect_base_url(
            "http://127.0.0.1:8787/brex",
            endpoint_name="brex_api",
        )

    assert "explicit non-production" in _detail(exc_info)


def test_exact_loopback_endpoint_override_is_allowed_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = "http://127.0.0.1:8787/brex"
    monkeypatch.setenv("BREX_API_BASE_URL", candidate)

    assert native_connect_base_url(None, endpoint_name="brex_api") == candidate


def test_provider_lab_origin_authorizes_its_loopback_subpaths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787")
    candidate = "http://127.0.0.1:8787/brex"

    assert (
        native_connect_base_url(candidate, endpoint_name="brex_api")
        == candidate
    )


def test_explicit_test_environment_authorizes_loopback_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "test")
    assert (
        native_connect_base_url(
            "http://localhost:8787/brex",
            endpoint_name="brex_api",
        )
        == "http://localhost:8787/brex"
    )
    with pytest.raises(HTTPException):
        native_connect_base_url(
            "http://provider-lab.example/brex",
            endpoint_name="brex_api",
        )


def test_production_rejects_http_even_when_operator_overridden(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    monkeypatch.setenv(
        "BREX_API_BASE_URL",
        "http://127.0.0.1:8787/brex",
    )

    with pytest.raises(HTTPException) as exc_info:
        native_connect_base_url(None, endpoint_name="brex_api")

    assert "must use HTTPS" in _detail(exc_info)


def test_production_fixed_host_accepts_exact_https_operator_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = "https://approved-provider-proxy.example/brex"
    monkeypatch.setenv("FYRALIS_ENV", "production")
    monkeypatch.setenv("BREX_API_BASE_URL", candidate)

    assert native_connect_base_url(None, endpoint_name="brex_api") == candidate


def test_jira_production_host_is_limited_to_atlassian_cloud(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    valid = jira_credentials(
        {
            "base_url": "https://acme.atlassian.net",
            "account_email": "admin@example.com",
            "api_token": "secret",
        },
    )
    assert valid[0] == "https://acme.atlassian.net"

    with pytest.raises(HTTPException) as exc_info:
        jira_credentials(
            {
                "base_url": "https://credential-capture.example",
                "account_email": "admin@example.com",
                "api_token": "secret",
            },
        )
    assert "allowed production domains" in _detail(exc_info)


@pytest.mark.parametrize(
    "candidate",
    (
        "https://127.0.0.1:3000",
        "https://10.0.0.8:3000",
        "https://grafana.internal",
    ),
)
def test_grafana_production_rejects_direct_private_targets(
    monkeypatch: pytest.MonkeyPatch,
    candidate: str,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")

    with pytest.raises(HTTPException) as exc_info:
        grafana_inputs(
            {
                "base_url": candidate,
                "service_account_token": "secret",
            },
        )

    assert "local/private host" in _detail(exc_info)


def test_grafana_private_target_requires_exact_operator_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = "https://grafana.internal"
    monkeypatch.setenv("FYRALIS_ENV", "production")
    monkeypatch.setenv("GRAFANA_API_BASE_URL", candidate)

    assert grafana_inputs(
        {
            "base_url": candidate,
            "service_account_token": "secret",
        },
    )[0] == candidate
