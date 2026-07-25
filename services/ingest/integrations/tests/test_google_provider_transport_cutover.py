"""Focused ProviderTransport coverage for Calendar/Drive."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    ProviderPermanentError,
    QuotaRequirement,
    RequestContext,
)
from services.ingest.integrations.gmail.client import GoogleHttpClient
from services.ingest.integrations.google_calendar.client import GoogleCalendarClient
from services.ingest.integrations.google_drive.client import GoogleDriveClient


pytestmark = pytest.mark.asyncio


class _Minter:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def mint(self, **kwargs: Any) -> str:
        self.calls.append(kwargs)
        return "token"

    def invalidate(self, **kwargs: Any) -> None:
        raise AssertionError("token invalidation was not expected")


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, request_context, policy, call):  # noqa: ANN001, ANN202
        self.contexts.append(request_context)
        return await call()


def _quota(
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions: dict[str, str],
) -> tuple[QuotaRequirement, ...]:
    return (
        QuotaRequirement(
            scope="installation",
            bucket_key=(
                f"{source}:{operation}:{tenant_id}:{installation_id}:"
                f"{dimensions['user']}"
            ),
            capacity=10,
            refill_per_second=10.0,
        ),
    )


async def test_calendar_watch_uses_exact_request_binding() -> None:
    recorder = _Recorder()
    minter = _Minter()
    raw = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"id": "channel", "resourceId": "resource"},
            )
        )
    )
    try:
        http = GoogleHttpClient(
            minter,  # type: ignore[arg-type]
            http_client=raw,
            source="google_calendar",
            tenant_id="tenant-1",
            installation_id="calendar-install-1",
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        calendar = GoogleCalendarClient(http, base_url="https://calendar.test/v3")
        await calendar.watch_events(
            calendar_id="alice@example.com",
            user_email="alice@example.com",
            channel_id="channel",
            address="https://fyralis.test/google-calendar",
            token="secret",
        )
    finally:
        await raw.aclose()

    context = recorder.contexts[-1]
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == (
        "google_calendar",
        "events.watch",
        "tenant-1",
        "calendar-install-1",
    )
    assert minter.calls[-1]["source"] == "google_calendar"
    assert minter.calls[-1]["tenant_id"] == "tenant-1"
    assert minter.calls[-1]["installation_id"] == "calendar-install-1"


async def test_drive_child_hydration_uses_exact_request_binding() -> None:
    recorder = _Recorder()
    minter = _Minter()
    raw = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"document body")
        )
    )
    try:
        http = GoogleHttpClient(
            minter,  # type: ignore[arg-type]
            http_client=raw,
            source="google_drive",
            tenant_id="tenant-2",
            installation_id="drive-install-2",
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        drive = GoogleDriveClient(http, base_url="https://drive.test/v3")
        await drive.export_text(
            user_email="bob@example.com",
            file_id="file-1",
            mime_type="application/vnd.google-apps.document",
            max_bytes=1024,
        )
    finally:
        await raw.aclose()

    context = recorder.contexts[-1]
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == (
        "google_drive",
        "files.export",
        "tenant-2",
        "drive-install-2",
    )


async def test_production_request_missing_installation_fails_before_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    sent = False

    def _unexpected_request(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent = True
        return httpx.Response(200, json={"files": []})

    raw = httpx.AsyncClient(transport=httpx.MockTransport(_unexpected_request))
    recorder = _Recorder()
    try:
        http = GoogleHttpClient(
            _Minter(),  # type: ignore[arg-type]
            http_client=raw,
            source="google_drive",
            tenant_id="tenant-3",
            installation_id=None,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        drive = GoogleDriveClient(http, base_url="https://drive.test/v3")
        with pytest.raises(
            ProviderPermanentError,
            match="missing exact tenant/installation binding",
        ):
            await drive.list_files(user_email="carol@example.com")
    finally:
        await raw.aclose()

    assert sent is False
    assert recorder.contexts == []


@pytest.mark.parametrize(
    ("module_name", "source", "scope"),
    [
        (
            "services.ingest.integrations.google_calendar.oauth",
            "google_calendar",
            "calendar.readonly",
        ),
        (
            "services.ingest.integrations.google_drive.oauth",
            "google_drive",
            "drive.readonly",
        ),
    ],
)
async def test_preflight_is_tenant_bound_before_installation_exists(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    source: str,
    scope: str,
) -> None:
    import importlib

    oauth = importlib.import_module(module_name)
    tenant_id = uuid4()
    captured: dict[str, Any] = {}

    class _HttpContext:
        async def __aenter__(self):
            return object()

        async def __aexit__(self, *exc: Any) -> None:
            return None

    def _build(minter: Any, **kwargs: Any) -> _HttpContext:
        captured.update(kwargs)
        return _HttpContext()

    async def _enumerate(directory: Any, *, workspace_domain: str):
        assert workspace_domain == "example.com"
        return {"users": [], "groups": [], "org_units": []}

    class _Request:
        state = SimpleNamespace(
            auth=SimpleNamespace(tenant_id=tenant_id),
        )

        async def json(self) -> dict[str, str]:
            return {
                "workspace_domain": "example.com",
                "admin_email": "admin@example.com",
                "scope": scope,
            }

    monkeypatch.setattr(oauth, "get_minter", lambda: object())
    monkeypatch.setattr(oauth, "build_google_onboarding_http_client", _build)
    monkeypatch.setattr(oauth, "enumerate_domain", _enumerate)

    response = await oauth.connect_preflight(_Request())

    assert response.status_code == 200
    assert captured == {
        "source": source,
        "tenant_id": str(tenant_id),
        "quota_dimensions": {"workspace": "example.com"},
    }


@pytest.mark.parametrize(
    ("module_name", "source", "scope_alias", "long_scope"),
    [
        (
            "services.ingest.integrations.google_calendar.watch",
            "google_calendar",
            "calendar.readonly",
            "https://www.googleapis.com/auth/calendar.readonly",
        ),
        (
            "services.ingest.integrations.google_drive.watch",
            "google_drive",
            "drive.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ),
    ],
)
async def test_watch_renewal_client_has_exact_installation_binding(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    source: str,
    scope_alias: str,
    long_scope: str,
) -> None:
    import importlib

    watch = importlib.import_module(module_name)
    gmail_client = importlib.import_module(
        "services.ingest.integrations.gmail.client"
    )
    dwd = importlib.import_module("services.ingest.integrations.gmail.dwd")
    tenant_id = uuid4()
    installation_id = uuid4()
    captured: dict[str, Any] = {}

    class _Http:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc: Any) -> None:
            return None

    def _build(minter: Any, **kwargs: Any) -> _Http:
        captured.update(kwargs)
        return _Http()

    monkeypatch.setattr(gmail_client, "build_google_http_client", _build)
    monkeypatch.setattr(dwd, "get_minter", lambda: object())

    client, close = await watch._make_client(
        scope_alias,
        tenant_id=tenant_id,
        installation_id=installation_id,
    )
    try:
        assert client._scope == long_scope
    finally:
        await close()

    assert captured == {
        "source": source,
        "tenant_id": str(tenant_id),
        "installation_id": str(installation_id),
    }
