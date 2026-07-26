"""Exact-installation guarantees for composed live certification."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest

from services.ingest.synthetic.validation_runs.composition import (
    LiveTarget,
    seed_live_installs,
)


class _Connection:
    def __init__(self, installation_ids: dict[str, str]) -> None:
        self.installation_ids = installation_ids
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, str]]:
        self.fetches.append((query, args))
        source = (
            "google_calendar"
            if "google_calendar_installations" in query
            else "google_drive"
        )
        installation_id = self.installation_ids.get(source)
        return [{"id": installation_id}] if installation_id is not None else []

    async def execute(self, query: str, *args: Any) -> None:
        self.executes.append((query, args))


class _Pool:
    def __init__(self, connection: _Connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire(self):
        yield self.connection


class _MetaConnection:
    def __init__(self, rows_by_source: dict[str, list[dict[str, str]]]) -> None:
        self.rows_by_source = rows_by_source
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, str]]:
        self.fetches.append((query, args))
        source = (
            "whatsapp"
            if "whatsapp_installations" in query
            else "facebook_pages"
        )
        return self.rows_by_source.get(source, [])

    async def execute(self, query: str, *args: Any) -> None:
        self.executes.append((query, args))


class _MetaSecretStore:
    def __init__(self) -> None:
        self.puts: list[tuple[str, str, Any]] = []

    async def rotate(
        self,
        ref: str,
        plaintext: str,
        *,
        tenant_id: Any,
    ) -> None:
        raise AssertionError("test rows do not carry an existing secret ref")

    async def put(
        self,
        plaintext: str,
        *,
        label: str,
        tenant_id: Any,
    ) -> str:
        self.puts.append((plaintext, label, tenant_id))
        return f"encrypted-ref-{len(self.puts)}"


class _ProviderBindingConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []
        self.executes: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetches.append((query, args))
        if "FROM provider_installations" in query:
            return self.rows
        raise AssertionError(f"unexpected fetch: {query}")

    async def execute(self, query: str, *args: Any) -> None:
        self.executes.append((query, args))


@pytest.mark.asyncio
async def test_google_live_rows_bind_the_seeded_workspace_installation() -> None:
    calendar_tenant = uuid4()
    drive_tenant = uuid4()
    connection = _Connection(
        {
            "google_calendar": "calendar-install",
            "google_drive": "drive-install",
        }
    )
    targets = [
        LiveTarget(
            tenant_id=calendar_tenant,
            source="google_calendar",
            slug="calendar-a",
            gcal_calendar_id="calendar@example.com",
            gcal_channel_id="calendar-channel",
            gcal_watch_token="calendar-token",
        ),
        LiveTarget(
            tenant_id=drive_tenant,
            source="google_drive",
            slug="drive-b",
            gdrive_drive_id="drive-id",
            gdrive_kind="shared_drive",
            gdrive_channel_id="drive-channel",
            gdrive_watch_token="drive-token",
        ),
    ]

    await seed_live_installs(_Pool(connection), targets)  # type: ignore[arg-type]

    assert len(connection.fetches) == 2
    for query, args in connection.fetches:
        assert "workspace_domain = $2" in query
        assert "LIMIT 1" not in query
        assert args[1] in {"x3-calendar-a.example", "x3-drive-b.example"}
    assert any(
        args[2] == "calendar-install"
        for query, args in connection.executes
        if "google_calendar_calendars" in query
    )
    assert any(
        args[2] == "drive-install"
        for query, args in connection.executes
        if "google_drive_targets" in query
    )


@pytest.mark.asyncio
async def test_google_live_rows_fail_when_exact_installation_is_missing() -> None:
    target = LiveTarget(
        tenant_id=uuid4(),
        source="google_calendar",
        slug="missing",
        gcal_calendar_id="calendar@example.com",
        gcal_channel_id="calendar-channel",
        gcal_watch_token="calendar-token",
    )

    with pytest.raises(RuntimeError, match="requires exactly one active installation"):
        await seed_live_installs(  # type: ignore[arg-type]
            _Pool(_Connection({})),
            [target],
        )


@pytest.mark.asyncio
async def test_meta_live_seeding_updates_only_exact_existing_installations() -> None:
    whatsapp_tenant = uuid4()
    facebook_tenant = uuid4()
    connection = _MetaConnection({
        "whatsapp": [{"id": "wa-install", "app_secret_ref": None}],
        "facebook_pages": [{"id": "fb-install", "app_secret_ref": None}],
    })
    secret_store = _MetaSecretStore()
    targets = [
        LiveTarget(
            tenant_id=whatsapp_tenant,
            source="whatsapp",
            slug="wa",
            whatsapp_phone_number_id="phone-exact",
        ),
        LiveTarget(
            tenant_id=facebook_tenant,
            source="facebook_pages",
            slug="fb",
            facebook_page_id="page-exact",
        ),
    ]

    await seed_live_installs(  # type: ignore[arg-type]
        _Pool(connection),  # type: ignore[arg-type]
        targets,
        secret_store=secret_store,
    )

    assert len(connection.fetches) == 2
    for query, args in connection.fetches:
        assert "tenant_id = $1" in query
        assert "enabled = true" in query
        assert "LIMIT 1" not in query
        assert args[1] in {"phone-exact", "page-exact"}
    assert any(
        args == ("wa-install", "encrypted-ref-1")
        for query, args in connection.executes
        if "UPDATE whatsapp_installations" in query
    )
    assert any(
        args == ("fb-install", "encrypted-ref-2")
        for query, args in connection.executes
        if "UPDATE facebook_page_installations" in query
    )
    assert [item[0] for item in secret_store.puts] == [
        "v-whatsapp-app-secret",
        "v-facebook-pages-app-secret",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["whatsapp", "facebook_pages"])
@pytest.mark.parametrize("match_count", [0, 2])
async def test_meta_live_seeding_fails_closed_on_nonexact_installation(
    source: str,
    match_count: int,
) -> None:
    installation_field = (
        {"whatsapp_phone_number_id": "phone-exact"}
        if source == "whatsapp"
        else {"facebook_page_id": "page-exact"}
    )
    target = LiveTarget(
        tenant_id=uuid4(),
        source=source,
        slug="meta",
        **installation_field,
    )
    connection = _MetaConnection({
        source: [
            {"id": f"install-{index}", "app_secret_ref": None}
            for index in range(match_count)
        ],
    })

    with pytest.raises(
        RuntimeError,
        match=rf"requires exactly one enabled installation.*found {match_count}",
    ):
        await seed_live_installs(  # type: ignore[arg-type]
            _Pool(connection),  # type: ignore[arg-type]
            [target],
            secret_store=_MetaSecretStore(),
        )

    assert connection.executes == []


@pytest.mark.asyncio
async def test_provider_live_seeding_preserves_exact_existing_tenant() -> None:
    tenant_id = uuid4()
    connection = _ProviderBindingConnection(
        [{"id": "jira-provider-install", "tenant_id": tenant_id}],
    )
    target = LiveTarget(
        tenant_id=tenant_id,
        source="jira",
        slug="jira",
        jira_site="exact.atlassian.net",
    )

    await seed_live_installs(  # type: ignore[arg-type]
        _Pool(connection),  # type: ignore[arg-type]
        [target],
    )

    assert connection.fetches[0][1] == ("jira", "exact.atlassian.net")
    assert len(connection.executes) == 1
    query, args = connection.executes[0]
    assert "UPDATE provider_installations" in query
    assert "tenant_id = $2" in query
    assert args == ("jira-provider-install", tenant_id)


@pytest.mark.asyncio
async def test_provider_live_seeding_refuses_cross_tenant_rebinding() -> None:
    requested_tenant = uuid4()
    existing_tenant = uuid4()
    connection = _ProviderBindingConnection(
        [{
            "id": "jira-provider-install",
            "tenant_id": existing_tenant,
        }],
    )
    target = LiveTarget(
        tenant_id=requested_tenant,
        source="jira",
        slug="jira",
        jira_site="shared.atlassian.net",
    )

    with pytest.raises(RuntimeError, match="refuses to rebind.*across tenants"):
        await seed_live_installs(  # type: ignore[arg-type]
            _Pool(connection),  # type: ignore[arg-type]
            [target],
        )

    assert connection.executes == []
