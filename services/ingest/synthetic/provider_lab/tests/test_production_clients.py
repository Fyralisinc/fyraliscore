"""Production-client conformance against the local Provider Lab.

These tests deliberately instantiate the real clients with their documented
local endpoint seams.  The Provider Lab is therefore checked at the boundary
Fyralis actually uses, instead of only through hand-written HTTP requests.
"""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from services.ingest.integrations.figma.client import FigmaClient
from services.ingest.integrations.github.client import GithubClient
from services.ingest.integrations.gmail.client import (
    GMAIL_METADATA_SCOPE,
    GmailClient,
    GoogleHttpClient,
)
from services.ingest.integrations.google_calendar.client import (
    CALENDAR_READONLY_SCOPE,
    GoogleCalendarClient,
)
from services.ingest.integrations.google_drive.client import (
    DRIVE_READONLY_SCOPE,
    GoogleDriveClient,
)
from services.ingest.integrations.jira.client import JiraClient
from services.ingest.integrations.slack.client import SlackClient
from services.ingest.synthetic.fixtures.jira_generator import make_jira
from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _transport(app) -> httpx.ASGITransport:  # noqa: ANN001
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43123))


class _ProviderLabMinter:
    def __init__(self) -> None:
        self.invalidations: list[tuple[str, tuple[str, ...]]] = []

    async def mint(
        self,
        *,
        user_email: str,
        scopes: list[str],
        **_context: object,
    ) -> str:
        assert scopes
        return f"lab-gmail::{user_email}"

    def invalidate(self, *, user_email: str, scopes: list[str]) -> None:
        self.invalidations.append((user_email, tuple(scopes)))


async def test_production_slack_client_runs_unmodified_against_lab() -> None:
    app = build_provider_lab_app(
        fixtures={
            "slack": [
                {
                    "team_id": "T_PRODUCTION_CLIENT",
                    "channels": [
                        {
                            "id": "C_GENERAL",
                            "name": "general",
                            "messages": [
                                {"ts": "2.0", "text": "newer"},
                                {"ts": "1.0", "text": "older"},
                            ],
                        }
                    ],
                }
            ]
        }
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id="T_PRODUCTION_CLIENT",
        base_url="http://provider-lab/slack/api",
        http_client=http_client,
    )
    client._bot_token_cache.set(  # type: ignore[attr-defined]
        "lab-slack::T_PRODUCTION_CLIENT",
        ttl_seconds=float("inf"),
    )

    try:
        channels = await client.conversations_list()
        messages, next_cursor = await client.conversations_history(
            channel="C_GENERAL",
            limit=1,
        )
    finally:
        await client.aclose()

    assert channels == [
        {
            "id": "C_GENERAL",
            "name": "general",
            "team_id": "T_PRODUCTION_CLIENT",
        }
    ]
    assert messages == [{"text": "newer", "ts": "2.0"}]
    assert next_cursor == "1"


async def test_production_github_client_runs_unmodified_against_lab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = build_provider_lab_app(
        fixtures={
            "github": [
                {
                    "installation_id": "77",
                    "repos": [
                        {
                            "full_name": "acme/api",
                            "events_by_type": {
                                "issues": [
                                    {"id": 1, "node_id": "I_1"},
                                    {"id": 2, "node_id": "I_2"},
                                ]
                            },
                        }
                    ],
                }
            ]
        }
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    monkeypatch.setattr(
        "services.ingest.integrations.github.client.mint_app_jwt",
        lambda: "provider-lab-app-jwt",
    )
    client = GithubClient(
        pool=None,  # type: ignore[arg-type]
        http_client=http_client,
        api_base_url="http://provider-lab/github",
        backfill_installation_id="77",
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
    )

    try:
        repositories = await client.list_repositories_for_backfill("77")
        records, etag, next_page = await client.list_repo_events(
            owner="acme",
            repo="api",
            event_type="issues",
            per_page=1,
        )
        changed, repeated_etag = await client.head_repo_events(
            owner="acme",
            repo="api",
            event_type="issues",
            etag=etag,
        )
    finally:
        await client.aclose()
        await http_client.aclose()

    assert repositories == ["acme/api"]
    assert records == [{"id": 1, "node_id": "I_1"}]
    assert next_page == 2
    assert changed is False
    assert repeated_etag == etag


async def test_production_gmail_client_runs_unmodified_against_lab() -> None:
    email = "production-client@example.test"
    app = build_provider_lab_app(
        fixtures={
            "gmail": [
                {
                    "email": email,
                    "current_history_id": "101",
                    "messages": [
                        {
                            "id": "m1",
                            "threadId": "t1",
                            "payload": {"headers": []},
                        }
                    ],
                    "history_events": [
                        {"history_id": "101", "message_id": "m1"}
                    ],
                }
            ]
        }
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    minter = _ProviderLabMinter()
    google_http = GoogleHttpClient(
        minter,  # type: ignore[arg-type]
        http_client=http_client,
        source="gmail",
        tenant_id=str(uuid4()),
        installation_id=str(uuid4()),
    )
    client = GmailClient(
        google_http,
        base_url="http://provider-lab/gmail/gmail/v1",
    )

    try:
        listed = await client.messages_list(
            user_email=email,
            scope=GMAIL_METADATA_SCOPE,
        )
        hydrated = await client.get_message(
            user_email=email,
            scope=GMAIL_METADATA_SCOPE,
            message_id="m1",
        )
        history = await client.history_list(
            user_email=email,
            scope=GMAIL_METADATA_SCOPE,
            start_history_id="100",
        )
    finally:
        await google_http.__aexit__()
        await http_client.aclose()

    assert listed["messages"] == [{"id": "m1", "threadId": "t1"}]
    assert hydrated["id"] == "m1"
    assert history["history"][0]["messagesAdded"][0]["message"]["id"] == "m1"
    assert minter.invalidations == []


async def test_production_google_calendar_client_used_surface_against_lab() -> None:
    email = "calendar-owner@example.test"
    app = build_provider_lab_app(
        fixtures={
            "google_calendar": [
                {
                    "events": {
                        email: [
                            {
                                "id": "event-1",
                                "summary": "Provider Lab event",
                                "updated": "2025-01-01T00:00:00Z",
                            }
                        ]
                    },
                    "delta": {
                        email: [
                            {
                                "id": "event-2",
                                "summary": "Incremental event",
                                "updated": "2025-01-02T00:00:00Z",
                            }
                        ]
                    },
                }
            ]
        }
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    google_http = GoogleHttpClient(
        _ProviderLabMinter(),  # type: ignore[arg-type]
        http_client=http_client,
        source="google_calendar",
        tenant_id=str(uuid4()),
        installation_id=str(uuid4()),
    )
    client = GoogleCalendarClient(
        google_http,
        scope=CALENDAR_READONLY_SCOPE,
        base_url="http://provider-lab/gcal/calendar/v3",
    )

    try:
        calendars = await client.list_calendars(user_email=email)
        full = await client.list_events(
            calendar_id=email,
            user_email=email,
        )
        delta = await client.list_events(
            calendar_id=email,
            user_email=email,
            sync_token="sync-1",
        )
        channel = await client.watch_events(
            calendar_id=email,
            user_email=email,
            channel_id="calendar-channel",
            address="https://example.test/calendar-push",
            token="calendar-watch-secret",
        )
        await client.stop_channel(
            user_email=email,
            channel_id=channel["id"],
            resource_id=channel["resourceId"],
        )
    finally:
        await http_client.aclose()

    assert calendars["items"] == [{"id": email, "summary": email}]
    assert full["items"][0]["id"] == "event-1"
    assert full["nextSyncToken"] == "sync-1"
    assert delta["items"][0]["id"] == "event-2"
    assert channel["id"] == "calendar-channel"


async def test_production_google_drive_client_used_surface_against_lab() -> None:
    email = "drive-owner@example.test"
    app = build_provider_lab_app(
        fixtures={
            "google_drive": [
                {
                    "files": [
                        {
                            "id": "file-1",
                            "name": "runbook.txt",
                            "mimeType": "text/plain",
                        }
                    ],
                    "changes": [
                        {
                            "fileId": "file-1",
                            "removed": False,
                            "file": {"id": "file-1", "name": "runbook.txt"},
                        }
                    ],
                    "extracted_text": {"file-1": "provider lab document"},
                    "comments": {"file-1": [{"id": "comment-1"}]},
                    "revisions": {"file-1": [{"id": "revision-1"}]},
                    "start_page_token": "spt-1",
                    "new_start_page_token": "spt-2",
                }
            ]
        }
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    google_http = GoogleHttpClient(
        _ProviderLabMinter(),  # type: ignore[arg-type]
        http_client=http_client,
        source="google_drive",
        tenant_id=str(uuid4()),
        installation_id=str(uuid4()),
    )
    client = GoogleDriveClient(
        google_http,
        scope=DRIVE_READONLY_SCOPE,
        base_url="http://provider-lab/gdrive/drive/v3",
    )

    try:
        start_token = await client.get_start_page_token(user_email=email)
        files = await client.list_files(user_email=email)
        changes = await client.list_changes(
            user_email=email,
            page_token=start_token,
        )
        exported = await client.export_text(
            user_email=email,
            file_id="file-1",
            mime_type="text/plain",
            max_bytes=1_000,
        )
        comments = await client.list_comments(
            user_email=email,
            file_id="file-1",
        )
        revisions = await client.list_revisions(
            user_email=email,
            file_id="file-1",
        )
        channel = await client.watch_changes(
            user_email=email,
            page_token=start_token,
            channel_id="drive-channel",
            address="https://example.test/drive-push",
            token="drive-watch-secret",
        )
        await client.stop_channel(
            user_email=email,
            channel_id=channel["id"],
            resource_id=channel["resourceId"],
        )
    finally:
        await http_client.aclose()

    assert start_token == "spt-1"
    assert files["files"][0]["id"] == "file-1"
    assert changes["newStartPageToken"] == "spt-2"
    assert exported == "provider lab document"
    assert comments["comments"] == [{"id": "comment-1"}]
    assert revisions["revisions"] == [{"id": "revision-1"}]
    assert channel["id"] == "drive-channel"


async def test_production_figma_client_used_surface_against_lab() -> None:
    app = build_provider_lab_app(
        fixtures={
            "figma": [
                {
                    "files": {
                        "FIGMA_FILE": {
                            "key": "FIGMA_FILE",
                            "name": "Provider Lab design",
                            "events": [
                                {
                                    "id": "version-1",
                                    "event_type": "FILE_VERSION_UPDATE",
                                    "version": "version-1",
                                    "created_at": "2025-01-01T00:00:00Z",
                                },
                                {
                                    "id": "comment-1",
                                    "event_type": "FILE_COMMENT",
                                    "message": "Looks good",
                                    "created_at": "2025-01-02T00:00:00Z",
                                },
                            ],
                        }
                    }
                }
            ]
        }
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = FigmaClient(
        base_url="http://provider-lab/figma",
        api_base_url="http://provider-lab/figma",
        api_token="provider-lab-token",
        http_client=http_client,
        team_id="TEAM_ONE",
    )

    try:
        current_user = await client.get_current_user()
        files = await client.list_files()
        file = await client.get_file("FIGMA_FILE")
        events, next_offset, total = await client.list_events(
            "FIGMA_FILE",
            limit=20,
        )
    finally:
        await client.aclose()
        await http_client.aclose()

    assert current_user["id"] == "provider-lab-user"
    assert files[0]["key"] == "FIGMA_FILE"
    assert file["name"] == "Provider Lab design"
    assert {event["event_type"] for event in events} == {
        "FILE_COMMENT",
        "FILE_VERSION_UPDATE",
    }
    assert next_offset is None
    assert total == 2


async def test_production_jira_client_used_surface_against_lab() -> None:
    fixture = make_jira(projects=1, issues_per_project=3)
    fixture["projects"][0]["delta"] = list(
        fixture["projects"][0]["issues"],
    )
    project_key = fixture["projects"][0]["project_key"]
    app = build_provider_lab_app(fixtures={"jira": [fixture]})
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = JiraClient(
        base_url="http://provider-lab/jira",
        api_base_url="http://provider-lab/jira",
        account_email="sandbox@acme.example",
        api_token="provider-lab-token",
        http_client=http_client,
    )

    try:
        current_user = await client.myself()
        projects, next_start, total_projects = await client.list_projects()
        issues, next_token, is_last = await client.search_issues(
            jql=f'project = "{project_key}" ORDER BY updated ASC',
        )
        has_updates = await client.has_updates_since(
            project_key=project_key,
            updated_min_jql="2025/01/01 00:00",
        )
    finally:
        await client.aclose()
        await http_client.aclose()

    assert current_user["accountId"] == "sandbox-account"
    assert [project["key"] for project in projects] == [project_key]
    assert next_start is None
    assert total_projects == 1
    assert len(issues) == 3
    assert next_token is None
    assert is_last is True
    assert has_updates is True
