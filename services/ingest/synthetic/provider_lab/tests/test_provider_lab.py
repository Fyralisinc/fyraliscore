from __future__ import annotations

import base64
import json

import httpx
import pytest

from services.ingest.source_contract import CANONICAL_SOURCE_IDS
from services.ingest.synthetic.provider_lab import (
    InjectedDisconnect,
    build_lab_adapter_registry,
    build_provider_lab_app,
)


def _transport(app, *, host: str = "127.0.0.1") -> httpx.ASGITransport:
    return httpx.ASGITransport(app=app, client=(host, 43123))


def _jwt_with_sub(email: str) -> str:
    encoded = base64.urlsafe_b64encode(
        json.dumps({"sub": email}).encode()
    ).decode().rstrip("=")
    return f"header.{encoded}.signature"


async def test_registry_validates_all_27_sources_and_exposes_parity_hook() -> None:
    registry = build_lab_adapter_registry()

    assert registry.sources == CANONICAL_SOURCE_IDS
    inventory = registry.inventory()
    assert len(inventory) == 27
    assert {
        item["source"] for item in inventory if item["implemented"]
    } == set(CANONICAL_SOURCE_IDS)

    registry.validate_expected_sources(tuple(reversed(CANONICAL_SOURCE_IDS)))
    with pytest.raises(ValueError, match="parity failure"):
        registry.validate_expected_sources(CANONICAL_SOURCE_IDS[:-1])


async def test_provider_lab_cannot_start_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    with pytest.raises(
        RuntimeError,
        match="cannot start in production",
    ):
        build_provider_lab_app()


async def test_non_loopback_peer_and_forwarded_peer_are_rejected() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app, host="203.0.113.9"),
        base_url="http://provider-lab",
    ) as remote:
        response = await remote.get("/healthz")
    assert response.status_code == 403
    assert response.json()["error"]["code"] == "loopback_only"

    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as local:
        forwarded = await local.get(
            "/healthz", headers={"X-Forwarded-For": "198.51.100.7"}
        )
    assert forwarded.status_code == 403


async def test_strict_unsupported_routes_are_ledgered_without_fake_success() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        known = await client.get("/notion/v1/search")
        wrong_method = await client.post("/slack/api/conversations.list")
        unknown = await client.get("/made_up/v1/items")
        ledger = await client.get("/_lab/ledger")

    assert known.status_code == 501
    assert known.json()["error"]["code"] == "unsupported_provider_route"
    assert wrong_method.status_code == 501
    assert unknown.status_code == 404
    assert unknown.json()["error"]["code"] == "unknown_source"
    assert [entry["status_code"] for entry in ledger.json()["entries"]] == [
        501,
        501,
        404,
    ]
    # Control-plane reads do not make the ledger self-referential.
    assert ledger.json()["count"] == 3


async def test_virtual_clock_is_manual_monotonic_and_resettable() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        initial = (await client.get("/_lab/clock")).json()["now"]
        advanced = (
            await client.post(
                "/_lab/clock/advance",
                json={"seconds": 2.5, "milliseconds": 250},
            )
        ).json()["now"]
        rewound = await client.put(
            "/_lab/clock", json={"now": "2024-12-31T23:59:59Z"}
        )
        reset = (await client.post("/_lab/reset")).json()

    assert initial == "2025-01-01T00:00:00.000Z"
    assert advanced == "2025-01-01T00:00:02.750Z"
    assert rewound.status_code == 409
    assert rewound.json()["error"]["code"] == "clock_rewind_forbidden"
    assert reset["clock"]["now"] == initial


async def test_enforced_scoped_token_bucket_refills_on_virtual_clock() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        configured = await client.post(
            "/_lab/quotas",
            json={
                "source": "slack",
                "scope": "T_ONE",
                "bucket": "web-api",
                "mode": "enforce",
                "capacity": 1,
                "refill_per_second": 0.5,
            },
        )
        first = await client.get(
            "/slack/api/conversations.list",
            headers={"X-Provider-Lab-Scope": "T_ONE"},
        )
        limited = await client.get(
            "/slack/api/conversations.list",
            headers={"X-Provider-Lab-Scope": "T_ONE"},
        )
        other_scope = await client.get(
            "/slack/api/conversations.list",
            headers={"X-Provider-Lab-Scope": "T_TWO"},
        )
        await client.post("/_lab/clock/advance", json={"seconds": 2})
        refilled = await client.get(
            "/slack/api/conversations.list",
            headers={"X-Provider-Lab-Scope": "T_ONE"},
        )

    assert configured.status_code == 201
    assert first.status_code == 200
    assert first.headers["x-ratelimit-remaining"] == "0"
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "2"
    assert other_scope.status_code == 200
    assert refilled.status_code == 200


async def test_observe_quota_records_exhaustion_but_allows_request() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        await client.post(
            "/_lab/quotas",
            json={
                "source": "discord",
                "scope": "G_ONE",
                "bucket": "rest",
                "mode": "observe",
                "capacity": 1,
                "initial_tokens": 0,
                "refill_per_second": 0,
            },
        )
        response = await client.get(
            "/discord/api/v10/users/@me/guilds",
            headers={"X-Provider-Lab-Scope": "G_ONE"},
        )
        ledger = (
            await client.get("/_lab/ledger", params={"source": "discord"})
        ).json()["entries"]

    assert response.status_code == 200
    assert response.headers["x-provider-lab-quota-observed"] == "exceeded"
    assert ledger[0]["quota"] == {
        "configured": True,
        "mode": "observe",
        "remaining": 0.0,
        "would_limit": True,
    }


async def test_fault_schedule_is_deterministic_and_advances_virtual_time() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        created = await client.post(
            "/_lab/faults",
            json={
                "source": "slack",
                "route_id": "slack.conversations_list",
                "status_code": 503,
                "body": {"error": "scheduled"},
                "after_requests": 1,
                "every": 2,
                "max_hits": 2,
                "latency_ms": 1000,
            },
        )
        statuses = [
            (
                await client.get(
                    "/slack/api/conversations.list",
                    headers={"X-Provider-Lab-Scope": "T_FAULT"},
                )
            ).status_code
            for _ in range(5)
        ]
        clock = (await client.get("/_lab/clock")).json()["now"]
        fault = (await client.get("/_lab/faults")).json()["rules"][0]

    assert created.status_code == 201
    assert statuses == [200, 503, 200, 503, 200]
    assert clock == "2025-01-01T00:00:02.000Z"
    assert fault["hits"] == 2


async def test_malformed_json_fault_is_deliberately_not_parseable() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        await client.post(
            "/_lab/faults",
            json={
                "source": "gmail",
                "route_id": "gmail.profile",
                "action": "malformed_json",
                "status_code": 502,
                "max_hits": 1,
            },
        )
        response = await client.get(
            "/gmail/gmail/v1/users/me/profile",
            headers={"X-Provider-Lab-Scope": "user@example.test"},
        )

    assert response.status_code == 502
    assert response.headers["content-type"].startswith("application/json")
    with pytest.raises(json.JSONDecodeError):
        response.json()


async def test_disconnect_fault_is_ledgered_before_transport_failure() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        await client.post(
            "/_lab/faults",
            json={
                "source": "slack",
                "route_id": "slack.conversations_list",
                "action": "disconnect",
                "max_hits": 1,
            },
        )
        with pytest.raises(InjectedDisconnect):
            await client.get("/slack/api/conversations.list")
        entry = (await client.get("/_lab/ledger")).json()["entries"][0]

    assert entry["outcome"] == "fault_disconnect"
    assert entry["status_code"] is None
    assert entry["fault_id"] == "fault-000001"


async def test_ledger_redacts_credentials_and_supports_clear() -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        await client.get(
            "/slack/api/conversations.history",
            params={"channel": "C1", "access_token": "query-secret"},
            headers={
                "Authorization": "Bearer token-secret",
                "X-Provider-Lab-Scope": "T1",
                "If-None-Match": "safe-etag",
            },
        )
        entry = (await client.get("/_lab/ledger")).json()["entries"][0]
        cleared = await client.delete("/_lab/ledger")
        empty = await client.get("/_lab/ledger")

    assert entry["headers"]["authorization"] == "[REDACTED]"
    assert entry["headers"]["if-none-match"] == "safe-etag"
    assert {item["name"]: item["value"] for item in entry["query"]}[
        "access_token"
    ] == "[REDACTED]"
    assert entry["request_body"]["sha256"]
    assert cleared.status_code == 204
    assert empty.json()["count"] == 0


async def test_seeded_slack_state_routes_and_resets_to_startup_fixture() -> None:
    fixtures = {
        "slack": [
            {
                "team_id": "T_REF",
                "channels": [
                    {
                        "id": "C_REF",
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
    app = build_provider_lab_app(fixtures=fixtures)
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        listed = await client.get(
            "/slack/api/conversations.list",
            headers={"Authorization": "Bearer lab-slack::T_REF"},
        )
        history = await client.get(
            "/slack/api/conversations.history",
            params={"channel": "C_REF", "limit": 1},
            headers={"Authorization": "Bearer lab-slack::T_REF"},
        )
        await client.put(
            "/_lab/sources/slack/state",
            json={"teams": {}, "messages": {}, "direct_messages": {}, "users": {}},
        )
        await client.delete("/_lab/sources/slack/state")
        reset_list = await client.get(
            "/slack/api/conversations.list",
            headers={"Authorization": "Bearer lab-slack::T_REF"},
        )

    assert listed.json()["channels"][0]["id"] == "C_REF"
    assert history.json()["messages"] == [{"text": "newer", "ts": "2.0"}]
    assert history.json()["response_metadata"]["next_cursor"] == "1"
    assert reset_list.json() == listed.json()


async def test_reference_github_routes_support_token_pagination_and_etag() -> None:
    fixtures = {
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
    app = build_provider_lab_app(fixtures=fixtures)
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        token = await client.post(
            "/github/app/installations/77/access_tokens"
        )
        authorization = {"Authorization": f"Bearer {token.json()['token']}"}
        repos = await client.get(
            "/github/installation/repositories", headers=authorization
        )
        page = await client.get(
            "/github/repos/acme/api/issues",
            params={"per_page": 1, "page": 1},
            headers=authorization,
        )
        unchanged = await client.get(
            "/github/repos/acme/api/issues",
            headers={**authorization, "If-None-Match": page.headers["etag"]},
        )

    assert token.status_code == 201
    assert repos.json()["repositories"] == [{"full_name": "acme/api"}]
    assert page.json() == [{"id": 1, "node_id": "I_1"}]
    assert 'rel="next"' in page.headers["link"]
    assert unchanged.status_code == 304


async def test_reference_gmail_routes_support_dwd_and_hydration() -> None:
    email = "person@example.test"
    fixtures = {
        "gmail": [
            {
                "email": email,
                "current_history_id": "101",
                "messages": [
                    {"id": "m1", "threadId": "t1", "payload": {"headers": []}}
                ],
                "history_events": [
                    {"history_id": "101", "message_id": "m1"}
                ],
            }
        ]
    }
    app = build_provider_lab_app(fixtures=fixtures)
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        token = await client.post(
            "/gmail/token",
            content=(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
                f"&assertion={_jwt_with_sub(email)}"
            ),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        authorization = {"Authorization": f"Bearer {token.json()['access_token']}"}
        listed = await client.get(
            "/gmail/gmail/v1/users/me/messages", headers=authorization
        )
        hydrated = await client.get(
            "/gmail/gmail/v1/users/me/messages/m1", headers=authorization
        )
        history = await client.get(
            "/gmail/gmail/v1/users/me/history",
            params={"startHistoryId": "100"},
            headers=authorization,
        )

    assert token.json()["access_token"] == f"lab-gmail::{email}"
    assert listed.json()["messages"] == [{"id": "m1", "threadId": "t1"}]
    assert hydrated.json()["id"] == "m1"
    assert history.json()["history"][0]["messagesAdded"][0]["message"]["id"] == "m1"


async def test_reference_discord_routes_are_scoped_by_guild() -> None:
    fixtures = {
        "discord": [
            {
                "guild_id": "10",
                "channels": [
                    {
                        "id": "20",
                        "name": "general",
                        "messages": [
                            {"id": "102", "content": "new"},
                            {"id": "101", "content": "old"},
                        ],
                    }
                ],
            }
        ]
    }
    app = build_provider_lab_app(fixtures=fixtures)
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        guilds = await client.get(
            "/discord/api/v10/users/@me/guilds",
            headers={"Authorization": "Bot lab-discord::10"},
        )
        channels = await client.get("/discord/api/v10/guilds/10/channels")
        messages = await client.get(
            "/discord/api/v10/channels/20/messages",
            params={"before": "102"},
        )

    assert guilds.json() == [{"id": "10"}]
    assert channels.json() == [{"id": "20", "name": "general", "type": 0}]
    assert messages.json() == [{"content": "old", "id": "101"}]
