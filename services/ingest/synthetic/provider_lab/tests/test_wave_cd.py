from __future__ import annotations

import base64
import hashlib
import hmac
import json

import httpx

from services.ingest.synthetic.fixtures.ashby_generator import make_ashby
from services.ingest.synthetic.fixtures.aws_generator import make_aws
from services.ingest.synthetic.fixtures.hibob_generator import make_hibob
from services.ingest.synthetic.fixtures.linkedin_generator import make_linkedin
from services.ingest.synthetic.fixtures.notion_generator import make_notion
from services.ingest.synthetic.fixtures.signal_generator import make_signal
from services.ingest.synthetic.fixtures.telegram_generator import make_telegram
from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _transport(app) -> httpx.ASGITransport:
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43124))


def _basic(username: str, password: str) -> str:
    encoded = base64.b64encode(
        f"{username}:{password}".encode("utf-8")
    ).decode("ascii")
    return f"Basic {encoded}"


def _fixtures() -> dict[str, list[dict]]:
    return {
        "notion": [
            make_notion(
                workspace_id="notion-ws",
                pages_per_database=3,
                loose_pages=3,
                blocks_per_page=1,
                comments_per_item=1,
                page_size=2,
            )
        ],
        "hibob": [
            make_hibob(
                company_id="hibob-co",
                rows_per_entity=3,
                page_size=2,
            )
        ],
        "ashby": [
            make_ashby(
                org_id="ashby-org",
                entities=["candidate"],
                rows_per_entity=3,
                page_size=2,
            )
        ],
        "linkedin": [
            make_linkedin(
                organization_urn="urn:li:organization:123",
                rows_per_entity=3,
                page_size=2,
            )
        ],
        "aws": [
            make_aws(
                account_id="123456789012",
                events=3,
                per_page=2,
            )
        ],
        "telegram": [
            make_telegram(dialogs=1, messages_per_dialog=3, page_size=2)
        ],
        "signal": [
            make_signal(threads=2, messages_per_thread=2, page_size=2)
        ],
    }


async def test_wave_cd_fixture_routes_and_cursor_resume() -> None:
    fixtures = _fixtures()
    app = build_provider_lab_app(fixtures=fixtures)
    notion_db = fixtures["notion"][0]["databases"][0]["database_id"]
    telegram_dialog = fixtures["telegram"][0]["dialog_order"][0]

    notion_headers = {
        "Authorization": "Bearer lab-notion::notion-ws",
        "Notion-Version": "2022-06-28",
    }
    linkedin_headers = {
        "Authorization": "Bearer token",
        "LinkedIn-Version": "202605",
        "X-Restli-Protocol-Version": "2.0.0",
    }
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        notion_1 = await client.post(
            f"/notion/v1/databases/{notion_db}/query",
            headers=notion_headers,
            json={"page_size": 2},
        )
        notion_2 = await client.post(
            f"/notion/v1/databases/{notion_db}/query",
            headers=notion_headers,
            json={
                "page_size": 2,
                "start_cursor": notion_1.json()["next_cursor"],
            },
        )
        hibob_1 = await client.get(
            "/hibob/v1/bulk/people/salaries",
            headers={"Authorization": _basic("svc", "token")},
            params={"limit": 2},
        )
        hibob_2 = await client.get(
            "/hibob/v1/bulk/people/salaries",
            headers={"Authorization": _basic("svc", "token")},
            params={
                "limit": 2,
                "cursor": hibob_1.json()["response_metadata"]["next_cursor"],
            },
        )
        ashby_1 = await client.post(
            "/ashby/candidate.list",
            headers={
                "Authorization": _basic("key", ""),
                "X-Provider-Lab-Scope": "ashby-org",
            },
            json={"limit": 2},
        )
        assert ashby_1.status_code == 200, ashby_1.text
        assert "nextCursor" in ashby_1.json(), ashby_1.text
        ashby_2 = await client.post(
            "/ashby/candidate.list",
            headers={
                "Authorization": _basic("key", ""),
                "X-Provider-Lab-Scope": "ashby-org",
            },
            json={"limit": 2, "cursor": ashby_1.json()["nextCursor"]},
        )
        linkedin_1 = await client.get(
            "/linkedin/posts",
            headers=linkedin_headers,
            params={
                "q": "author",
                "author": "urn:li:organization:123",
                "count": 2,
            },
        )
        linkedin_2 = await client.get(
            "/linkedin/posts",
            headers=linkedin_headers,
            params={
                "q": "author",
                "author": "urn:li:organization:123",
                "start": 2,
                "count": 2,
            },
        )
        aws_1 = await client.post(
            "/aws",
            headers={
                "Authorization": "AWS4-HMAC-SHA256 Credential=lab/signature",
                "X-Amz-Target": (
                    "com.amazonaws.cloudtrail.v20131101."
                    "CloudTrail_20131101.LookupEvents"
                ),
            },
            json={"MaxResults": 2},
        )
        aws_2 = await client.post(
            "/aws",
            headers={
                "Authorization": "AWS4-HMAC-SHA256 Credential=lab/signature",
                "X-Amz-Target": (
                    "com.amazonaws.cloudtrail.v20131101."
                    "CloudTrail_20131101.LookupEvents"
                ),
            },
            json={"MaxResults": 2, "NextToken": aws_1.json()["NextToken"]},
        )
        telegram_1 = await client.post(
            "/telegram/transport/get_history",
            headers={"Authorization": "Session lab-telegram::account"},
            json={"dialog_id": telegram_dialog, "limit": 2},
        )
        telegram_2 = await client.post(
            "/telegram/transport/get_history",
            headers={"Authorization": "Session lab-telegram::account"},
            json={
                "dialog_id": telegram_dialog,
                "limit": 2,
                "offset_id": telegram_1.json()["next_offset_id"],
            },
        )

    assert [len(notion_1.json()["results"]), len(notion_2.json()["results"])] == [
        2,
        1,
    ]
    assert [len(hibob_1.json()["results"]), len(hibob_2.json()["results"])] == [
        2,
        1,
    ]
    assert [len(ashby_1.json()["results"]), len(ashby_2.json()["results"])] == [
        2,
        1,
    ]
    assert [
        len(linkedin_1.json()["elements"]),
        len(linkedin_2.json()["elements"]),
    ] == [2, 1]
    assert [len(aws_1.json()["Events"]), len(aws_2.json()["Events"])] == [2, 1]
    assert [
        len(telegram_1.json()["messages"]),
        len(telegram_2.json()["messages"]),
    ] == [2, 1]


async def test_wave_cd_auth_is_provider_shaped_and_strict() -> None:
    app = build_provider_lab_app(fixtures=_fixtures())
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        responses = [
            await client.post("/notion/v1/search", json={}),
            await client.post("/hibob/v1/people/search", json={}),
            await client.post("/ashby/candidate.list", json={}),
            await client.get(
                "/linkedin/posts",
                headers={"Authorization": "Bearer token"},
            ),
            await client.post("/aws", json={}),
            await client.post(
                "/telegram/transport/me",
                json={},
            ),
            await client.post(
                "/signal/jsonrpc",
                json={"jsonrpc": "2.0", "id": 1, "method": "listGroups"},
            ),
        ]
        unsupported = await client.get("/telegram/messages.getHistory")

    assert [response.status_code for response in responses] == [
        401,
        401,
        401,
        400,
        403,
        401,
        401,
    ]
    assert unsupported.status_code == 501
    assert (
        unsupported.json()["error"]["code"]
        == "unsupported_provider_route"
    )


async def test_signal_is_limited_to_pinned_json_rpc_and_sse() -> None:
    app = build_provider_lab_app(fixtures=_fixtures())
    headers = {"Authorization": "Session lab-signal::account"}
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        groups = await client.post(
            "/signal/jsonrpc",
            headers=headers,
            json={"jsonrpc": "2.0", "id": 1, "method": "listGroups"},
        )
        subscription = await client.post(
            "/signal/jsonrpc",
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "subscribeReceive",
            },
        )
        subscription_id = subscription.json()["result"]["subscription"]
        events = await client.get(
            f"/signal/events/{subscription_id}",
            headers=headers,
        )
        rejected = await client.post(
            "/signal/jsonrpc",
            headers=headers,
            json={"jsonrpc": "2.0", "id": 3, "method": "getHistory"},
        )

    assert groups.json()["result"]
    assert events.status_code == 200
    assert events.headers["content-type"].startswith("text/event-stream")
    assert "event: receive" in events.text
    assert rejected.json()["error"]["code"] == -32601


async def test_meta_graph_and_signed_webhook_surfaces() -> None:
    app = build_provider_lab_app()
    facebook_state = {
        "pages": {
            "page-1": {
                "id": "page-1",
                "name": "Provider Lab Page",
                "access_token": "page-token",
                "tasks": ["MESSAGING"],
            }
        },
        "user_pages": {},
        "conversations": {
            "page-1": [
                {"id": "conversation-1", "updated_time": "2026-01-01T00:00:00Z"}
            ]
        },
        "messages": {
            "conversation-1": [
                {"id": "message-1", "message": "hello"},
                {"id": "message-2", "message": "world"},
                {"id": "message-3", "message": "again"},
            ]
        },
        "verify_tokens": ["verify"],
        "app_secrets": {"page-1": "facebook-secret"},
        "installations": {"page-1": {"enabled": True}},
    }
    whatsapp_state = {
        "verify_tokens": ["verify"],
        "app_secrets": {"phone-1": "whatsapp-secret"},
        "installations": {"phone-1": {"enabled": True}},
    }
    facebook_delivery = {
        "object": "page",
        "entry": [
            {
                "id": "page-1",
                "messaging": [
                    {
                        "sender": {"id": "person-1"},
                        "recipient": {"id": "page-1"},
                        "message": {"mid": "message-4", "text": "hello"},
                    }
                ],
            }
        ],
    }
    whatsapp_delivery = {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": "phone-1"},
                            "messages": [{"id": "wamid-1", "text": {"body": "hi"}}],
                            "statuses": [{"id": "wamid-2", "status": "read"}],
                        }
                    }
                ]
            }
        ],
    }

    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        await client.put(
            "/_lab/sources/facebook_pages/state",
            json=facebook_state,
        )
        await client.put("/_lab/sources/whatsapp/state", json=whatsapp_state)
        pages = await client.get(
            "/facebook/v23.0/me/accounts",
            params={"access_token": "user-token"},
        )
        messages_1 = await client.get(
            "/facebook/v23.0/conversation-1/messages",
            params={"access_token": "page-token", "limit": 2},
        )
        messages_2 = await client.get(
            "/facebook/v23.0/conversation-1/messages",
            params={
                "access_token": "page-token",
                "limit": 2,
                "after": messages_1.json()["paging"]["cursors"]["after"],
            },
        )
        facebook_verify = await client.get(
            "/facebook/webhook",
            params={
                "hub.mode": "subscribe",
                "hub.verify_token": "verify",
                "hub.challenge": "challenge",
            },
        )
        whatsapp_verify = await client.get(
            "/whatsapp/webhook",
            params={
                "hub.mode": "subscribe",
                "hub.verify_token": "verify",
                "hub.challenge": "challenge",
            },
        )
        facebook_raw = json.dumps(
            facebook_delivery,
            separators=(",", ":"),
        ).encode()
        facebook_webhook = await client.post(
            "/facebook/webhook",
            content=facebook_raw,
            headers={
                "Content-Type": "application/json",
                "X-Hub-Signature-256": (
                    "sha256="
                    + hmac.new(
                        b"facebook-secret",
                        facebook_raw,
                        hashlib.sha256,
                    ).hexdigest()
                ),
            },
        )
        whatsapp_raw = json.dumps(
            whatsapp_delivery,
            separators=(",", ":"),
        ).encode()
        whatsapp_webhook = await client.post(
            "/whatsapp/webhook",
            content=whatsapp_raw,
            headers={
                "Content-Type": "application/json",
                "X-Hub-Signature-256": (
                    "sha256="
                    + hmac.new(
                        b"whatsapp-secret",
                        whatsapp_raw,
                        hashlib.sha256,
                    ).hexdigest()
                ),
            },
        )
        no_whatsapp_history = await client.get("/whatsapp/history")

    assert pages.json()["data"][0]["id"] == "page-1"
    assert [len(messages_1.json()["data"]), len(messages_2.json()["data"])] == [
        2,
        1,
    ]
    assert facebook_verify.text == whatsapp_verify.text == "challenge"
    assert facebook_webhook.json()["messages"] == 1
    assert whatsapp_webhook.json()["messages"] == 1
    assert whatsapp_webhook.json()["statuses"] == 1
    assert no_whatsapp_history.status_code == 501


async def test_wave_cd_quota_and_telegram_flood_wait_fault_boundary() -> None:
    fixtures = _fixtures()
    telegram_dialog = fixtures["telegram"][0]["dialog_order"][0]
    app = build_provider_lab_app(fixtures=fixtures)
    async with httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    ) as client:
        fault = await client.post(
            "/_lab/faults",
            json={
                "source": "telegram",
                "route_id": "telegram.get_history",
                "status_code": 420,
                "body": {
                    "error": {
                        "code": "FLOOD_WAIT",
                        "seconds": 5,
                    }
                },
                "headers": {"Retry-After": "5"},
                "max_hits": 1,
            },
        )
        flood_wait = await client.post(
            "/telegram/transport/get_history",
            headers={"Authorization": "Session lab-telegram::account"},
            json={"dialog_id": telegram_dialog},
        )
        quota = await client.post(
            "/_lab/quotas",
            json={
                "source": "signal",
                "scope": "account",
                "bucket": "json-rpc",
                "mode": "enforce",
                "capacity": 1,
                "initial_tokens": 0,
                "refill_per_second": 0,
            },
        )
        limited = await client.post(
            "/signal/jsonrpc",
            headers={"Authorization": "Session lab-signal::account"},
            json={"jsonrpc": "2.0", "id": 1, "method": "listGroups"},
        )

    assert fault.status_code == 201
    assert flood_wait.status_code == 420
    assert flood_wait.json()["error"]["code"] == "FLOOD_WAIT"
    assert flood_wait.headers["retry-after"] == "5"
    assert quota.status_code == 201
    assert limited.status_code == 429
    assert limited.json()["error"]["code"] == "quota_exceeded"
