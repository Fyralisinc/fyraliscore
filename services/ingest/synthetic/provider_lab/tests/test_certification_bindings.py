"""Certification-kit coverage through Provider Lab production surfaces."""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import ProviderPermanentError
from lib.shared.provider_transport import RequestContext
from services.ingest.integrations.facebook_pages.client import (
    FacebookPagesClient,
)
from services.ingest.source_certification.runtime import (
    resolve_fixture_factory,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    source_definition,
)
from services.ingest.synthetic.provider_lab import (
    build_provider_lab_app,
    start_provider_lab,
)


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        self.contexts.append(context)
        return await call()


def _all_history_fixtures() -> dict[str, list[dict]]:
    return {
        source_id: [
            resolve_fixture_factory(source_id)(
                fixture_params={},
                installation_id=f"x3-certification-{source_id}",
            )
        ]
        for source_id in CANONICAL_SOURCE_IDS
        if source_definition(source_id).history is not None
    }


def test_all_26_history_fixture_bindings_seed_provider_lab() -> None:
    fixtures = _all_history_fixtures()

    assert len(fixtures) == 26
    assert "whatsapp" not in fixtures
    server = start_provider_lab(fixtures)
    try:
        registered = server.app.state.provider_lab.registry.sources
        assert set(fixtures).issubset(registered)
    finally:
        server.shutdown()


async def test_facebook_pages_production_client_pages_certification_fixture() -> None:
    fixture = resolve_fixture_factory("facebook_pages")(
        fixture_params={
            "conversations": 1,
            "messages_per_conversation": 3,
        },
        installation_id="PAGE-CERTIFICATION",
    )
    app = build_provider_lab_app(fixtures={"facebook_pages": [fixture]})
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app,
            client=("127.0.0.1", 43129),
        ),
        base_url="http://provider-lab",
    )
    recorder = _Recorder()
    client = FacebookPagesClient(
        base_url="http://provider-lab/facebook/v23.0",
        access_token="spam-facebook-pages::PAGE-CERTIFICATION",
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        http_client=http,
        provider_transport=recorder,
        allow_unlimited_local=True,
    )

    try:
        conversations, conversation_after = await client.list_conversations(
            page_id="PAGE-CERTIFICATION",
            limit=1,
        )
        messages, first_after = await client.list_messages(
            conversation_id=conversations[0]["id"],
            limit=2,
        )
        remaining, final_after = await client.list_messages(
            conversation_id=conversations[0]["id"],
            after=first_after,
            limit=2,
        )
    finally:
        await http.aclose()

    assert conversation_after is None
    assert len(conversations) == 1
    assert len(messages) == 2
    assert len(remaining) == 1
    assert first_after is not None
    assert final_after is None
    assert {context.operation for context in recorder.contexts} == {
        "conversations.list",
        "messages.list",
    }


async def test_facebook_pages_multi_install_fixtures_are_isolated() -> None:
    fixtures = [
        resolve_fixture_factory("facebook_pages")(
            fixture_params={
                "conversations": 1,
                "messages_per_conversation": message_count,
            },
            installation_id=page_id,
        )
        for page_id, message_count in (
            ("PAGE-A", 2),
            ("PAGE-B", 3),
        )
    ]
    app = build_provider_lab_app(fixtures={"facebook_pages": fixtures})
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app,
            client=("127.0.0.1", 43129),
        ),
        base_url="http://provider-lab",
    )
    clients = [
        FacebookPagesClient(
            base_url="http://provider-lab/facebook/v23.0",
            access_token=f"spam-facebook-pages::{page_id}",
            tenant_id=uuid4(),
            installation_row_id=uuid4(),
            http_client=http,
            provider_transport=_Recorder(),
            allow_unlimited_local=True,
        )
        for page_id in ("PAGE-A", "PAGE-B")
    ]

    try:
        results = []
        for page_id, client in zip(("PAGE-A", "PAGE-B"), clients, strict=True):
            conversations, _ = await client.list_conversations(page_id=page_id)
            messages, _ = await client.list_messages(
                conversation_id=conversations[0]["id"],
            )
            results.append((conversations, messages))
        with pytest.raises(ProviderPermanentError, match="HTTP 403"):
            await clients[0].list_conversations(page_id="PAGE-B")
    finally:
        await http.aclose()

    assert [
        (
            [conversation["id"] for conversation in conversations],
            len(messages),
        )
        for conversations, messages in results
    ] == [
        (["PAGE-A-conversation-1"], 2),
        (["PAGE-B-conversation-1"], 3),
    ]


def test_facebook_pages_duplicate_page_fixture_is_rejected() -> None:
    duplicate = [
        resolve_fixture_factory("facebook_pages")(
            fixture_params={},
            installation_id="PAGE-DUPLICATE",
        )
        for _ in range(2)
    ]

    with pytest.raises(ValueError, match="duplicate pages ids"):
        build_provider_lab_app(fixtures={"facebook_pages": duplicate})
