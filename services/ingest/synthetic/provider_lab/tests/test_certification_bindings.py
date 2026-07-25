"""Certification-kit coverage through Provider Lab production surfaces."""
from __future__ import annotations

from uuid import uuid4

import httpx

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
