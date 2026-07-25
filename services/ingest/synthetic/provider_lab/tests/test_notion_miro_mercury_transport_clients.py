"""Real Notion, Miro, and Mercury clients against Provider Lab."""
from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest

from lib.shared.provider_transport import RequestContext
from services.ingest.integrations.mercury.client import MercuryClient
from services.ingest.integrations.miro.client import MiroClient
from services.ingest.integrations.notion.client import NotionClient
from services.ingest.integrations.notion.oauth import _exchange_code_for_token
from services.ingest.synthetic.fixtures.mercury_generator import make_mercury
from services.ingest.synthetic.fixtures.miro_generator import make_miro
from services.ingest.synthetic.fixtures.notion_generator import make_notion
from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _transport(app) -> httpx.ASGITransport:  # noqa: ANN001
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43127))


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        self.contexts.append(context)
        return await call()


async def test_real_clients_and_notion_oauth_run_against_lab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notion_fixture = make_notion(
        workspace_id="workspace-production-client",
        databases=1,
        pages_per_database=2,
        loose_pages=1,
        blocks_per_page=1,
        comments_per_item=1,
        page_size=1,
    )
    miro_fixture = make_miro(
        org_id="org-production-client",
        boards=1,
        items_per_board=2,
    )
    mercury_fixture = make_mercury(
        accounts=1,
        transactions_per_account=2,
    )
    app = build_provider_lab_app(
        fixtures={
            "notion": [notion_fixture],
            "miro": [miro_fixture],
            "mercury": [mercury_fixture],
        },
    )
    http = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    tenant_id = uuid4()
    recorder = _Recorder()

    def preinstall_binding(tenant: UUID) -> dict[str, object]:
        assert tenant == tenant_id
        return {
            "tenant_id": tenant,
            "provider_transport": recorder,
            "allow_unlimited_local": True,
            "require_tenant_installation": False,
        }

    monkeypatch.setattr(
        "services.ingest.integrations.notion.oauth."
        "tenant_preinstall_transport_kwargs",
        preinstall_binding,
    )
    monkeypatch.setenv("NOTION_CLIENT_ID", "provider-lab-client")
    monkeypatch.setenv("NOTION_CLIENT_SECRET", "provider-lab-secret")
    monkeypatch.setenv(
        "NOTION_REDIRECT_URI",
        "https://fyralis.test/integrations/notion/callback",
    )

    notion = NotionClient(
        tenant_id=tenant_id,
        installation_row_id=uuid4(),
        workspace_id=notion_fixture["workspace_id"],
        bot_token=f"lab-notion::{notion_fixture['workspace_id']}",
        http_client=http,
        api_base_url="http://provider-lab/notion",
        provider_transport=recorder,
        allow_unlimited_local=True,
    )
    miro = MiroClient(
        base_url="http://provider-lab/miro",
        tenant_id=tenant_id,
        installation_row_id=uuid4(),
        api_token="lab-miro",
        http_client=http,
        provider_transport=recorder,
        allow_unlimited_local=True,
    )
    mercury = MercuryClient(
        base_url="http://provider-lab/mercury",
        tenant_id=tenant_id,
        installation_row_id=uuid4(),
        api_token="lab-mercury",
        http_client=http,
        provider_transport=recorder,
        allow_unlimited_local=True,
    )

    try:
        token = await _exchange_code_for_token(
            "provider-lab-code",
            tenant_id=tenant_id,
            http_client=http,
            token_url="http://provider-lab/notion/v1/oauth/token",
        )
        databases, database_cursor, database_more = await notion.search(
            object_filter="database",
        )
        database_id = notion_fixture["databases"][0]["database_id"]
        rows, row_cursor, row_more = await notion.query_database(
            database_id,
            page_size=1,
        )
        row_id = rows[0]["id"]
        blocks, _, _ = await notion.list_block_children(row_id)
        comments, _, _ = await notion.list_comments(row_id)
        hydrated = await notion.retrieve_page(row_id)
        bot = await notion.retrieve_bot_user()

        boards = await miro.list_boards()
        board_id = boards[0]["id"]
        board = await miro.get_board(board_id)
        items, item_cursor, _ = await miro.list_items(board_id, limit=1)

        accounts = await mercury.list_accounts()
        account_id = accounts[0]["id"]
        account = await mercury.get_account(account_id)
        transactions, transaction_cursor, _ = (
            await mercury.list_transactions(account_id, limit=1)
        )
    finally:
        await http.aclose()

    assert token["workspace_id"] == notion_fixture["workspace_id"]
    assert token["access_token"].startswith("lab-notion::")
    assert databases[0]["id"] == database_id
    assert database_cursor is None and database_more is False
    assert row_cursor is not None and row_more is True
    assert blocks and comments
    assert hydrated["id"] == row_id
    assert bot["type"] == "bot"
    assert board["id"] == board_id
    assert len(items) == 1 and item_cursor is not None
    assert account["id"] == account_id
    assert len(transactions) == 1 and transaction_cursor == 1

    operations = {
        (context.source, context.operation)
        for context in recorder.contexts
    }
    assert operations == {
        ("notion", "oauth.token.exchange"),
        ("notion", "search"),
        ("notion", "databases.query"),
        ("notion", "blocks.children.list"),
        ("notion", "comments.list"),
        ("notion", "pages.retrieve"),
        ("notion", "users.me"),
        ("miro", "boards.list"),
        ("miro", "boards.get"),
        ("miro", "board_items.list"),
        ("mercury", "accounts.list"),
        ("mercury", "accounts.get"),
        ("mercury", "transactions.list"),
    }
    oauth_context = next(
        context
        for context in recorder.contexts
        if context.operation == "oauth.token.exchange"
    )
    assert oauth_context.tenant_id == str(tenant_id)
    assert oauth_context.installation_id is None


async def test_mercury_start_filters_the_current_transaction_collection() -> None:
    app = build_provider_lab_app(
        fixtures={
            "mercury": [
                {
                    "account-1": {
                        "account": {"id": "account-1"},
                        "transactions": [
                            {
                                "id": "old",
                                "postedAt": "2026-01-01T10:00:00Z",
                            },
                            {
                                "id": "new",
                                "createdAt": "2026-02-03T10:00:00Z",
                            },
                        ],
                        # Supplying ``start`` is a date filter. It must not
                        # switch to the retired mock server's synthetic pool.
                        "delta": [
                            {
                                "id": "synthetic-delta",
                                "createdAt": "2026-02-04T10:00:00Z",
                            },
                        ],
                    },
                },
            ],
        },
    )
    http = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    mercury = MercuryClient(
        base_url="http://provider-lab/mercury",
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        api_token="lab-mercury",
        http_client=http,
        allow_unlimited_local=True,
    )
    try:
        rows, next_offset, total = await mercury.list_transactions(
            "account-1",
            start="2026-02-01",
        )
    finally:
        await http.aclose()

    assert [row["id"] for row in rows] == ["new"]
    assert next_offset is None
    assert total == 1
