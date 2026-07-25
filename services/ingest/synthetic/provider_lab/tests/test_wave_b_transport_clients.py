"""Real Wave-B clients against the Provider Lab used API surface."""
from __future__ import annotations

from uuid import uuid4

import httpx

from lib.shared.provider_transport import RequestContext
from services.ingest.integrations.brex.client import BrexClient
from services.ingest.integrations.carta.client import CartaClient
from services.ingest.integrations.deel.client import DeelClient
from services.ingest.integrations.fireflies.client import FirefliesClient
from services.ingest.synthetic.fixtures.brex_generator import make_brex
from services.ingest.synthetic.fixtures.carta_generator import make_carta
from services.ingest.synthetic.fixtures.deel_generator import make_deel
from services.ingest.synthetic.fixtures.fireflies_generator import (
    make_fireflies,
)
from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _transport(app) -> httpx.ASGITransport:  # noqa: ANN001
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43126))


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        self.contexts.append(context)
        return await call()


async def test_brex_carta_deel_and_fireflies_clients_run_against_lab() -> None:
    brex = make_brex(
        accounts=2,
        transactions_per_account=2,
        account_kinds=["checking", "card"],
    )
    carta = make_carta(rows_per_entity=2)
    deel = make_deel(contracts=1, payments_per_contract=2)
    fireflies = make_fireflies(
        workspace_id="workspace-production-client",
        transcripts=2,
    )
    app = build_provider_lab_app(
        fixtures={
            "brex": [brex],
            "carta": [carta],
            "deel": [deel],
            "fireflies": [fireflies],
        },
    )
    http = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    tenant_id = uuid4()
    recorder = _Recorder()
    transport_kwargs = {
        "provider_transport": recorder,
        "allow_unlimited_local": True,
    }
    clients = (
        BrexClient(
            base_url="http://provider-lab/brex",
            tenant_id=tenant_id,
            installation_row_id=uuid4(),
            api_token="lab-brex",
            http_client=http,
            **transport_kwargs,
        ),
        CartaClient(
            base_url="http://provider-lab/carta",
            issuer_id=carta["firm_id"],
            tenant_id=tenant_id,
            install_row_id=uuid4(),
            access_token="lab-carta",
            http_client=http,
            **transport_kwargs,
        ),
        DeelClient(
            base_url="http://provider-lab/deel",
            tenant_id=tenant_id,
            installation_row_id=uuid4(),
            api_token="lab-deel",
            http_client=http,
            **transport_kwargs,
        ),
        FirefliesClient(
            base_url="http://provider-lab/fireflies",
            tenant_id=tenant_id,
            installation_row_id=uuid4(),
            api_token="lab-fireflies",
            http_client=http,
            **transport_kwargs,
        ),
    )
    brex_client, carta_client, deel_client, fireflies_client = clients

    try:
        accounts = await brex_client.list_accounts()
        cash_account = next(
            account
            for account in accounts
            if account["_fyralis_account_kind"] == "cash"
        )
        card_account = next(
            account
            for account in accounts
            if account["_fyralis_account_kind"] == "card"
        )
        brex_transactions, brex_next, _ = (
            await brex_client.list_transactions(
                cash_account["id"],
                account_kind="cash",
                limit=1,
            )
        )
        await brex_client.list_transactions(
            card_account["id"],
            account_kind="card",
            limit=1,
        )

        issuers, issuer_cursor = await carta_client.list_issuers()
        await carta_client.get_issuer()
        await carta_client.list_stakeholders(page_size=1)
        await carta_client.list_share_classes(page_size=1)
        option_grants, option_cursor = await carta_client.list_option_grants(
            page_size=1,
        )
        await carta_client.list_convertible_notes(page_size=1)

        contracts = await deel_client.list_contracts()
        await deel_client.get_contract(contracts[0]["id"])
        invoices, invoice_cursor, _ = await deel_client.list_payments(
            contracts[0]["id"],
            limit=1,
        )

        workspace = await fireflies_client.get_workspace()
        transcripts, transcript_cursor = (
            await fireflies_client.list_transcripts_graphql(limit=1)
        )
        transcript = await fireflies_client.get_transcript(
            transcripts[0]["id"],
        )
    finally:
        await http.aclose()

    assert len(accounts) == 2
    assert len(brex_transactions) == 2
    assert brex_next is None
    assert issuers[0]["id"] == carta["firm_id"]
    assert issuer_cursor is None
    assert len(option_grants) == 1
    assert option_cursor == "off:1"
    assert contracts[0]["id"] == deel["contract_order"][0]
    assert len(invoices) == 1
    assert invoice_cursor == 1
    assert workspace["workspace_id"] == "workspace-production-client"
    assert len(transcripts) == 1
    assert transcript_cursor == 1
    assert transcript["id"] == transcripts[0]["id"]
    operations_by_source = {
        source: {
            context.operation
            for context in recorder.contexts
            if context.source == source
        }
        for source in ("brex", "carta", "deel", "fireflies")
    }
    assert operations_by_source == {
        "brex": {
            "accounts.cash.list",
            "accounts.card.list",
            "transactions.cash.list",
            "transactions.card.list",
        },
        "carta": {
            "issuers.list",
            "issuers.get",
            "stakeholders.list",
            "share_classes.list",
            "option_grants.list",
            "convertible_notes.list",
        },
        "deel": {
            "contracts.list",
            "contracts.get",
            "invoices.list",
        },
        "fireflies": {
            "user.get",
            "transcripts.list",
            "transcript.get",
        },
    }
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id is not None
        for context in recorder.contexts
    )
