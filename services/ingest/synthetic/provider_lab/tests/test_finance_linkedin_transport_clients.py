"""Real finance/LinkedIn clients against the strict Provider Lab surfaces."""
from __future__ import annotations

from uuid import uuid4

import httpx

from lib.shared.provider_transport import RequestContext
from services.ingest.integrations.gusto.client import GustoClient
from services.ingest.integrations.linkedin.client import LinkedinClient
from services.ingest.integrations.quickbooks.client import QuickBooksClient
from services.ingest.integrations.ramp.client import RampClient
from services.ingest.synthetic.fixtures.gusto_generator import make_gusto
from services.ingest.synthetic.fixtures.linkedin_generator import make_linkedin
from services.ingest.synthetic.fixtures.quickbooks_generator import (
    make_quickbooks,
)
from services.ingest.synthetic.fixtures.ramp_generator import make_ramp
from services.ingest.synthetic.provider_lab import build_provider_lab_app


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        self.contexts.append(context)
        return await call()


async def test_real_clients_run_unmodified_against_provider_lab() -> None:
    quickbooks = make_quickbooks(rows_per_entity=2)
    ramp = make_ramp(rows_per_entity=2)
    gusto = make_gusto(rows_per_entity=2)
    linkedin = make_linkedin(
        organization_urn="urn:li:organization:123",
        rows_per_entity=2,
    )
    app = build_provider_lab_app(
        fixtures={
            "quickbooks": [quickbooks],
            "ramp": [ramp],
            "gusto": [gusto],
            "linkedin": [linkedin],
        },
    )
    http = httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=app,
            client=("127.0.0.1", 43127),
        ),
        base_url="http://provider-lab",
    )
    tenant_id = uuid4()
    recorder = _Recorder()
    common = {
        "tenant_id": tenant_id,
        "http_client": http,
        "provider_transport": recorder,
        "allow_unlimited_local": True,
    }
    qbo_client = QuickBooksClient(
        base_url="http://provider-lab/quickbooks",
        realm_id=quickbooks["realm_id"],
        install_row_id=uuid4(),
        access_token="lab-quickbooks",
        **common,
    )
    ramp_client = RampClient(
        base_url="http://provider-lab/ramp",
        business_id=ramp["business_id"],
        install_row_id=uuid4(),
        client_id="lab-client",
        client_secret="lab-secret",
        **common,
    )
    gusto_client = GustoClient(
        base_url="http://provider-lab/gusto",
        company_uuid=gusto["company_uuid"],
        install_row_id=uuid4(),
        access_token="lab-gusto",
        **common,
    )
    linkedin_client = LinkedinClient(
        base_url="http://provider-lab/linkedin",
        organization_urn=linkedin["organization_urn"],
        install_row_id=uuid4(),
        access_token="lab-linkedin",
        **common,
    )

    try:
        invoices, _ = await qbo_client.query("Invoice")
        qbo_company = await qbo_client.company_info()

        await ramp_client.mint_token()
        transactions, _ = await ramp_client.list_transactions()
        reimbursements, _ = await ramp_client.list_reimbursements()
        cards, _ = await ramp_client.list_cards()
        users, _ = await ramp_client.list_users()
        ramp_business = await ramp_client.business()

        employees, _ = await gusto_client.list_employees()
        payrolls, _ = await gusto_client.list_payrolls()
        gusto_company = await gusto_client.company()

        posts, _ = await linkedin_client.list_posts()
        share_statistics = await linkedin_client.share_statistics()
        follower_statistics = await linkedin_client.follower_statistics()
        linkedin_org = await linkedin_client.get_organization()
    finally:
        await http.aclose()

    assert len(invoices) == 2
    assert qbo_company["CompanyInfo"]["CompanyName"] == "Sandbox Co"
    assert len(transactions) == 2
    assert len(reimbursements) == 2
    assert len(cards) == 2
    assert len(users) == 2
    assert ramp_business["id"] == ramp["business_id"]
    assert len(employees) == 2
    assert len(payrolls) == 2
    assert gusto_company["uuid"] == gusto["company_uuid"]
    assert len(posts) == 2
    assert len(share_statistics) == 2
    assert len(follower_statistics) == 2
    assert str(linkedin_org["id"]) == "123"

    operations_by_source = {
        source: {
            context.operation
            for context in recorder.contexts
            if context.source == source
        }
        for source in ("quickbooks", "ramp", "gusto", "linkedin")
    }
    assert operations_by_source == {
        "quickbooks": {"entities.query", "company_info.get"},
        "ramp": {
            "oauth.token.mint",
            "transactions.list",
            "reimbursements.list",
            "cards.list",
            "users.list",
            "business.get",
        },
        "gusto": {"employees.list", "payrolls.list", "companies.get"},
        "linkedin": {
            "posts.list",
            "share_statistics.list",
            "follower_statistics.list",
            "organizations.get",
        },
    }
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id is not None
        for context in recorder.contexts
    )
