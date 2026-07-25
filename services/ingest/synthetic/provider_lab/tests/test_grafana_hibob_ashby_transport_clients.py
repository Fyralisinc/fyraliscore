"""Real Grafana, HiBob, and Ashby clients against Provider Lab."""
from __future__ import annotations

from uuid import uuid4

import httpx

from lib.shared.provider_transport import RequestContext
from services.ingest.integrations.ashby.client import AshbyClient
from services.ingest.integrations.grafana.client import GrafanaClient
from services.ingest.integrations.hibob.client import HibobClient
from services.ingest.synthetic.fixtures.ashby_generator import make_ashby
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.fixtures.hibob_generator import make_hibob
from services.ingest.synthetic.provider_lab import build_provider_lab_app


def _transport(app) -> httpx.ASGITransport:  # noqa: ANN001
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43128))


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        self.contexts.append(context)
        return await call()


async def test_real_clients_cover_the_complete_used_lab_surface() -> None:
    grafana_fixture = make_grafana(annotations=2)
    hibob_fixture = make_hibob(
        company_id="hibob-production-client",
        rows_per_entity=2,
        page_size=1,
    )
    ashby_fixture = make_ashby(
        org_id="ashby-production-client",
        entities=["candidate"],
        rows_per_entity=2,
        page_size=1,
    )
    app = build_provider_lab_app(
        fixtures={
            "grafana": [grafana_fixture],
            "hibob": [hibob_fixture],
            "ashby": [ashby_fixture],
        },
    )
    http = httpx.AsyncClient(
        transport=_transport(app),
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
    grafana = GrafanaClient(
        base_url="http://provider-lab/grafana",
        api_token="lab-grafana",
        installation_row_id=uuid4(),
        **common,
    )
    hibob = HibobClient(
        base_url="http://provider-lab/hibob",
        company_id=hibob_fixture["company_id"],
        service_user_id=hibob_fixture["company_id"],
        token="lab-hibob",
        installation_row_id=uuid4(),
        **common,
    )
    ashby = AshbyClient(
        base_url="http://provider-lab/ashby",
        org_id=ashby_fixture["org_id"],
        api_key=f"lab-ashby--{ashby_fixture['org_id']}",
        installation_row_id=uuid4(),
        **common,
    )

    try:
        annotations = await grafana.list_annotations(limit=1)
        org = await grafana.get_org()
        company = await hibob.company_info()
        timeoff, _ = await hibob.list_entities(
            "timeoff",
            modified_since="2025-01-01T00:00:00Z",
        )
        salaries, salary_cursor = await hibob.list_entities(
            "payroll",
            limit=1,
        )
        work, work_cursor = await hibob.list_entities(
            "lifecycle",
            limit=1,
        )
        candidates, candidate_cursor, _ = await ashby.list_entities(
            "candidate",
            limit=1,
        )
        candidate = await ashby.get_entity(
            "candidate",
            candidates[0]["id"],
        )
    finally:
        await http.aclose()

    assert len(annotations) == 1
    assert org["name"] == "Sandbox Org"
    assert company["id"] == hibob_fixture["company_id"]
    assert timeoff
    assert len(salaries) == 1 and salary_cursor is not None
    assert len(work) == 1 and work_cursor is not None
    assert len(candidates) == 1 and candidate_cursor is not None
    assert candidate["id"] == candidates[0]["id"]

    assert {
        source: {
            context.operation
            for context in recorder.contexts
            if context.source == source
        }
        for source in ("grafana", "hibob", "ashby")
    } == {
        "grafana": {"annotations.list", "org.get"},
        "hibob": {
            "people.search",
            "timeoff.changes.list",
            "people.salaries.list",
            "people.work.list",
        },
        "ashby": {"entities.list", "entities.info"},
    }
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id is not None
        for context in recorder.contexts
    )
