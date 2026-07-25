from __future__ import annotations

import httpx
import pytest

from services.ingest.synthetic.fixtures.brex_generator import make_brex
from services.ingest.synthetic.fixtures.carta_generator import make_carta
from services.ingest.synthetic.fixtures.deel_generator import make_deel
from services.ingest.synthetic.fixtures.figma_generator import make_figma
from services.ingest.synthetic.fixtures.fireflies_generator import make_fireflies
from services.ingest.synthetic.fixtures.google_calendar_generator import (
    make_google_calendar,
)
from services.ingest.synthetic.fixtures.google_drive_generator import (
    make_google_drive,
)
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.fixtures.gusto_generator import make_gusto
from services.ingest.synthetic.fixtures.jira_generator import make_jira
from services.ingest.synthetic.fixtures.mercury_generator import make_mercury
from services.ingest.synthetic.fixtures.miro_generator import make_miro
from services.ingest.synthetic.fixtures.quickbooks_generator import (
    make_quickbooks,
)
from services.ingest.synthetic.fixtures.ramp_generator import make_ramp
from services.ingest.synthetic.provider_lab import build_provider_lab_app


WAVE_B_SOURCES = (
    "brex",
    "carta",
    "deel",
    "figma",
    "fireflies",
    "google_calendar",
    "google_drive",
    "grafana",
    "gusto",
    "jira",
    "mercury",
    "miro",
    "quickbooks",
    "ramp",
)


def _transport(app) -> httpx.ASGITransport:
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43124))


def _fixtures() -> dict[str, list[dict]]:
    return {
        "brex": [
            make_brex(
                accounts=2,
                transactions_per_account=3,
                account_kinds=["checking", "card"],
            )
        ],
        "carta": [make_carta(rows_per_entity=3)],
        "deel": [make_deel(contracts=2, payments_per_contract=3)],
        "figma": [make_figma(team_id="team-wave-b", files=1, events=4)],
        "fireflies": [
            make_fireflies(workspace_id="workspace-wave-b", transcripts=4)
        ],
        "google_calendar": [
            make_google_calendar(
                calendars=["calendar@example.test"], events_per_calendar=3
            )
        ],
        "google_drive": [
            make_google_drive(
                files_per_target=3,
                comments_per_file=1,
                revisions_per_file=1,
            )
        ],
        "grafana": [make_grafana(annotations=4)],
        "gusto": [make_gusto(rows_per_entity=3)],
        "jira": [make_jira(projects=1, issues_per_project=3)],
        "mercury": [make_mercury(accounts=1, transactions_per_account=3)],
        "miro": [make_miro(org_id="org-wave-b", boards=1, items_per_board=3)],
        "quickbooks": [make_quickbooks(rows_per_entity=3)],
        "ramp": [make_ramp(rows_per_entity=3)],
    }


async def test_wave_b_generator_fixtures_reach_all_provider_shaped_routes() -> None:
    fixtures = _fixtures()
    app = build_provider_lab_app(fixtures=fixtures)
    carta_id = fixtures["carta"][0]["firm_id"]
    gusto_id = fixtures["gusto"][0]["company_uuid"]

    async with httpx.AsyncClient(
        transport=_transport(app), base_url="http://provider-lab"
    ) as client:
        inventory = await client.get("/_lab/adapters")
        responses = {
            "brex": await client.get("/brex/v2/accounts/cash"),
            "carta": await client.get(
                "/carta/v1alpha1/issuers",
                headers={"Authorization": "Bearer carta-test"},
            ),
            "deel": await client.get("/deel/contracts"),
            "figma": await client.get(
                "/figma/v1/teams/team-wave-b/projects"
            ),
            "fireflies": await client.post(
                "/fireflies/graphql", json={"query": "query { user { id } }"}
            ),
            # These aliases are the paths emitted by endpoint(...) when a
            # the explicit Provider Lab endpoint overrides are configured.
            "google_calendar": await client.get(
                "/gcal/calendar/v3/calendars/calendar%40example.test/events"
            ),
            "google_drive": await client.get("/gdrive/drive/v3/files"),
            "grafana": await client.get("/grafana/api/annotations"),
            "gusto": await client.get(
                f"/gusto/v1/companies/{gusto_id}/employees"
            ),
            "jira": await client.get("/jira/rest/api/3/project/search"),
            "mercury": await client.get("/mercury/accounts"),
            "miro": await client.get("/miro/boards"),
            "quickbooks": await client.get(
                "/quickbooks/v3/company/realm/query",
                params={
                    "query": (
                        "SELECT * FROM Invoice STARTPOSITION 1 MAXRESULTS 1"
                    )
                },
            ),
            "ramp": await client.get("/ramp/business"),
        }

    assert inventory.json()["implemented_count"] == 27
    assert set(responses) == set(WAVE_B_SOURCES)
    assert all(response.status_code == 200 for response in responses.values())
    assert responses["brex"].json()["items"]
    assert responses["carta"].json()["issuers"][0]["id"] == carta_id
    assert responses["deel"].json()["data"]
    assert responses["figma"].json()["projects"][0]["id"] == "mock-project"
    assert (
        responses["fireflies"].json()["data"]["user"]["id"]
        == "workspace-wave-b"
    )
    assert len(responses["google_calendar"].json()["items"]) == 3
    assert len(responses["google_drive"].json()["files"]) == 3
    assert len(responses["grafana"].json()) == 4
    assert len(responses["gusto"].json()) == 3
    assert responses["jira"].json()["values"]
    assert responses["mercury"].json()["accounts"]
    assert responses["miro"].json()["data"]
    assert (
        len(
            responses["quickbooks"].json()["QueryResponse"]["Invoice"]
        )
        == 1
    )
    assert responses["ramp"].json()["id"] == fixtures["ramp"][0]["business_id"]


async def test_wave_b_secondary_declared_surfaces_are_provider_shaped() -> None:
    fixtures = _fixtures()
    app = build_provider_lab_app(fixtures=fixtures)
    carta_id = fixtures["carta"][0]["firm_id"]
    deel_contract = fixtures["deel"][0]["contract_order"][0]
    figma_key = fixtures["figma"][0]["file_order"][0]
    mercury_account = fixtures["mercury"][0]["account_order"][0]
    miro_board = fixtures["miro"][0]["board_order"][0]
    gusto_id = fixtures["gusto"][0]["company_uuid"]
    jira_key = fixtures["jira"][0]["projects"][0]["project_key"]
    drive_target = fixtures["google_drive"][0]["targets"][0]
    drive_file = next(iter(drive_target["extracted_text"]))

    async with httpx.AsyncClient(
        transport=_transport(app), base_url="http://provider-lab"
    ) as client:
        carta_headers = {"Authorization": "Bearer carta-test"}
        calls = {
            "brex_card_accounts": await client.get(
                "/brex/v2/accounts/card"
            ),
            "brex_card_transactions": await client.get(
                "/brex/v2/transactions/card/primary"
            ),
            "carta_issuer": await client.get(
                f"/carta/v1alpha1/issuers/{carta_id}",
                headers=carta_headers,
            ),
            "carta_stakeholders": await client.get(
                f"/carta/v1alpha1/issuers/{carta_id}/stakeholders",
                headers=carta_headers,
            ),
            "carta_share_classes": await client.get(
                f"/carta/v1alpha1/issuers/{carta_id}/shareClasses",
                headers=carta_headers,
            ),
            "carta_notes": await client.get(
                f"/carta/v1alpha1/issuers/{carta_id}/convertibleNotes",
                headers=carta_headers,
            ),
            "deel_contract": await client.get(
                f"/deel/contracts/{deel_contract}"
            ),
            "figma_files": await client.get(
                "/figma/v1/projects/mock-project/files"
            ),
            "figma_file": await client.get(f"/figma/v1/files/{figma_key}"),
            "figma_versions": await client.get(
                f"/figma/v1/files/{figma_key}/versions"
            ),
            "figma_comments": await client.get(
                f"/figma/v1/files/{figma_key}/comments"
            ),
            "gcal_token": await client.post("/gcal/token"),
            "gdrive_token": await client.post("/gdrive/token"),
            "gdrive_start": await client.get(
                "/gdrive/drive/v3/changes/startPageToken"
            ),
            "gdrive_drives": await client.get("/gdrive/drive/v3/drives"),
            "gdrive_export": await client.get(
                f"/gdrive/drive/v3/files/{drive_file}/export"
            ),
            "gdrive_media": await client.get(
                f"/gdrive/drive/v3/files/{drive_file}", params={"alt": "media"}
            ),
            "gdrive_comments": await client.get(
                f"/gdrive/drive/v3/files/{drive_file}/comments"
            ),
            "gdrive_revisions": await client.get(
                f"/gdrive/drive/v3/files/{drive_file}/revisions"
            ),
            "grafana_org": await client.get("/grafana/api/org"),
            "gusto_company": await client.get(
                f"/gusto/v1/companies/{gusto_id}"
            ),
            "jira_count": await client.post(
                "/jira/rest/api/3/search/approximate-count",
                json={"jql": f'project = "{jira_key}"'},
            ),
            "jira_myself": await client.get("/jira/rest/api/3/myself"),
            "mercury_account": await client.get(
                f"/mercury/account/{mercury_account}"
            ),
            "miro_board": await client.get(f"/miro/boards/{miro_board}"),
            "qbo_company": await client.get(
                "/quickbooks/v3/company/realm/companyinfo/realm"
            ),
            "ramp_token": await client.post("/ramp/token"),
            "ramp_reimbursements": await client.get("/ramp/reimbursements"),
            "ramp_cards": await client.get("/ramp/cards"),
            "ramp_users": await client.get("/ramp/users"),
        }

    assert all(response.status_code == 200 for response in calls.values())
    assert calls["brex_card_accounts"].json()["items"]
    assert calls["brex_card_transactions"].json()["items"]
    assert calls["carta_issuer"].json()["issuer"]["id"] == carta_id
    assert calls["carta_stakeholders"].json()["stakeholders"]
    assert calls["carta_share_classes"].json()["shareClasses"]
    assert calls["carta_notes"].json()["convertibleNotes"]
    assert calls["deel_contract"].json()["data"]["id"] == deel_contract
    assert calls["figma_files"].json()["files"][0]["key"] == figma_key
    assert calls["figma_file"].json()["key"] == figma_key
    assert calls["figma_versions"].json()["pagination"]["next_page"] is None
    assert "comments" in calls["figma_comments"].json()
    assert calls["gcal_token"].json()["access_token"] == "sandbox-access-token"
    assert calls["gdrive_token"].json()["access_token"] == "sandbox-access-token"
    assert calls["gdrive_start"].json()["startPageToken"]
    assert "drives" in calls["gdrive_drives"].json()
    assert calls["gdrive_export"].text
    assert calls["gdrive_media"].content == calls["gdrive_export"].content
    assert calls["gdrive_comments"].json()["comments"]
    assert calls["gdrive_revisions"].json()["revisions"]
    assert calls["grafana_org"].json()["name"] == "Sandbox Org"
    assert calls["gusto_company"].json()["uuid"] == gusto_id
    assert calls["jira_count"].json()["count"] == 3
    assert calls["jira_myself"].json()["accountId"] == "sandbox-account"
    assert calls["mercury_account"].json()["id"] == mercury_account
    assert calls["miro_board"].json()["id"] == miro_board
    assert calls["qbo_company"].json()["CompanyInfo"]["CompanyName"] == "Sandbox Co"
    assert calls["ramp_token"].json()["access_token"] == "mock-ramp-access-token"
    assert calls["ramp_reimbursements"].json()["data"]
    assert calls["ramp_cards"].json()["data"]
    assert calls["ramp_users"].json()["data"]


async def test_cursor_token_and_header_pagination_matches_legacy_mocks() -> None:
    fixtures = _fixtures()
    app = build_provider_lab_app(fixtures=fixtures)
    brex_account = fixtures["brex"][0]["account_order"][0]
    carta_id = fixtures["carta"][0]["firm_id"]
    deel_contract = fixtures["deel"][0]["contract_order"][0]
    gusto_id = fixtures["gusto"][0]["company_uuid"]
    jira_key = fixtures["jira"][0]["projects"][0]["project_key"]
    mercury_account = fixtures["mercury"][0]["account_order"][0]
    miro_board = fixtures["miro"][0]["board_order"][0]

    async with httpx.AsyncClient(
        transport=_transport(app), base_url="http://provider-lab"
    ) as client:
        brex_page = await client.get(
            f"/brex/v2/transactions/cash/{brex_account}",
            params={"limit": 1},
        )
        brex_next = await client.get(
            f"/brex/v2/transactions/cash/{brex_account}",
            params={"limit": 1, "cursor": brex_page.json()["next_cursor"]},
        )
        carta_page = await client.get(
            f"/carta/v1alpha1/issuers/{carta_id}/optionGrants",
            params={"pageSize": 1},
            headers={"Authorization": "Bearer carta-test"},
        )
        carta_next = await client.get(
            f"/carta/v1alpha1/issuers/{carta_id}/optionGrants",
            params={
                "pageSize": 1,
                "pageToken": carta_page.json()["nextPageToken"],
            },
            headers={"Authorization": "Bearer carta-test"},
        )
        deel_page = await client.get(
            "/deel/invoices",
            params={"contract_id": deel_contract, "limit": 1, "offset": 1},
        )
        fireflies_page = await client.post(
            "/fireflies/graphql",
            json={
                "query": "query($skip:Int,$limit:Int){ transcripts { id } }",
                "variables": {"skip": 1, "limit": 1},
            },
        )
        gusto_page = await client.get(
            f"/gusto/v1/companies/{gusto_id}/employees",
            params={"page": 2, "per": 1},
        )
        jira_page = await client.post(
            "/jira/rest/api/3/search/jql",
            json={
                "jql": f'project = "{jira_key}" ORDER BY updated ASC',
                "maxResults": 1,
            },
        )
        jira_next = await client.post(
            "/jira/rest/api/3/search/jql",
            json={
                "jql": f'project = "{jira_key}" ORDER BY updated ASC',
                "maxResults": 1,
                "nextPageToken": jira_page.json()["nextPageToken"],
            },
        )
        mercury_page = await client.get(
            f"/mercury/account/{mercury_account}/transactions",
            params={"limit": 1, "offset": 1},
        )
        miro_page = await client.get(
            f"/miro/boards/{miro_board}/items", params={"limit": 1}
        )
        miro_next = await client.get(
            f"/miro/boards/{miro_board}/items",
            params={"limit": 1, "cursor": miro_page.json()["cursor"]},
        )
        quickbooks_page = await client.get(
            "/quickbooks/v3/company/realm/query",
            params={
                "query": (
                    "SELECT * FROM Invoice STARTPOSITION 2 MAXRESULTS 1"
                )
            },
        )
        ramp_page = await client.get(
            "/ramp/transactions", params={"page_size": 2}
        )
        ramp_next = await client.get(ramp_page.json()["page"]["next"])

    assert brex_page.json()["items"] != brex_next.json()["items"]
    assert carta_page.json()["optionGrants"] != carta_next.json()["optionGrants"]
    assert deel_page.json()["page"]["total_rows"] == 3
    assert len(deel_page.json()["data"]) == 1
    assert len(fireflies_page.json()["data"]["transcripts"]) == 1
    assert gusto_page.headers["x-total-count"] == "3"
    assert gusto_page.headers["x-page"] == "2"
    assert jira_page.json()["issues"] != jira_next.json()["issues"]
    assert len(mercury_page.json()["transactions"]) == 1
    assert miro_page.json()["data"] != miro_next.json()["data"]
    assert (
        quickbooks_page.json()["QueryResponse"]["Invoice"][0]
        == fixtures["quickbooks"][0]["entities"]["Invoice"][1]
    )
    assert ramp_page.json()["page"]["next"].startswith(
        "http://provider-lab/ramp/transactions?"
    )
    assert len(ramp_next.json()["data"]) == 1


async def test_provider_filters_delta_modes_and_expired_google_tokens() -> None:
    fixtures = _fixtures()
    calendar_event = fixtures["google_calendar"][0]["events"][
        "calendar@example.test"
    ][0]
    fixtures["google_calendar"][0]["delta"][
        "calendar@example.test"
    ] = [{**calendar_event, "id": "delta-calendar-event"}]
    drive_target = fixtures["google_drive"][0]["targets"][0]
    drive_target["changes"] = [
        {"fileId": "file-1", "removed": False},
        {"fileId": "file-2", "removed": True},
    ]
    grafana_times = sorted(
        item["time"] for item in fixtures["grafana"][0]["annotations"]
    )
    gusto_id = fixtures["gusto"][0]["company_uuid"]

    async with httpx.AsyncClient(
        transport=_transport(build_provider_lab_app(fixtures=fixtures)),
        base_url="http://provider-lab",
    ) as client:
        calendar_delta = await client.get(
            "/gcal/calendar/v3/calendars/calendar%40example.test/events",
            params={"syncToken": "sync-1"},
        )
        calendar_expired = await client.get(
            "/gcal/calendar/v3/calendars/calendar%40example.test/events",
            params={"syncToken": "EXPIRED"},
        )
        drive_delta = await client.get(
            "/gdrive/drive/v3/changes",
            params={"pageToken": "spt-1", "pageSize": 1},
        )
        drive_expired = await client.get(
            "/gdrive/drive/v3/changes", params={"pageToken": "EXPIRED"}
        )
        grafana_window = await client.get(
            "/grafana/api/annotations",
            params={"from": grafana_times[1], "to": grafana_times[-1], "limit": 2},
        )
        payrolls = fixtures["gusto"][0]["entities"]["payroll"]
        payroll_window = await client.get(
            f"/gusto/v1/companies/{gusto_id}/payrolls",
            params={
                "start_date": payrolls[1]["check_date"],
                "end_date": payrolls[2]["check_date"],
            },
        )

    assert calendar_delta.json()["items"][0]["id"] == "delta-calendar-event"
    assert calendar_expired.status_code == 410
    assert len(drive_delta.json()["changes"]) == 1
    assert drive_expired.status_code == 410
    assert len(grafana_window.json()) == 2
    assert [row["check_date"] for row in payroll_window.json()] == [
        payrolls[1]["check_date"],
        payrolls[2]["check_date"],
    ]


async def test_auth_and_provider_errors_are_not_converted_to_generic_success() -> None:
    fixtures = _fixtures()
    carta_id = fixtures["carta"][0]["firm_id"]
    app = build_provider_lab_app(fixtures=fixtures)

    async with httpx.AsyncClient(
        transport=_transport(app), base_url="http://provider-lab"
    ) as client:
        carta_token = await client.post("/carta/o/access_token/")
        carta_token_no_slash = await client.post("/carta/o/access_token")
        carta_unauthorized = await client.get("/carta/v1alpha1/issuers")
        carta_bad_token = await client.get(
            f"/carta/v1alpha1/issuers/{carta_id}/optionGrants",
            params={"pageToken": "broken"},
            headers={"Authorization": "Bearer carta-test"},
        )
        missing_deel = await client.get("/deel/contracts/missing")
        missing_figma = await client.get("/figma/v1/files/missing")
        invalid_graphql = await client.post(
            "/fireflies/graphql",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        missing_mercury = await client.get("/mercury/account/missing")
        missing_miro = await client.get("/miro/boards/missing")

    assert carta_token.json()["access_token"] == "mock-carta-access-token"
    assert carta_token_no_slash.json() == carta_token.json()
    assert carta_unauthorized.status_code == 401
    assert carta_bad_token.status_code == 400
    assert missing_deel.status_code == 404
    assert missing_figma.status_code == 404
    assert invalid_graphql.status_code == 400
    assert missing_mercury.status_code == 404
    assert missing_miro.status_code == 404


@pytest.mark.parametrize("source", WAVE_B_SOURCES)
async def test_wave_b_rejects_undeclared_routes_strictly(source: str) -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app), base_url="http://provider-lab"
    ) as client:
        response = await client.get(f"/{source}/definitely-not-implemented")

    assert response.status_code == 501
    assert response.json()["error"]["code"] == "unsupported_provider_route"


@pytest.mark.parametrize(
    ("path", "method"),
    [
        ("/figma/v1/variables/local", "GET"),
    ],
)
async def test_routes_outside_the_pinned_used_surface_remain_explicit_501(
    path: str, method: str
) -> None:
    app = build_provider_lab_app()
    async with httpx.AsyncClient(
        transport=_transport(app), base_url="http://provider-lab"
    ) as client:
        response = await client.request(method, path)

    assert response.status_code == 501
    assert response.json()["error"]["code"] == "unsupported_provider_route"
