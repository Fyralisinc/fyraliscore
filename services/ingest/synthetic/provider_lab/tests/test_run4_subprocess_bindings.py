"""Regression coverage for Provider Lab bindings used by spawned Run 4 workers."""

from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from lib.integrations.provider_lab import provider_lab_endpoint_overrides
from services.ingest.ingestion.fetchers import _clients as builders
from services.ingest.synthetic.fixtures.ashby_generator import make_ashby
from services.ingest.synthetic.fixtures.aws_generator import make_aws
from services.ingest.synthetic.fixtures.carta_generator import make_carta
from services.ingest.synthetic.fixtures.discord_generator import (
    make_discord_guild,
)
from services.ingest.synthetic.fixtures.github_generator import (
    make_github_repos,
)
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.fixtures.hibob_generator import make_hibob
from services.ingest.synthetic.fixtures.signal_generator import make_signal
from services.ingest.synthetic.fixtures.telegram_generator import make_telegram
from services.ingest.synthetic.provider_lab.server import ProviderLabServer


pytestmark = pytest.mark.asyncio


async def test_spawned_worker_client_boundaries_use_provider_lab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account_id = "123456789012"
    telegram_fixture = make_telegram(
        dialogs=1,
        messages_per_dialog=3,
        seed="run4",
    )
    signal_fixture = make_signal(
        threads=1,
        messages_per_thread=3,
        seed="run4",
    )
    ashby_fixture = make_ashby(
        org_id="run4-org",
        entities=[
            "candidate",
            "interview_stage_group",
            "job_posting",
        ],
    )
    github_fixture = make_github_repos(
        org_or_user="run4-owner",
        repos=1,
        events_per_repo=2,
        installation_id="run4-gh",
    )
    discord_fixture = make_discord_guild(
        guild_id="run4-guild",
        channels=1,
        messages_per_channel=2,
    )
    carta_issuer_ids = (
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
    )
    grafana_instances = (
        ("https://grafana-a.provider-lab.test", 2),
        ("https://grafana-b.provider-lab.test", 3),
    )
    hibob_companies = (
        ("hibob-company-a", 2),
        ("hibob-company-b", 3),
    )
    fixtures = {
        "aws": [make_aws(account_id=account_id, events=3, per_page=2)],
        "telegram": [telegram_fixture],
        "signal": [signal_fixture],
        "ashby": [ashby_fixture],
        "github": [github_fixture],
        "discord": [discord_fixture],
        "carta": [
            make_carta(firm_id=issuer_id, entities=["stakeholder"])
            for issuer_id in carta_issuer_ids
        ],
        "grafana": [
            make_grafana(base_url=base_url, annotations=annotation_count)
            for base_url, annotation_count in grafana_instances
        ],
        "hibob": [
            make_hibob(
                company_id=company_id,
                rows_per_entity=row_count,
                seed=company_id,
            )
            for company_id, row_count in hibob_companies
        ],
    }

    with ProviderLabServer(fixtures) as lab:
        monkeypatch.setenv("PROVIDER_LAB_URL", lab.base_url)
        for key, value in provider_lab_endpoint_overrides(lab.base_url).items():
            monkeypatch.setenv(key, value)

        http = httpx.AsyncClient(timeout=30)
        monkeypatch.setattr(builders, "_HTTP", http)
        monkeypatch.setattr(builders, "_GITHUB_CLIENTS", {})
        monkeypatch.setattr(
            builders,
            "_provider_transport_kwargs",
            lambda: {"allow_unlimited_local": True},
        )
        tenant_id = uuid4()
        clients: list[object] = []
        carta_results: list[
            tuple[list[dict[str, object]], list[dict[str, object]]]
        ] = []
        grafana_results: list[list[dict[str, object]]] = []
        hibob_results: list[list[dict[str, object]]] = []
        try:
            aws = await builders.build_aws_client(
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "account_id": account_id,
                    "region": "us-east-1",
                    # Lab mode must override this without mutating the row.
                    "credential_kind": "assume_role",
                    "secret_ref": None,
                }
            )
            clients.append(aws)
            aws_page = await aws.list_events(
                account_id=account_id,
                region="us-east-1",
                limit=2,
            )

            telegram = await builders.build_telegram_client(
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "account_label": "run4-telegram",
                    "api_id": None,
                    "api_hash_secret_ref": None,
                    "session_secret_ref": None,
                    "backfill_session_secret_ref": None,
                }
            )
            clients.append(telegram)
            telegram_page = await telegram.get_history(
                dialog_id=telegram_fixture["dialog_order"][0],
                access_hash=None,
                dialog_kind="chat",
                limit=2,
            )

            signal = await builders.build_signal_client(
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "account_label": "run4-signal",
                    "session_secret_ref": None,
                    "backfill_session_secret_ref": None,
                }
            )
            clients.append(signal)
            signal_page = await signal.get_history(
                thread_id=signal_fixture["thread_order"][0],
                thread_kind="direct",
                limit=2,
            )

            ashby = await builders.build_ashby_client(
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "base_url": "https://api.ashbyhq.com",
                    "org_id": "run4-org",
                    "secret_ref": None,
                }
            )
            clients.append(ashby)
            ashby_page = await ashby.list_entities("candidate")
            ashby_nonincremental_pages = [
                await ashby.list_entities(
                    entity_type,
                    sync_token="must-not-be-sent",
                )
                for entity_type in (
                    "interview_stage_group",
                    "job_posting",
                )
            ]

            github = await builders.build_github_client(
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "installation_id": "run4-gh",
                }
            )
            clients.append(github)
            owner, repo = github_fixture["repos"][0]["full_name"].split(
                "/",
                1,
            )
            comments = await github.list_repo_events(
                owner=owner,
                repo=repo,
                event_type="issue_comments",
                installation_id="run4-gh",
            )
            commits = await github.list_repo_events(
                owner=owner,
                repo=repo,
                event_type="commits",
                installation_id="run4-gh",
            )
            reviews = await github.list_pr_reviews(
                owner=owner,
                repo=repo,
                pull_number=1,
                installation_id="run4-gh",
            )

            discord = await builders.build_discord_client(
                {
                    "id": uuid4(),
                    "tenant_id": tenant_id,
                    "installation_id": "run4-guild",
                }
            )
            clients.append(discord)
            active_threads = await discord.list_active_guild_threads("run4-guild")
            archived_threads = await discord.list_channel_archived_threads(
                discord_fixture["channels"][0]["id"],
                archive_kind="public",
            )

            for issuer_id in carta_issuer_ids:
                carta = await builders.build_carta_client(
                    {
                        "id": uuid4(),
                        "tenant_id": tenant_id,
                        "base_url": "https://api.carta.com",
                        "firm_id": issuer_id,
                        "secret_ref": None,
                        "refresh_secret_ref": None,
                    }
                )
                clients.append(carta)
                issuers, _ = await carta.list_issuers()
                stakeholders, _ = await carta.list_stakeholders()
                carta_results.append((issuers, stakeholders))

            for base_url, _annotation_count in grafana_instances:
                grafana = await builders.build_grafana_client(
                    {
                        "id": uuid4(),
                        "tenant_id": tenant_id,
                        "base_url": base_url,
                        "secret_ref": None,
                    }
                )
                clients.append(grafana)
                grafana_results.append(await grafana.list_annotations())

            for index, (company_id, _row_count) in enumerate(hibob_companies):
                hibob = await builders.build_hibob_client(
                    {
                        "id": uuid4(),
                        "tenant_id": tenant_id,
                        "base_url": "https://api.hibob.com",
                        "company_id": company_id,
                        "service_user_id": f"service-user-{index}",
                        "secret_ref": None,
                    }
                )
                clients.append(hibob)
                rows, _ = await hibob.list_entities("employee")
                hibob_results.append(rows)
        finally:
            for client in clients:
                close = getattr(client, "aclose", None)
                if close is not None:
                    await close()
            await http.aclose()

    assert len(aws_page["events"]) == 2
    assert len(telegram_page[0]) == 2
    assert len(signal_page[0]) == 2
    assert len(ashby_page[0]) == 1
    assert [
        (len(rows), next_cursor, next_sync_token)
        for rows, next_cursor, next_sync_token in ashby_nonincremental_pages
    ] == [(1, None, None), (1, None, None)]
    assert [len(comments[0]), len(commits[0]), len(reviews[0])] == [0, 0, 0]
    assert active_threads == []
    assert archived_threads == []
    assert [
        (
            [issuer["id"] for issuer in issuers],
            [stakeholder["issuerId"] for stakeholder in stakeholders],
        )
        for issuers, stakeholders in carta_results
    ] == [([issuer_id], [issuer_id]) for issuer_id in carta_issuer_ids]
    assert [len(rows) for rows in grafana_results] == [2, 3]
    assert [len(rows) for rows in hibob_results] == [2, 3]
    assert {row["displayName"] for rows in hibob_results for row in rows} == {
        row["displayName"]
        for company_id, row_count in hibob_companies
        for row in make_hibob(
            company_id=company_id,
            rows_per_entity=row_count,
            seed=company_id,
        )["entities"]["employee"]
    }


async def test_aws_sigv4_scope_isolates_multiple_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    account_ids = ("123456789012", "210987654321")
    fixtures = {
        "aws": [make_aws(account_id=account_id, events=2) for account_id in account_ids]
    }

    with ProviderLabServer(fixtures) as lab:
        monkeypatch.setenv("PROVIDER_LAB_URL", lab.base_url)
        for key, value in provider_lab_endpoint_overrides(lab.base_url).items():
            monkeypatch.setenv(key, value)

        http = httpx.AsyncClient(timeout=30)
        monkeypatch.setattr(builders, "_HTTP", http)
        monkeypatch.setattr(
            builders,
            "_provider_transport_kwargs",
            lambda: {"allow_unlimited_local": True},
        )
        clients: list[object] = []
        pages = []
        try:
            for account_id in account_ids:
                client = await builders.build_aws_client(
                    {
                        "id": uuid4(),
                        "tenant_id": uuid4(),
                        "account_id": account_id,
                        "region": "us-east-1",
                        "credential_kind": "assume_role",
                        "secret_ref": None,
                    }
                )
                clients.append(client)
                pages.append(
                    await client.list_events(
                        account_id=account_id,
                        region="us-east-1",
                    )
                )
        finally:
            for client in clients:
                await client.aclose()  # type: ignore[attr-defined]
            await http.aclose()

    assert [len(page["events"]) for page in pages] == [2, 2]
    assert [{event["EventId"] for event in page["events"]} for page in pages] == [
        {event["eventId"] for event in fixture["events"]} for fixture in fixtures["aws"]
    ]
