"""Fixture-realism behavioral pre-flight (A29 / Decision 12).

M6.7 verification surfaced THREE fixture-realism gaps in a row —
synthetic fixtures missing fields real provider responses always carry
(gmail Message-ID, github node_id) or producing timestamps outside the
`observations` partition coverage (slack/gmail 2023 base). Each gap was
invisible until a full run executed.

This pre-flight is the structural defense: a fast, fail-fast gate that
runs BEFORE a 90-minute validation run. For each source it exercises
the REAL path — drive the source's actual backfill fetcher against its
mock client, then run the emitted record through the REAL handler
(mirroring shard_fetch's `webhook_metadata` lift + the normalizer's
blob-unwrap + dispatch) — and asserts:

  1. the handler returns a draft WITHOUT raising  (catches missing
     required fields, e.g. gmail Message-ID),
  2. `draft.external_id` is non-null               (catches missing
     dedup-key fields, e.g. github node_id),
  3. `draft.occurred_at` falls within the live `observations` partition
     coverage                                       (catches out-of-
     range fixture timestamps, e.g. the 2023 base).

The check is BEHAVIORAL (runs the code), not a static scan of which
fields a handler reads — handlers read fields conditionally / nested /
from headers, so static extraction would be brittle and rot. Running
the real fetcher+handler is the only robust signal.
"""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import asyncpg

from services.ingest.ingestion.fetchers import aws as _aws_fetcher
from services.ingest.ingestion.fetchers import brex as _brex_fetcher
from services.ingest.ingestion.fetchers import carta as _carta_fetcher
from services.ingest.ingestion.fetchers import deel as _deel_fetcher
from services.ingest.ingestion.fetchers import discord as _discord_fetcher
from services.ingest.ingestion.fetchers import figma as _figma_fetcher
from services.ingest.ingestion.fetchers import fireflies as _fireflies_fetcher
from services.ingest.ingestion.fetchers import github as _github_fetcher
from services.ingest.ingestion.fetchers import gmail as _gmail_fetcher
from services.ingest.ingestion.fetchers import gusto as _gusto_fetcher
from services.ingest.ingestion.fetchers import hibob as _hibob_fetcher
from services.ingest.ingestion.fetchers import miro as _miro_fetcher
from services.ingest.ingestion.fetchers import ramp as _ramp_fetcher
from services.ingest.ingestion.fetchers import signal as _signal_fetcher
from services.ingest.ingestion.fetchers import slack as _slack_fetcher
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel
from services.ingest.synthetic.fixtures import (
    make_aws,
    make_brex,
    make_carta,
    make_deel,
    make_discord_guild,
    make_figma,
    make_fireflies,
    make_github_repos,
    make_gmail_mailbox,
    make_gusto,
    make_hibob,
    make_miro,
    make_ramp,
    make_signal,
    make_slack_workspace,
)
from services.ingest.synthetic.mock_clients import (
    MockAwsClient,
    MockBrexClient,
    MockCartaClient,
    MockDeelClient,
    MockDiscordClient,
    MockFigmaClient,
    MockFirefliesClient,
    MockGithubClient,
    MockGmailClient,
    MockGustoClient,
    MockHibobClient,
    MockMiroClient,
    MockRampClient,
    MockSignalClient,
    MockSlackClient,
)


# AWS CloudTrail fixture anchor inside the fetcher's 90-day backfill window AND
# the observations partition coverage (≈ 2026-05-15). The fixture's default
# 2026-01 base would fall outside the 90-day floor and yield ZERO records, which
# the preflight reads as a (spurious) realism failure. Mirrors run_all_sources'
# _AWS_BASE_MS so preflight and the live run anchor identically.
_AWS_PREFLIGHT_BASE_MS = 1778803200000


class PreflightFailure(AssertionError):
    """A source's fixture-generated record failed the realism gate.

    Raising fails the run BEFORE substrate spin-up — it is a real
    finding (a fixture diverged from what its handler / the observations
    schema require), not flaky infra.
    """


@dataclass
class SourcePreflightResult:
    source: str
    channel: str
    records_checked: int
    sample_external_id: str
    sample_occurred_at: str


# A minimal install row + shard_identifier per source, mirroring what
# `source_onboarding`'s planner would hand the fetcher. Values are
# filled from each fixture below.
async def _close() -> None:
    return None


def _patch_client(module: Any, attr: str, client: Any) -> None:
    async def _open(_install: Any):  # noqa: ANN202
        return client, _close

    setattr(module, attr, _open)


async def _gmail_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    client = MockGmailClient(fixture=fixture)
    _patch_client(_gmail_fetcher, "_open_gmail_client", client)
    install = {
        "id": uuid4(),
        "scope": "gmail.metadata",
        "tenant_id": uuid4(),
    }
    shard = {
        "shard_kind": "gmail_mailbox_window",
        "mailbox_email": fixture["email"],
        "user_id": None,
        "initial_history_id": fixture.get("starting_history_id"),
    }
    result = await _gmail_fetcher.fetch_page_gmail(install, shard, None)
    return list(result.records)


async def _github_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    client = MockGithubClient(fixture=fixture)
    _patch_client(_github_fetcher, "_open_github_client", client)
    repo = fixture["repos"][0]
    full_name = repo["full_name"]
    owner, _, name = full_name.partition("/")
    event_type = next(iter(repo["events_by_type"].keys()))
    install = {"id": uuid4(), "installation_id": fixture["installation_id"]}
    shard = {
        "shard_kind": "github_repo_events",
        "event_type": event_type,
        "owner": owner,
        "repo": name,
        "repo_full_name": full_name,
        "installation_id": fixture["installation_id"],
    }
    result = await _github_fetcher.fetch_page_github(install, shard, None)
    return list(result.records)


async def _slack_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    client = MockSlackClient(fixture=fixture)
    _patch_client(_slack_fetcher, "_open_slack_client", client)
    channel = fixture["channels"][0]
    install = {"id": uuid4(), "installation_id": fixture["team_id"]}
    shard = {
        "shard_kind": "slack_channel_window",
        "channel_id": channel["id"],
        "team_id": fixture["team_id"],
        "installation_id": fixture["team_id"],
    }
    result = await _slack_fetcher.fetch_page_slack(install, shard, None)
    return list(result.records)


async def _discord_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    client = MockDiscordClient(fixture=fixture)
    _patch_client(_discord_fetcher, "_open_discord_client", client)
    channel = fixture["channels"][0]
    install = {"id": uuid4(), "installation_id": fixture["guild_id"]}
    shard = {
        "shard_kind": "discord_channel_window",
        "channel_id": channel["id"],
        "guild_id": fixture["guild_id"],
        "installation_id": fixture["guild_id"],
    }
    result = await _discord_fetcher.fetch_page_discord(install, shard, None)
    return list(result.records)


async def _brex_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Brex (Bearer / Mercury archetype): shard per account; the fetcher reads
    # `shard_identifier["account_id"]` and emits an account snapshot + txns.
    client = MockBrexClient(fixture=fixture)
    _patch_client(_brex_fetcher, "_open_brex_client", client)
    account_id = fixture["account_order"][0]
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "base_url": "https://platform.brexapis.com"}
    shard = {"shard_kind": _brex_fetcher.SHARD_KIND_ACCOUNT_TXNS,
             "account_id": account_id}
    result = await _brex_fetcher.fetch_page_brex(install, shard, None)
    return list(result.records)


async def _deel_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Deel (Bearer / Mercury archetype): shard per contract; the fetcher reads
    # `shard_identifier["contract_id"]` and emits a contract snapshot + payments.
    client = MockDeelClient(fixture=fixture)
    _patch_client(_deel_fetcher, "_open_deel_client", client)
    contract_id = fixture["contract_order"][0]
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "base_url": "https://api.letsdeel.com"}
    shard = {"shard_kind": _deel_fetcher.SHARD_KIND_CONTRACT_PAYMENTS,
             "contract_id": contract_id}
    result = await _deel_fetcher.fetch_page_deel(install, shard, None)
    return list(result.records)


async def _ramp_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Ramp (OAuth client-credentials, keyset REST): shard per entity_type.
    client = MockRampClient(fixture=fixture)
    _patch_client(_ramp_fetcher, "_open_ramp_client", client)
    entity_type = next(iter(fixture["entities"].keys()))
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "business_id": fixture["business_id"],
               "base_url": "https://api.ramp.com/developer/v1"}
    shard = {"shard_kind": _ramp_fetcher.SHARD_KIND_ENTITY,
             "entity_type": entity_type}
    result = await _ramp_fetcher.fetch_page_ramp(install, shard, None)
    return list(result.records)


async def _gusto_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Gusto (OAuth payroll REST): shard per entity kind (employee/payroll).
    client = MockGustoClient(fixture=fixture)
    _patch_client(_gusto_fetcher, "_open_gusto_client", client)
    entity_type = next(iter(fixture["entities"].keys()))
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "company_uuid": fixture["company_uuid"],
               "base_url": "https://api.gusto.com"}
    shard = {"shard_kind": _gusto_fetcher.SHARD_KIND_ENTITY,
             "entity_type": entity_type}
    result = await _gusto_fetcher.fetch_page_gusto(install, shard, None)
    return list(result.records)


async def _fireflies_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Fireflies (HMAC / Brex archetype): ONE transcript shard per install; the
    # fetcher reads `shard_identifier["workspace_id"]` (external_id namespace)
    # and emits one record per transcript (NO snapshot).
    client = MockFirefliesClient(fixture=fixture)
    _patch_client(_fireflies_fetcher, "_open_fireflies_client", client)
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "base_url": "https://api.fireflies.ai"}
    shard = {"shard_kind": _fireflies_fetcher.SHARD_KIND_TRANSCRIPTS,
             "workspace_id": fixture["workspace_id"],
             "installation_id": str(install["id"]),
             "transcript_cursor": None}
    result = await _fireflies_fetcher.fetch_page_fireflies(install, shard, None)
    return list(result.records)


async def _signal_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Signal (gateway / Telegram archetype): shard per thread; the fetcher reads
    # `shard_identifier["thread_id"]` + installation_id (external_id namespace).
    client = MockSignalClient(fixture=fixture)
    _patch_client(_signal_fetcher, "_open_signal_client", client)
    tid = fixture["thread_order"][0]
    thread = fixture["threads"][str(tid)]
    install = {"id": uuid4(), "tenant_id": uuid4()}
    shard = {"shard_kind": _signal_fetcher.SHARD_KIND_THREAD_HISTORY,
             "thread_id": tid,
             "thread_kind": thread.get("thread_kind") or "direct",
             "thread_title": thread.get("title"),
             "installation_id": str(install["id"]),
             "offset_id_cursor": None}
    result = await _signal_fetcher.fetch_page_signal(install, shard, None)
    return list(result.records)


async def _aws_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # AWS (poll / Grafana-backfill archetype): ONE event shard per install; the
    # fetcher reads (account_id, region) FROM THE INSTALL ROW for the external_id
    # namespace (aws:{account_id}:{region}:event:{event_id}).
    client = MockAwsClient(fixture=fixture)
    _patch_client(_aws_fetcher, "_open_aws_client", client)
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "account_id": fixture["account_id"], "region": fixture["region"],
               "credential_kind": "assume_role"}
    shard = {"shard_kind": _aws_fetcher.SHARD_KIND_ACCOUNT_EVENTS,
             "installation_id": str(install["id"]),
             "account_id": fixture["account_id"], "region": fixture["region"],
             "updated_cursor": None}
    result = await _aws_fetcher.fetch_page_aws(install, shard, None)
    return list(result.records)


async def _miro_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Miro (HMAC / Brex archetype): shard per board; the fetcher reads
    # `shard_identifier["board_id"]` + org_id (external_id namespace) and emits
    # one record per item (NO snapshot).
    client = MockMiroClient(fixture=fixture)
    _patch_client(_miro_fetcher, "_open_miro_client", client)
    board_id = fixture["board_order"][0]
    board = fixture["boards"][board_id]
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "base_url": "https://api.miro.com/v2"}
    shard = {"shard_kind": _miro_fetcher.SHARD_KIND_BOARD_ITEMS,
             "board_id": board_id, "board_name": board.get("name"),
             "org_id": str(fixture["org_id"]),
             "installation_id": str(install["id"]),
             "item_cursor": None}
    result = await _miro_fetcher.fetch_page_miro(install, shard, None)
    return list(result.records)


async def _figma_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Figma (HMAC / Brex archetype): shard per file; the fetcher reads
    # `shard_identifier["file_key"]` + team_id (external_id namespace) and emits
    # one record per event (pure event stream, NO snapshot).
    client = MockFigmaClient(fixture=fixture)
    _patch_client(_figma_fetcher, "_open_figma_client", client)
    file_key = fixture["file_order"][0]
    f_meta = fixture["files"][file_key]
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "team_id": fixture["team_id"],
               "base_url": "https://api.figma.com"}
    shard = {"shard_kind": _figma_fetcher.SHARD_KIND_FILE_EVENTS,
             "file_key": file_key, "file_name": f_meta.get("name"),
             "team_id": fixture["team_id"],
             "installation_id": str(install["id"]),
             "event_cursor": None}
    result = await _figma_fetcher.fetch_page_figma(install, shard, None)
    return list(result.records)


async def _hibob_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    client = MockHibobClient(fixture=fixture)
    _patch_client(_hibob_fetcher, "_open_hibob_client", client)
    entity_type = next(iter(fixture["entities"].keys()))
    install = {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "company_id": fixture["company_id"],
        "service_user_id": "svc-pre",
        "base_url": "https://api.hibob.com",
    }
    shard = {
        "shard_kind": _hibob_fetcher.SHARD_KIND_ENTITY,
        "entity_type": entity_type,
        "company_id": fixture["company_id"],
        "installation_id": str(install["id"]),
        "updated_cursor": None,
    }
    result = await _hibob_fetcher.fetch_page_hibob(install, shard, None)
    return list(result.records)


async def _carta_records(fixture: dict[str, Any]) -> list[dict[str, Any]]:
    # Carta (poll / Gusto-backfill archetype): shard per entity_type; the fetcher
    # reads `shard_identifier["entity_type"]` + install firm_id (external_id
    # namespace: carta:{firm_id}:{entity_kind}:{entity_id}:{version} — the
    # version is a content digest; see handlers/carta.carta_version).
    client = MockCartaClient(fixture=fixture)
    _patch_client(_carta_fetcher, "_open_carta_client", client)
    entity_type = next(iter(fixture["entities"].keys()))
    install = {"id": uuid4(), "tenant_id": uuid4(),
               "firm_id": fixture["firm_id"],
               "base_url": "https://api.carta.com"}
    shard = {"shard_kind": _carta_fetcher.SHARD_KIND_ENTITY,
             "entity_type": entity_type, "firm_id": fixture["firm_id"],
             "installation_id": str(install["id"]),
             "updated_cursor": None}
    result = await _carta_fetcher.fetch_page_carta(install, shard, None)
    return list(result.records)


_SOURCE_SPECS: dict[str, Any] = {
    "gmail": (lambda: make_gmail_mailbox(email="preflight@example.com",
                                         messages=3), _gmail_records),
    "github": (lambda: make_github_repos(org_or_user="preflight", repos=1,
                                          events_per_repo=2), _github_records),
    "slack": (lambda: make_slack_workspace(team_id="T_PRE", channels=1,
                                           messages_per_channel=3),
              _slack_records),
    "discord": (lambda: make_discord_guild(guild_id="G_PRE", channels=1,
                                           messages_per_channel=3),
                _discord_records),
    # IN-FIN2 finance sources (additive — finance was not previously covered).
    "brex": (lambda: make_brex(accounts=1, transactions_per_account=3,
                               seed="pre"), _brex_records),
    "ramp": (lambda: make_ramp(business_id="r-pre", entities=["transaction"],
                               rows_per_entity=2), _ramp_records),
    "gusto": (lambda: make_gusto(company_uuid="c-pre", entities=["employee"],
                                 rows_per_entity=2), _gusto_records),
    "deel": (lambda: make_deel(contracts=1, payments_per_contract=3,
                               seed="pre"), _deel_records),
    # Vertical-2 sources (additive — these verticals were not previously covered).
    "fireflies": (lambda: make_fireflies(workspace_id="ws-pre", transcripts=3,
                                         seed="pre"), _fireflies_records),
    "signal": (lambda: make_signal(threads=1, messages_per_thread=3,
                                   seed="pre"), _signal_records),
    "aws": (lambda: make_aws(account_id="900000000001", region="us-east-1",
                             events=3, base_ms=_AWS_PREFLIGHT_BASE_MS,
                             seed="pre"), _aws_records),
    "miro": (lambda: make_miro(org_id="org-pre", boards=1, items_per_board=3,
                               seed="pre"), _miro_records),
    "figma": (lambda: make_figma(team_id="team-pre", events=3,
                                 seed="pre"), _figma_records),
    "hibob": (lambda: make_hibob(company_id="hibob-co-pre",
                                 entities=["employee", "lifecycle", "timeoff", "payroll"],
                                 rows_per_entity=1, seed="pre"),
              _hibob_records),
    "carta": (lambda: make_carta(firm_id="firm-pre", rows_per_entity=1,
                                 seed="pre"), _carta_records),
}


_BOUND_RE = re.compile(
    r"FROM \('([^']+)'\) TO \('([^']+)'\)"
)


async def _partition_coverage(
    pool: asyncpg.Pool, table: str = "observations",
) -> tuple[dt.datetime, dt.datetime]:
    """Return (min_lower, max_upper) across the table's range partitions.

    Parses `pg_get_expr(relpartbound)` rather than assuming a fixed
    window, so the gate adapts to whatever partitions currently exist.
    """
    rows = await pool.fetch(
        """
        SELECT pg_get_expr(c.relpartbound, c.oid) AS bounds
          FROM pg_inherits i
          JOIN pg_class c ON c.oid = i.inhrelid
         WHERE i.inhparent = (SELECT oid FROM pg_class WHERE relname = $1)
        """,
        table,
    )
    lowers: list[dt.datetime] = []
    uppers: list[dt.datetime] = []
    for r in rows:
        m = _BOUND_RE.search(r["bounds"] or "")
        if not m:
            continue
        lowers.append(dt.datetime.fromisoformat(m.group(1)))
        uppers.append(dt.datetime.fromisoformat(m.group(2)))
    if not lowers:
        raise PreflightFailure(
            f"{table} has no parseable range partitions; cannot validate "
            f"fixture occurred_at coverage."
        )
    return min(lowers), max(uppers)


async def preflight_source(
    source: str, pool: asyncpg.Pool,
) -> SourcePreflightResult:
    """Run the realism gate for one source. Raises PreflightFailure."""
    make_fixture, get_records = _SOURCE_SPECS[source]
    fixture = make_fixture()
    channel = resolve_channel(source, "backfill")
    if channel is None:
        raise PreflightFailure(
            f"{source}: no channel mapping for (source, 'backfill') — "
            f"the normalizer would drop every backfill record."
        )
    handler = get_handler(channel)

    records = await get_records(fixture)
    if not records:
        raise PreflightFailure(
            f"{source}: fetcher produced zero records from a non-empty "
            f"fixture — the backfill path can never produce observations."
        )

    lower, upper = await _partition_coverage(pool)
    sample_ext = ""
    sample_when = ""
    for record in records:
        body = dict(record)
        headers = body.pop("webhook_metadata", {}) or {}
        try:
            draft = await handler(body, headers)
        except Exception as exc:  # noqa: BLE001 — surface as a finding
            raise PreflightFailure(
                f"{source}: handler {channel!r} raised on a fixture record "
                f"({type(exc).__name__}: {exc}). The fixture is missing a "
                f"field the handler requires (e.g. gmail Message-ID)."
            ) from exc
        if not draft.external_id:
            raise PreflightFailure(
                f"{source}: handler produced a NULL external_id — no dedup "
                f"key. The fixture is missing the field external_id derives "
                f"from (e.g. github node_id)."
            )
        occurred = draft.occurred_at
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=dt.timezone.utc)
        if not (lower <= occurred < upper):
            raise PreflightFailure(
                f"{source}: draft.occurred_at={occurred.isoformat()} is "
                f"outside the observations partition coverage "
                f"[{lower.isoformat()}, {upper.isoformat()}). The writer "
                f"would raise a missing-partition CheckViolation (A28). Move "
                f"the fixture's timestamp base into range."
            )
        sample_ext = draft.external_id
        sample_when = occurred.isoformat()

    return SourcePreflightResult(
        source=source,
        channel=channel,
        records_checked=len(records),
        sample_external_id=sample_ext,
        sample_occurred_at=sample_when,
    )


async def run_preflight(
    pool: asyncpg.Pool, sources: list[str] | None = None,
) -> list[SourcePreflightResult]:
    """Run the realism gate for every source. Raises on the first
    failure (fail-fast — a 90-minute run should not start on a known-bad
    fixture)."""
    sources = sources or list(_SOURCE_SPECS.keys())
    return [await preflight_source(s, pool) for s in sources]
