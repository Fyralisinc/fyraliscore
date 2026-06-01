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

from services.ingestion.fetchers import discord as _discord_fetcher
from services.ingestion.fetchers import github as _github_fetcher
from services.ingestion.fetchers import gmail as _gmail_fetcher
from services.ingestion.fetchers import slack as _slack_fetcher
from services.ingestion.handlers import get_handler
from services.ingestion.normalizer.channel_mapping import resolve_channel
from services.synthetic.fixtures import (
    make_discord_guild,
    make_github_repos,
    make_gmail_mailbox,
    make_slack_workspace,
)
from services.synthetic.mock_clients import (
    MockDiscordClient,
    MockGithubClient,
    MockGmailClient,
    MockSlackClient,
)


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
