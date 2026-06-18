"""Alpen e2e ingestion driver — drive Fyralis's REAL source clients/fetchers/
handlers against the external saas-api-mocks ("spammer") Alpen Labs company,
writing observations into an isolated DB. Ingestion-only (no Kafka, no LLM).

Pattern (mirrors services/ingest/synthetic/validation_runs/preflight.py): for
each source, build the REAL integration client pointed at the mock's port,
monkeypatch the fetcher's `_open_<src>_client` to return it, enumerate shards
from the live mock, paginate every page, run the REAL handler, and write the
draft via core.ingest_from_draft.

Run:  DATABASE_URL=postgresql://company_os:company_os@localhost:5432/company_os_alpen \
      .venv/bin/python scripts/alpen_ingest.py mercury [github ...]
"""
from __future__ import annotations

import asyncio
import asyncpg
import os
import sys
from urllib.parse import urlparse
from uuid import UUID, uuid4

from lib.integrations import endpoints as _ep
from lib.shared.db import init_pool, get_pool, close_pool
from lib.shared.migrations import ensure_test_partition_window
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.core import ingest_from_draft
from services.ingest.ingestion.handlers import get_handler
from services.ingest.ingestion.normalizer.channel_mapping import resolve_channel

# The tenant the spammer prepared (./dev.sh status -> fyralis_tenant_id).
TENANT = UUID(os.environ.get(
    "ALPEN_TENANT_ID",
    "90864cdd-731b-44b3-96c5-78f0004af3e2",
))
HOST = os.environ.get("ALPEN_MOCK_HOST", "http://localhost")

# saas-api-mocks per-provider ports (dev.sh cmd_serve).
PORTS = {
    "slack": 7001, "discord": 7002, "github": 7003, "gmail": 7004,
    "calendar": 7005, "notion": 7006, "drive": 7007, "jira": 7008,
    "quickbooks": 7009, "grafana": 7010, "mercury": 7011, "ashby": 7012,
    "brex": 7013, "deel": 7014, "hibob": 7015, "figma": 7016, "miro": 7017,
    "ramp": 7018, "gusto": 7019, "carta": 7020, "linkedin": 7021,
    "fireflies": 7022, "aws": 7023, "telegram": 7024, "signal": 7025,
}


def base(src: str) -> str:
    return f"{HOST}:{PORTS[src]}"


def rebased(src: str, ep_name: str) -> str:
    """Mock base URL = mock host:port + the production base's PATH component.
    The mocks mirror each vendor's real API paths (e.g. Mercury /api/v1/...),
    so we graft the prod path (from endpoints._PROD) onto the mock port."""
    path = urlparse(_ep._PROD[ep_name]).path.rstrip("/")
    return f"{base(src)}{path}"


async def _noop(*_a, **_k):
    return None


async def _write_records(records, source: str, pool) -> int:
    """Run each fetched record through the real handler + writer. Returns
    the number of drafts successfully written as observations."""
    channel = resolve_channel(source, "backfill")
    if channel is None:
        raise RuntimeError(f"{source}: no (source,'backfill') channel mapping")
    handler = get_handler(channel)
    actor_repo = ActorRepo(pool)
    alias_repo = EntityAliasRepo(pool)
    written = 0
    for record in records:
        body = dict(record)
        headers = body.pop("webhook_metadata", {}) or {}
        draft = await handler(body, headers)
        if not draft.external_id:
            continue
        await ingest_from_draft(
            channel=draft.source_channel,
            draft=draft,
            pool=pool,
            tenant_id=TENANT,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            enqueue_trigger=False,   # ingestion-only: no Think trigger
            embedder=None,           # no embeddings
        )
        written += 1
    return written


# --------------------------------------------------------------------------
# Per-source onboarding + fetch. Each returns the count of observations written.
# --------------------------------------------------------------------------
async def ingest_mercury(pool) -> int:
    from services.ingest.ingestion.fetchers import mercury as fet
    from services.ingest.integrations.mercury.client import MercuryClient

    mbase = rebased("mercury", "mercury_api")
    client = MercuryClient(
        base_url=mbase,
        api_base_url=mbase,
        api_token="alpen-mercury-token",  # any-token mock
    )
    fet._open_mercury_client = lambda _install: _return(client)

    accounts = await client.list_accounts()
    print(f"  mercury: {len(accounts)} accounts discovered")
    install = {"id": uuid4(), "tenant_id": TENANT}
    total = 0
    for acct in accounts:
        account_id = str(acct.get("id") or acct.get("account_id"))
        shard = {"shard_kind": fet.SHARD_KIND_ACCOUNT_TXNS, "account_id": account_id}
        cursor = None
        records: list = []
        for _ in range(10_000):  # page guard
            res = await fet.fetch_page_mercury(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "mercury", pool)
    return total


async def _return(client):
    return client, _noop



async def ingest_ashby(pool) -> int:
    from services.ingest.ingestion.fetchers import ashby as fet
    from services.ingest.integrations.ashby.client import AshbyClient, DEFAULT_ENTITIES

    mbase = rebased("ashby", "ashby_api")          # mock host:port + prod /ashby path

    # ORG_ID the mock seeds for the Alpen run (spammers/ashby/seed.py ORG_ID).
    # It is the external_id namespace (`ashby:{org}:{kind}:{id}`) the handler
    # builds; it MUST be non-empty or _write_records skips every record, and it
    # MUST be "alpenlabs" to match ground-truth external_ids.
    org_id = "alpenlabs"

    # Any non-empty Basic username authenticates; "spam-ashby" mirrors the
    # production spammer preset. api_base_url points the client at the mock.
    client = AshbyClient(
        base_url=mbase,
        org_id=org_id,
        api_base_url=mbase,
        api_key="spam-ashby",
    )
    # The fetcher opens via `client, close = await _open_ashby_client(install)`;
    # _return(client) yields (client, _noop) so close() is a no-op.
    fet._open_ashby_client = lambda _install: _return(client)

    # ONBOARDING: Ashby is entity-type-sharded — there is NO list endpoint to
    # discover shards. The five RPC categories are fixed (client.DEFAULT_ENTITIES
    # == mock CATEGORIES == candidate/application/job/interview/offer); each is
    # one `ashby_entity` shard. The fetcher reads install["org_id"] and
    # shard["entity_type"].
    install = {"id": uuid4(), "tenant_id": TENANT, "org_id": org_id}

    total = 0
    for entity_type in DEFAULT_ENTITIES:
        shard = {"shard_kind": fet.SHARD_KIND_ENTITY, "entity_type": entity_type}
        cursor = None
        records = []
        for _ in range(10_000):                          # paginate every page
            res = await fet.fetch_page_ashby(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "ashby", pool)
    return total


async def ingest_aws(pool) -> int:
    # AWS CloudTrail management-events backfill (poll/Grafana archetype): ONE
    # `aws_account_events` shard per install. Unlike the HTTP-token sources, the
    # REAL AwsClient signs every LookupEvents call with SigV4 via aioboto3 and the
    # mock VERIFIES the signature against the seeded static secret — so we (a) point
    # aioboto3 at the mock root with endpoint_override (the mock dispatches on
    # POST /), and (b) preset client._creds with the seeded static keys so
    # resolve_credentials (which needs a secret_store) is never called. AWS has no
    # endpoints._PROD key, so there is no rebased(...) here.
    import asyncpg

    from services.ingest.ingestion.fetchers import aws as fet
    from services.ingest.integrations.aws.client import AwsClient
    from services.ingest.integrations.aws.credentials import AwsCredentials

    # Full-window walk: no floor (cover the whole seeded ~88-day corpus), 50/page
    # (CloudTrail's LookupEvents cap, matched by the mock).
    os.environ.setdefault("AWS_BACKFILL_WINDOW_DAYS", "0")
    os.environ.setdefault("AWS_EVENTS_PAGE_SIZE", "50")

    # ONBOARDING: the AWS mock is single-tenant per run — there is exactly one
    # install row carrying the SigV4 identity. Read it (and its static secret) from
    # the mock_orgs DB; this both enumerates the shard (account_id, region) and
    # gives us the secret the SigV4 mock will verify against.
    conn = await asyncpg.connect(
        "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    )
    try:
        rows = await conn.fetch(
            "SELECT account_id, region, access_key_id, secret_access_key "
            "FROM app_aws.installations ORDER BY created_at DESC"
        )
    finally:
        await conn.close()
    if not rows:
        raise RuntimeError("aws: no install in app_aws.installations (run not seeded)")

    print(f"  aws: {len(rows)} install(s) discovered")
    total = 0
    for row in rows:
        account_id = str(row["account_id"])
        region = str(row["region"])

        # Build the REAL AwsClient pointed at the mock root. endpoint_override is
        # the moto/localstack seam aioboto3 honours as endpoint_url; the mock
        # dispatches CloudTrail vs STS on the request shape at POST /.
        client = AwsClient(
            account_id=account_id,
            region=region,
            endpoint_override=base("aws"),
        )
        # Preset static creds (no session_token, no expiry) so the client's
        # _credentials() short-circuits before touching secret_store, and aioboto3
        # SigV4-signs with the seeded access-key the mock can verify.
        client._creds = AwsCredentials(
            access_key_id=str(row["access_key_id"]),
            secret_access_key=str(row["secret_access_key"]),
        )

        fet._open_aws_client = lambda _install: _return(client)

        # install/shard shapes copied from preflight._aws_records: the fetcher reads
        # account_id + region OFF THE INSTALL for the external_id namespace
        # (aws:{account_id}:{region}:event:{event_id}); the shard's updated_cursor
        # stays None for a FULL backfill.
        install = {
            "id": uuid4(),
            "tenant_id": TENANT,
            "account_id": account_id,
            "region": region,
            "credential_kind": "static_keys",
        }
        shard = {
            "shard_kind": fet.SHARD_KIND_ACCOUNT_EVENTS,
            "installation_id": str(install["id"]),
            "account_id": account_id,
            "region": region,
            "updated_cursor": None,
        }

        cursor = None
        records: list = []
        for _ in range(10_000):  # page guard
            res = await fet.fetch_page_aws(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "aws", pool)
    return total


async def ingest_brex(pool) -> int:
    from services.ingest.ingestion.fetchers import brex as fet
    from services.ingest.integrations.brex.client import BrexClient

    # _PROD["brex_api"] is https://platform.brexapis.com (empty path), so
    # rebased(...) == "http://localhost:7013" and the client hits the mock's
    # /v2/... routes at root (matching the live probe). The constructor uses
    # (api_base_url or base_url), so passing both = bbase mirrors Mercury.
    bbase = rebased("brex", "brex_api")
    client = BrexClient(base_url=bbase, api_base_url=bbase, api_token="bxt_alpen-brex-token")

    # Monkeypatch the fetcher's opener seam -> our pre-built mock-pointed client.
    fet._open_brex_client = lambda _install: _return(client)

    # ONBOARDING: list_accounts() enumerates both cash (GET /v2/accounts/cash,
    # cursor page) and card (GET /v2/accounts/card, bare array) accounts, each
    # tagged with a private "_fyralis_account_kind" ("cash"/"card"). That kind
    # is REQUIRED on the shard so the fetcher routes card txns to
    # /v2/transactions/card/primary and cash txns to /v2/transactions/cash/{id}.
    accounts = await client.list_accounts()

    install = {"id": uuid4(), "tenant_id": TENANT}
    total = 0
    for acct in accounts:
        account_id = str(acct.get("id") or acct.get("account_id") or "")
        if not account_id:
            continue
        account_kind = acct.get("_fyralis_account_kind")
        shard = {
            "shard_kind": fet.SHARD_KIND_ACCOUNT_TXNS,
            "account_id": account_id,
            "account_kind": account_kind,
        }
        cursor = None
        records = []
        for _ in range(10_000):                       # paginate every page
            res = await fet.fetch_page_brex(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "brex", pool)
    return total


async def ingest_calendar(pool) -> int:
    # Google Calendar mock (:7005). DWD auth: the consumer mints a per-user,
    # scope-bound bearer by signing a service-account JWT and POSTing it to
    # /token; the mock decodes the assertion WITHOUT verifying the signature
    # (it has no SA private key) but the JWT must still be a real RS256 token.
    # The seeded service account (private key included) lives in the mock DB
    # in app_calendar.accounts, so we build a genuine DwdTokenMinter over it
    # with token_uri pointed at the mock's /token.
    import os
    import asyncpg
    from services.ingest.ingestion.fetchers import google_calendar as fet
    from services.ingest.integrations.gmail.client import GoogleHttpClient
    from services.ingest.integrations.gmail.dwd import DwdTokenMinter, ServiceAccountKey
    from services.ingest.integrations.google_calendar.client import (
        GoogleCalendarClient,
        CALENDAR_READONLY_SCOPE,
    )

    cal_base = rebased("calendar", "google_calendar_api")   # http://localhost:7005/calendar/v3
    token_uri = base("calendar").rstrip("/") + "/token"     # http://localhost:7005/token

    # ---- ONBOARDING: pull the seeded service account + enumerate calendars ----
    # The /users/me/calendarList route only returns the *token owner's* own
    # calendar (the mock models one calendar per person, keyed by email), so it
    # can't enumerate the whole org from one token. Enumerate the seeded
    # calendars (== owner emails) straight from the mock's app_calendar tables,
    # scoped to the same account/run as the service account we mint with.
    mock_dsn = "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    conn = await asyncpg.connect(mock_dsn)
    try:
        acct = await conn.fetchrow(
            """
            SELECT a.id AS account_pk,
                   a.service_account_email,
                   a.service_account_private_key,
                   a.service_account_client_id
              FROM app_calendar.accounts a
              JOIN org.runs r ON r.id = a.run_id
             ORDER BY r.created_at DESC
             LIMIT 1
            """,
        )
        if acct is None:
            return 0
        cal_rows = await conn.fetch(
            """
            SELECT c.calendar_id
              FROM app_calendar.calendars c
             WHERE c.account_pk = $1
             ORDER BY c.calendar_id
            """,
            acct["account_pk"],
        )
    finally:
        await conn.close()

    calendar_ids = [r["calendar_id"] for r in cal_rows]
    if not calendar_ids:
        return 0

    # ---- Build the REAL Calendar client pointed at the mock ----
    sa_key = ServiceAccountKey(
        client_email=acct["service_account_email"],
        private_key_pem=acct["service_account_private_key"],
        private_key_id=acct["service_account_client_id"] or "",
        token_uri=token_uri,
    )
    minter = DwdTokenMinter(sa_key)
    await minter.__aenter__()
    http = GoogleHttpClient(minter)
    await http.__aenter__()
    client = GoogleCalendarClient(
        http, scope=CALENDAR_READONLY_SCOPE, base_url=cal_base,
    )

    # The seeded events span 2024-02 .. 2026-06; the fetcher's full-sync window
    # (GOOGLE_CALENDAR_BACKFILL_DAYS, default 180) would clip most of them, so
    # widen it to cover the whole corpus before the first fetch freezes
    # cur.time_min.
    prior_days = os.environ.get("GOOGLE_CALENDAR_BACKFILL_DAYS")
    os.environ["GOOGLE_CALENDAR_BACKFILL_DAYS"] = "4000"

    # Monkeypatch the fetcher's opener (it normally builds a DWD client from
    # env-loaded SA creds + endpoint resolution) to return our mock-pointed one.
    fet._open_calendar_client = lambda _install: _return(client)

    try:
        # The fetcher reads install["scope"] only inside the production opener
        # (which we replace), but we set it for parity with the real install row.
        install = {"id": uuid4(), "tenant_id": TENANT, "scope": "calendar.readonly"}
        total = 0
        for calendar_id in calendar_ids:
            # Shard shape mirrors fetch_page_google_calendar's reads:
            # calendar_id (required), owner_email (impersonated subject), and an
            # optional warm-start sync_token (absent here -> windowed full sync).
            shard = {
                "shard_kind": fet.SHARD_KIND_EVENTS,
                "calendar_id": calendar_id,
                "owner_email": calendar_id,
            }
            cursor = None
            records = []
            for _ in range(10_000):
                res = await fet.fetch_page_google_calendar(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "google_calendar", pool)
        return total
    finally:
        if prior_days is None:
            os.environ.pop("GOOGLE_CALENDAR_BACKFILL_DAYS", None)
        else:
            os.environ["GOOGLE_CALENDAR_BACKFILL_DAYS"] = prior_days
        await http.__aexit__(None, None, None)
        await minter.__aexit__(None, None, None)


async def ingest_carta(pool) -> int:
    from services.ingest.ingestion.fetchers import carta as fet
    from services.ingest.integrations.carta.client import CartaClient, DEFAULT_ENTITIES

    cbase = rebased("carta", "carta_api")  # mock host:port + prod base path (/carta)

    # ONBOARDING: discover the issuer (the per-install firm_id scope). The mock is
    # single-tenant per run and exposes exactly one issuer via GET /v1alpha1/issuers.
    # We build a temporary client (no issuer_id needed for list_issuers) to learn it.
    discover = CartaClient(
        base_url=cbase,
        api_base_url=cbase,
        access_token="alpen-carta-token",
    )
    issuer_ids: list[str] = []
    page_token = None
    try:
        for _ in range(10_000):
            issuers, page_token = await discover.list_issuers(
                page_size=50, page_token=page_token,
            )
            for iss in issuers:
                iid = str(iss.get("id") or iss.get("issuer_id") or "")
                if iid:
                    issuer_ids.append(iid)
            if not page_token:
                break
    finally:
        await discover.aclose()

    total = 0
    for issuer_id in issuer_ids:
        # One CartaClient scoped to this issuer; reused across every entity shard
        # (list_entity calls _require_issuer(), so issuer_id MUST be set here).
        client = CartaClient(
            base_url=cbase,
            api_base_url=cbase,
            issuer_id=issuer_id,
            access_token="alpen-carta-token",
        )
        fet._open_carta_client = lambda _install, _c=client: _return(_c)

        install = {"id": uuid4(), "tenant_id": TENANT, "firm_id": issuer_id,
                   "base_url": "https://api.carta.com"}

        # Entity-type-sharded source: iterate the four known cap-table collections.
        for entity_type in DEFAULT_ENTITIES:
            shard = {
                "shard_kind": fet.SHARD_KIND_ENTITY,
                "entity_type": entity_type,
                "firm_id": issuer_id,
                "installation_id": str(install["id"]),
                "updated_cursor": None,
            }
            cursor = None
            records = []
            for _ in range(10_000):
                res = await fet.fetch_page_carta(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "carta", pool)

        await client.aclose()

    return total


async def ingest_deel(pool) -> int:
    from services.ingest.ingestion.fetchers import deel as fet
    from services.ingest.integrations.deel.client import DeelClient

    # mock host:port + the prod base PATH from endpoints._PROD["deel_api"]
    # ("/rest/v2"); DeelClient appends paths like /contracts, /invoices to it.
    dbase = rebased("deel", "deel_api")
    # Deel mock accepts any non-empty Bearer token (single-tenant per run).
    client = DeelClient(
        base_url=dbase,
        api_base_url=dbase,
        api_token="alpen-deel-token",
    )

    # Monkeypatch the fetcher's client opener -> always hand back the mock client.
    fet._open_deel_client = lambda _install: _return(client)

    # ONBOARDING: discover shards = one per contract (client walks all cursor pages).
    contracts = await client.list_contracts()

    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "base_url": "https://api.letsdeel.com",
    }

    total = 0
    for contract in contracts:
        contract_id = contract.get("id") or contract.get("contract_id")
        if not contract_id:
            continue
        contract_id = str(contract_id)
        # FULL/backfill: no payment_cursor -> emits contract_snapshot + payments.
        shard = {
            "shard_kind": fet.SHARD_KIND_CONTRACT_PAYMENTS,
            "contract_id": contract_id,
        }
        cursor = None
        records = []
        for _ in range(10_000):  # paginate every page
            res = await fet.fetch_page_deel(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "deel", pool)
    return total


async def ingest_discord(pool) -> int:
    # Discord backfill. The fetcher (services/ingest/ingestion/fetchers/discord.py)
    # paginates /channels/{cid}/messages via `before=<snowflake>` and injects
    # guild_id from the shard so external_id ("discord:{id}") matches the live
    # Gateway MESSAGE_CREATE twin. resolve_channel("discord","backfill") ->
    # the SAME "discord:message" handler the Gateway path uses.
    import asyncpg
    from services.ingest.ingestion.fetchers import discord as fet
    from services.ingest.integrations.discord.client import DiscordClient

    # --- real-token: read the bot token + seeded guild from mock_orgs.
    # The mock matches `Authorization: Bot <token>` against
    # app_discord.applications.bot_token for the active run.
    mo = await asyncpg.connect(
        "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    )
    try:
        rows = await mo.fetch(
            "SELECT a.application_id, a.bot_token, g.guild_id "
            "FROM app_discord.applications a "
            "JOIN app_discord.guilds g ON g.application_pk = a.id"
        )
    finally:
        await mo.close()
    if not rows:
        return 0
    bot_token = rows[0]["bot_token"]
    # one app/bot in the seed; its installation_id is the guild it lives in.
    primary_guild_id = str(rows[0]["guild_id"])

    # --- base URL: the discord mock mounts routes at "/api/v10" at the
    # server ROOT (no "/discord" path prefix), so rebased("discord",
    # "discord_api") -> ".../discord/api/v10" would 404. Use base()+/api/v10.
    api_base = base("discord").rstrip("/") + "/api/v10"

    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "installation_id": primary_guild_id,
    }

    # Real DiscordClient pointed at the mock. base_url overrides the
    # endpoint resolver; the bot token is preset (DISCORD_BOT_TOKEN env is
    # unset). pool is passed for the 401/403 chokepoint path (never fires
    # with a valid token). One client per guild is built in the loop below.
    total = 0
    SEEN_PAGE_GUARD = 10_000

    async def _open(_install):
        gid = str(_install["installation_id"])
        client = DiscordClient(
            pool=pool,
            secret_store=None,
            tenant_id=TENANT,
            installation_row_id=_install["id"],
            guild_id=gid,
            base_url=api_base,
        )
        client._bot_token = bot_token
        return client

    # ONBOARDING: the bot's guilds (GET /users/@me/guilds), then each
    # guild's channels (GET /guilds/{gid}/channels). Shard = one text
    # channel; the fetcher reads shard["channel_id"] + shard["guild_id"].
    disco = await _open(install)
    try:
        guilds = await disco.list_guilds()
        all_channels: list[tuple[str, str]] = []  # (guild_id, channel_id)
        for g in guilds:
            gid = str(g.get("id") or g.get("guild_id") or "")
            if not gid:
                continue
            channels = await disco.list_guild_channels(gid)
            for ch in channels:
                # type 0 == GUILD_TEXT; only text channels carry messages.
                if ch.get("type") != 0:
                    continue
                cid = ch.get("id")
                if cid:
                    all_channels.append((gid, str(cid)))
    finally:
        await disco.aclose()

    # Fall back to the install's known guild if onboarding returned nothing.
    if not all_channels:
        fallback = await _open(install)
        try:
            channels = await fallback.list_guild_channels(primary_guild_id)
            for ch in channels:
                if ch.get("type") == 0 and ch.get("id"):
                    all_channels.append((primary_guild_id, str(ch["id"])))
        finally:
            await fallback.aclose()

    # Monkeypatch the fetcher opener to return a per-guild client. The
    # fetcher passes `install` straight through, so swap installation_id
    # to the shard's guild for correct guild-scoped auth/logging.
    for gid, cid in all_channels:
        guild_install = {**install, "installation_id": gid}
        client = await _open(guild_install)            # real DiscordClient for this guild
        fet._open_discord_client = lambda _i, _c=client: _return(_c)
        shard = {
            "shard_kind": fet.SHARD_KIND_CHANNEL_WINDOW,
            "channel_id": cid,
            "guild_id": gid,
            "installation_id": gid,
        }
        cursor = None
        records: list = []
        try:
            for _ in range(SEEN_PAGE_GUARD):
                res = await fet.fetch_page_discord(guild_install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "discord", pool)
        finally:
            await client.aclose()

    return total


async def ingest_drive(pool) -> int:
    """Google Drive backfill against the saas-api-mocks Drive mock (:7007).

    The Fyralis source/handler name is `google_drive` (fetcher module
    `fetchers.google_drive`, channel `google_drive:file`), but the driver's
    PORTS/base() key is `drive`. Auth is Google DWD: the mock mints a `ya29.`
    token via POST /token from a JWT assertion whose signature it does NOT
    verify (it only decodes `sub`/`scope`). So we drive the production
    GoogleDriveClient over the real GoogleHttpClient, swapping the DWD minter
    for a stub that exchanges an unsigned JWT at the mock's /token endpoint.

    NOTE: the live mock serves `/drive/v3/...` and `/token` directly (no
    `/gdrive` / `/gmail` prefix), so we do NOT use rebased(); we point the
    client at base('drive')+'/drive/v3' and the minter at base('drive')+'/token'.
    """
    import httpx
    import jwt as _jwt

    from services.ingest.ingestion.fetchers import google_drive as fet
    from services.ingest.integrations.gmail.client import GoogleHttpClient
    from services.ingest.integrations.google_drive.client import (
        DRIVE_READONLY_SCOPE,
        GoogleDriveClient,
    )

    # The seeded corpus spans back to early 2024; widen the windowed-backfill
    # horizon so the FULL files.list walk captures every seeded file (the
    # production default of 180 days would drop the older history).
    os.environ["GOOGLE_DRIVE_BACKFILL_DAYS"] = "3650"

    api_base = f"{base('drive')}/drive/v3"   # mock serves /drive/v3 directly
    token_url = f"{base('drive')}/token"     # mock serves /token directly
    scope = DRIVE_READONLY_SCOPE

    # --- Stub DWD minter: exchange an (unsigned-as-far-as-the-mock-cares) JWT
    # assertion for a real ya29. token at the mock's /token endpoint. Matches
    # DwdTokenMinter.mint(*, user_email, scopes, now=None) -> str.
    class _MockDwdMinter:
        def __init__(self, exchange_url: str) -> None:
            self._url = exchange_url
            self._client = httpx.AsyncClient(timeout=30.0)

        async def mint(self, *, user_email, scopes, now=None):  # noqa: ANN001
            assertion = _jwt.encode(
                {
                    "iss": "alpen-sa@fyralis-mock.iam.gserviceaccount.com",
                    "sub": user_email,
                    "scope": " ".join(scopes),
                    "aud": self._url,
                },
                "spammers-mock-key",   # mock does not verify the signature
                algorithm="HS256",
            )
            resp = await self._client.post(
                self._url,
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
            )
            resp.raise_for_status()
            return resp.json()["access_token"]

        def invalidate(self, *, user_email, scopes):  # noqa: ANN001
            return None

        async def aclose(self) -> None:
            await self._client.aclose()

    minter = _MockDwdMinter(token_url)
    http = GoogleHttpClient(minter)
    await http.__aenter__()
    client = GoogleDriveClient(http, scope=scope, base_url=api_base)

    # The fetcher reads `install["scope"]` only inside the PRODUCTION opener,
    # which we replace; our seam ignores the install and yields the live client.
    fet._open_drive_client = lambda _install: _return(client)

    # The shard-shape the fetcher reads (services/ingest/ingestion/fetchers/
    # google_drive.py::fetch_page_google_drive): drive_id, drive_kind,
    # owner_email, start_page_token. owner_email is the impersonated subject.
    install = {"id": uuid4(), "tenant_id": TENANT, "scope": "drive.readonly"}

    # --- ONBOARDING: discover the shards (one per drive).
    targets: list[dict] = []

    # Shared Drives via the Drive client's drives.list (useDomainAdminAccess).
    # The shared-drive owner_email is the admin we impersonate to read it.
    admin_email = "drive-admin@alpenlabs.io"
    try:
        body = await client.list_shared_drives(user_email=admin_email)
        for d in body.get("drives") or []:
            did = d.get("id")
            if did:
                targets.append({
                    "drive_id": did,
                    "drive_kind": "shared_drive",
                    "owner_email": admin_email,
                })
    except Exception as exc:  # noqa: BLE001
        print(f"  drive: shared-drive enumeration failed: {exc}")

    # My Drives (one shard per user) — discovered from the mock_orgs seed, since
    # the mock matches a user's My Drive by owner_email == token subject. None
    # are seeded for Alpen today, but enumerate defensively so the adapter is
    # corpus-agnostic.
    try:
        import asyncpg
        conn = await asyncpg.connect(
            "postgresql://company_os:company_os@localhost:5432/mock_orgs"
        )
        try:
            rows = await conn.fetch(
                "SELECT DISTINCT owner_email FROM app_drive.drives "
                "WHERE kind='my_drive' AND owner_email IS NOT NULL"
            )
        finally:
            await conn.close()
        for r in rows:
            owner = r["owner_email"]
            if owner:
                targets.append({
                    "drive_id": "my-drive",
                    "drive_kind": "my_drive",
                    "owner_email": owner,
                })
    except Exception as exc:  # noqa: BLE001
        print(f"  drive: my-drive enumeration skipped: {exc}")

    print(f"  drive: {len(targets)} drive shard(s) discovered")

    total = 0
    try:
        for tgt in targets:
            shard = {
                "shard_kind": fet.SHARD_KIND_FILES,
                "drive_id": tgt["drive_id"],
                "drive_kind": tgt["drive_kind"],
                "owner_email": tgt["owner_email"],
                "installation_id": str(install["id"]),
                "start_page_token": None,   # None -> FULL windowed backfill
            }
            cursor = None
            records: list = []
            for _ in range(10_000):   # page guard
                res = await fet.fetch_page_google_drive(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            # Handler/channel name is `google_drive` (not the `drive` port key).
            total += await _write_records(records, "google_drive", pool)
    finally:
        await http.__aexit__(None, None, None)
        await minter.aclose()

    return total


async def ingest_figma(pool) -> int:
    from services.ingest.ingestion.fetchers import figma as fet
    from services.ingest.integrations.figma.client import FigmaClient

    # Mock host:port + the prod base PATH from endpoints._PROD["figma_api"] (mounts /figma).
    fbase = rebased("figma", "figma_api")

    # The seeded single-tenant team. Discoverable via GET /_health on the mock, but
    # the Alpen run seeds a fixed TEAM_ID; the client + every external_id namespaces
    # on it (figma:{team_id}:event:...). Auth is any-non-empty-token (X-Figma-Token).
    team_id = "1357924680135792468"

    client = FigmaClient(
        base_url=fbase,
        api_base_url=fbase,
        api_token="alpen-figma-token",
        team_id=team_id,
    )
    # Monkeypatch the fetcher's opener so fetch_page_figma uses our mock-pointed client.
    fet._open_figma_client = lambda _install: _return(client)

    # ONBOARDING: enumerate the team's projects -> files (there is NO /v1/files list
    # and NO /events endpoint; list_files walks /v1/teams/{tid}/projects then
    # /v1/projects/{pid}/files). Each file becomes one figma_file_events shard.
    files = await client.list_files(team_id)

    # install row mirrors preflight._figma_records: team_id rides on the install so
    # _team_id_of(install, shard) resolves the external_id namespace.
    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "team_id": team_id,
        "base_url": "https://api.figma.com",
    }

    total = 0
    for f in files:
        file_key = str(f.get("key") or f.get("file_key") or "")
        if not file_key:
            continue
        shard = {
            "shard_kind": fet.SHARD_KIND_FILE_EVENTS,
            "file_key": file_key,
            "file_name": f.get("name"),
            "team_id": team_id,
            "installation_id": str(install["id"]),
            "event_cursor": None,
        }
        cursor = None
        records = []
        for _ in range(10_000):  # paginate every page (offset-walk over merged events)
            res = await fet.fetch_page_figma(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "figma", pool)
    return total


async def ingest_fireflies(pool) -> int:
    from services.ingest.ingestion.fetchers import fireflies as fet
    from services.ingest.integrations.fireflies.client import FirefliesClient

    # Mock host:port + prod base PATH from endpoints._PROD["fireflies_api"] ("/fireflies").
    # The client posts /graphql relative to api_base_url, so the effective URL is
    # http://localhost:7022/fireflies/graphql.
    fbase = rebased("fireflies", "fireflies_api")
    client = FirefliesClient(
        base_url=fbase,
        api_base_url=fbase,
        api_token="alpen-fireflies-token",   # mock accepts any non-empty Bearer
    )
    fet._open_fireflies_client = lambda _install: _return(client)

    # ONBOARDING: Fireflies has NO workspace/account list endpoint. The durable
    # install identity is the API-key owner returned by the GraphQL user() query;
    # get_workspace() maps user.id (or .email fallback) into workspace_id. That single
    # workspace_id is the one transcript shard's external_id namespace.
    ws = await client.get_workspace()
    workspace_id = str(ws.get("workspace_id") or ws.get("id") or ws.get("email") or "")

    install = {"id": uuid4(), "tenant_id": TENANT,
               "base_url": "https://api.fireflies.ai"}

    total = 0
    # ONE transcript shard per install (preflight _fireflies_records shape).
    shard = {"shard_kind": fet.SHARD_KIND_TRANSCRIPTS,
             "workspace_id": workspace_id,
             "installation_id": str(install["id"]),
             "transcript_cursor": None}

    cursor = None
    records = []
    for _ in range(10_000):                       # paginate every page
        res = await fet.fetch_page_fireflies(install, shard, cursor)
        records.extend(res.records)
        cursor = res.next_cursor
        if res.end_of_data:
            break
    total += await _write_records(records, "fireflies", pool)

    await client.aclose()
    return total


async def ingest_github(pool) -> int:
    import time
    from datetime import datetime, timedelta, timezone

    import asyncpg
    import httpx
    import jwt

    from services.ingest.ingestion.fetchers import github as fet
    from services.ingest.integrations.github.client import (
        CachedInstallationToken,
        GithubClient,
    )

    # github_api prod path is `https://api.github.com` (root, empty path), so
    # rebased() yields the bare mock host:port — the github mock mounts /app,
    # /installation, /repos at the root (NOT under a /github sub-path).
    gbase = rebased("github", "github_api")  # -> http://localhost:7003
    MOCK_ORGS_DSN = "postgresql://company_os:company_os@localhost:5432/mock_orgs"

    # ONBOARDING (1): resolve the seeded GitHub App (private key + numeric
    # app_id) and its installation ids for the Alpen tenant from mock_orgs.
    org_conn = await asyncpg.connect(MOCK_ORGS_DSN)
    try:
        app = await org_conn.fetchrow(
            """
            SELECT a.app_id, a.private_key
              FROM app_github.apps a
              JOIN org.runs r ON r.id = a.run_id
             WHERE r.fyralis_tenant_id = $1
             LIMIT 1
            """,
            TENANT,
        )
        if app is None:
            raise RuntimeError(
                "github: no app_github.apps row for the Alpen tenant in mock_orgs"
            )
        inst_rows = await org_conn.fetch(
            """
            SELECT i.installation_id
              FROM app_github.installations i
              JOIN app_github.apps a ON a.id = i.app_pk
              JOIN org.runs r ON r.id = a.run_id
             WHERE r.fyralis_tenant_id = $1
               AND i.suspended_at IS NULL
             ORDER BY i.installation_id
            """,
            TENANT,
        )
    finally:
        await org_conn.close()

    app_id = app["app_id"]
    private_key = app["private_key"]
    installation_ids = [str(r["installation_id"]) for r in inst_rows]
    print(f"  github: {len(installation_ids)} installation(s) discovered")

    async def _mint_ghs_token(installation_id: str) -> str:
        # Sign a short-lived App JWT (RS256, iss=app_id) with the seeded RSA
        # private key, then exchange it for a real `ghs_` installation token —
        # exactly the App-JWT flow the mock's auth expects.
        now = int(time.time())
        app_jwt = jwt.encode(
            {"iat": now - 60, "exp": now + 540, "iss": str(app_id)},
            private_key,
            algorithm="RS256",
        )
        async with httpx.AsyncClient(base_url=gbase, timeout=30.0) as hc:
            resp = await hc.post(
                f"/app/installations/{installation_id}/access_tokens",
                headers={
                    "Authorization": f"Bearer {app_jwt}",
                    "Accept": "application/vnd.github+json",
                },
            )
            resp.raise_for_status()
            return resp.json()["token"]

    # Class A repo-level list endpoints the fetcher + client support
    # (each maps to a webhook event the github:webhook handler consumes).
    # pr_reviews / check_runs are Class B fan-out and are out of scope here.
    EVENT_TYPES = ("issues", "pull_requests", "issue_comments", "commits")

    total = 0
    for installation_id in installation_ids:
        ghs_token = await _mint_ghs_token(installation_id)

        # Construct the REAL GithubClient pointed at the mock, and preset the
        # minted token in its in-process cache so list_repo_events authenticates
        # without re-minting (no GITHUB_APP_PRIVATE_KEY env needed here).
        client = GithubClient(
            pool=pool,
            api_base_url=gbase,
            backfill_installation_id=installation_id,
        )
        client._installation_tokens[installation_id] = CachedInstallationToken(
            token=ghs_token,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

        # Monkeypatch the fetcher's opener seam to hand back THIS client.
        fet._open_github_client = lambda _install, _c=client: _return(_c)

        # ONBOARDING (2): enumerate every repo accessible to the installation
        # (works in both selected/all-repos mode), fully paginated.
        repos = await client.list_repositories_for_backfill(installation_id)
        print(f"  github: install {installation_id} -> {len(repos)} repos")

        # The fetcher reads install['installation_id'] (via the opener's seam
        # the client is already bound, but the planner shape carries it too).
        install = {"id": uuid4(), "installation_id": installation_id}

        for full_name in repos:
            owner, _, name = full_name.partition("/")
            for event_type in EVENT_TYPES:
                # Shard shape copied EXACTLY from preflight._github_records.
                shard = {
                    "shard_kind": fet.SHARD_KIND_REPO_EVENTS,
                    "event_type": event_type,
                    "owner": owner,
                    "repo": name,
                    "repo_full_name": full_name,
                    "installation_id": installation_id,
                }
                cursor = None
                records: list = []
                for _ in range(10_000):  # page guard
                    res = await fet.fetch_page_github(install, shard, cursor)
                    records.extend(res.records)
                    cursor = res.next_cursor
                    if res.end_of_data:
                        break
                total += await _write_records(records, "github", pool)

        await client.aclose()

    return total


async def ingest_gmail(pool) -> int:
    """Gmail (Google DWD archetype): one mailbox_window shard per mailbox.

    Onboarding discovers mailbox emails from the mock_orgs DB
    (app_gmail.mailboxes for the latest org.runs row), since the live
    Gmail client only exposes message ops — directory enumeration would
    need a DirectoryClient + admin-domain wiring we don't have here.

    Auth: the real path is DWD JWT-bearer (GoogleHttpClient ->
    DwdTokenMinter). We build our OWN minter with a freshly-generated RSA
    key whose ServiceAccountKey.token_uri points at the mock's /token
    endpoint. The mock decodes the assertion WITHOUT verifying the
    signature (spammers/common/google_token.read_assertion) and mints a
    ya29.* bearer binding {sub=mailbox_email, scope}; require_mailbox
    resolves the mailbox from sub. Any RSA key works.
    """
    import asyncpg

    from services.ingest.ingestion.fetchers import gmail as fet
    from services.ingest.integrations.gmail.client import (
        GmailClient,
        GoogleHttpClient,
    )
    from services.ingest.integrations.gmail.dwd import (
        DwdTokenMinter,
        ServiceAccountKey,
    )

    # ---- mint a throwaway RSA service-account key (mock never verifies it) ----
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    _rsa = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    _pem = _rsa.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("ascii")
    sa_key = ServiceAccountKey(
        client_email="alpen-ingest@mock.iam.gserviceaccount.com",
        private_key_pem=_pem,
        private_key_id="alpen-mock-kid",
        token_uri=f"{base('gmail')}/token",  # mock /token (any-assertion DWD)
    )

    gbase = rebased("gmail", "gmail_api")  # http://localhost:7004/gmail/v1

    # One process-wide minter + http client; opener hands the SAME client to
    # every shard (close is a no-op so pagination across shards is safe).
    minter = DwdTokenMinter(sa_key)
    await minter.__aenter__()
    http = GoogleHttpClient(minter)
    await http.__aenter__()
    gmail_client = GmailClient(http, base_url=gbase)

    async def _close() -> None:
        return None

    async def _open(_install):
        return gmail_client, _close

    fet._open_gmail_client = _open

    # ---- onboarding: enumerate mailbox emails from the mock_orgs DB ----
    conn = await asyncpg.connect(
        "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    )
    try:
        rows = await conn.fetch(
            """
            SELECT m.email
              FROM app_gmail.mailboxes m
              JOIN app_gmail.customers c ON c.id = m.customer_pk
              JOIN org.runs r ON r.id = c.run_id
             WHERE r.id = (SELECT id FROM org.runs ORDER BY created_at DESC LIMIT 1)
             ORDER BY m.email
            """
        )
    finally:
        await conn.close()

    emails = [r["email"] for r in rows]
    print(f"  gmail: {len(emails)} mailboxes discovered")

    # install row: scope alias 'gmail.metadata' (CHECK-valid; get_message uses
    # format=metadata, which still returns payload.headers incl. Message-ID).
    install = {"id": uuid4(), "tenant_id": TENANT, "scope": "gmail.metadata"}

    total = 0
    try:
        for email in emails:
            shard = {
                "shard_kind": fet.SHARD_KIND_MAILBOX_WINDOW,
                "mailbox_email": email,
            }
            cursor = None
            records: list = []
            for _ in range(10_000):  # page guard
                res = await fet.fetch_page_gmail(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "gmail", pool)
    finally:
        await http.__aexit__(None, None, None)
        await minter.__aexit__(None, None, None)
    return total


async def ingest_grafana(pool) -> int:
    from services.ingest.ingestion.fetchers import grafana as fet
    from services.ingest.integrations.grafana.client import GrafanaClient

    # Alpen annotations span 2024-2026; the fetcher's default 90-day backfill
    # window would clip history to ~100 of 797. Widen it (mirrors Mercury's floor).
    os.environ["GRAFANA_BACKFILL_WINDOW_DAYS"] = "3650"

    # rebased("grafana","grafana_api") -> mock host:port + prod path. The grafana_api
    # prod base is intentionally empty (""), so this yields the bare mock origin
    # "http://localhost:7010"; the client appends "/api/annotations" itself.
    gbase = rebased("grafana", "grafana_api")
    client = GrafanaClient(
        base_url=gbase,
        api_base_url=gbase,
        api_token="glsa_alpen-grafana-token",  # any-token mock
    )
    fet._open_grafana_client = lambda _install: _return(client)

    # ONBOARDING: cheap connectivity/credential probe + org identity. Grafana
    # annotations are ORG-WIDE, so there is exactly ONE org-annotations shard per
    # install (not one-per-resource). No list endpoint to enumerate shards.
    org = await client.get_org()
    print(f"  grafana: org '{org.get('name')}' (id={org.get('id')}) — 1 org-annotations shard")

    # CRITICAL: install['base_url'] must be the REAL instance host, NOT the mock
    # localhost — the fetcher's _instance_of(install) derives the host that becomes
    # the record's _fyralis_instance, which the handler folds into the external_id
    # namespace (grafana:{instance}:annotation:{id}:{time}). Ground truth was seeded
    # with INSTANCE_HOST="alpenlabs.grafana.net" (seed.py BASE_URL), so we must match
    # it for dedup/identity to line up. The client already points at the mock via
    # api_base_url=gbase, so base_url here only feeds the external_id namespace.
    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "base_url": "https://alpenlabs.grafana.net",
    }

    # Full backfill: no warm-start cursor -> the fetcher walks the window
    # newest-first, advancing page_to_ms backward until a short page (end_of_data).
    shard = {"shard_kind": fet.SHARD_KIND_ORG_ANNOTATIONS}
    cursor = None
    records: list = []
    for _ in range(10_000):  # page guard
        res = await fet.fetch_page_grafana(install, shard, cursor)
        records.extend(res.records)
        cursor = res.next_cursor
        if res.end_of_data:
            break

    return await _write_records(records, "grafana", pool)


async def ingest_gusto(pool) -> int:
    import asyncpg as _asyncpg

    from services.ingest.ingestion.fetchers import gusto as fet
    from services.ingest.integrations.gusto.client import GustoClient, DEFAULT_ENTITIES

    # rebased("gusto","gusto_api") -> mock host:port + prod base PATH. The prod
    # host is https://api.gusto.com (no path), so this is just the bare mock
    # origin; GustoClient prepends /v1/companies/{company_uuid}/... itself.
    gbase = rebased("gusto", "gusto_api")

    # --- ONBOARDING: discover the company_uuid (the only shard root) ---------
    # The Gusto mock exposes NO list-companies endpoint (GET /v1/companies/{id}
    # 404s unless you already know the id). The mock is single-tenant per run;
    # the seeded company_uuid lives in mock_orgs.app_gusto.companies. Query it
    # there, falling back to the seed-stable constant if unreachable.
    company_uuid = None
    try:
        conn = await _asyncpg.connect(
            "postgresql://company_os:company_os@localhost:5432/mock_orgs"
        )
        try:
            company_uuid = await conn.fetchval(
                "SELECT company_uuid FROM app_gusto.companies "
                "ORDER BY created_at DESC LIMIT 1"
            )
        finally:
            await conn.close()
    except Exception:  # noqa: BLE001
        company_uuid = None
    if not company_uuid:
        # spammers/gusto/seed.py COMPANY_UUID (seed-stable Alpen Labs company).
        company_uuid = "a1b2c3d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d"
    company_uuid = str(company_uuid)
    print(f"  gusto: company_uuid={company_uuid}, entities={list(DEFAULT_ENTITIES)}")

    # Build the REAL client pointed at the mock. api_base_url wins over base_url
    # so reads hit the mock; any non-empty Bearer is accepted by the mock.
    client = GustoClient(
        base_url=gbase,
        api_base_url=gbase,
        company_uuid=company_uuid,
        access_token="alpen-gusto-token",  # any-token mock
    )
    # Monkeypatch the fetcher opener -> (client, close). _return yields the same
    # (client, _noop) tuple shape _open_gusto_client returns in production.
    fet._open_gusto_client = lambda _install: _return(client)

    # install dict shape copied from preflight._gusto_records: fetch_page_gusto
    # reads install["company_uuid"] (via _company_uuid_of) only.
    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "company_uuid": company_uuid,
        "base_url": gbase,
    }

    total = 0
    # Entity-type-sharded source: one gusto_entity shard per entity kind.
    for entity_type in DEFAULT_ENTITIES:
        shard = {
            "shard_kind": fet.SHARD_KIND_ENTITY,
            "entity_type": entity_type,
        }
        cursor = None
        records: list = []
        for _ in range(10_000):  # page guard
            res = await fet.fetch_page_gusto(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "gusto", pool)
    return total


async def ingest_hibob(pool) -> int:
    from services.ingest.ingestion.fetchers import hibob as fet
    from services.ingest.integrations.hibob.client import HibobClient

    # Mock host:port + the prod base PATH from endpoints._PROD["hibob_api"].
    hbase = rebased("hibob", "hibob_api")

    # The mock authenticates ANY well-formed Basic(id:token) — it does not match a
    # provisioned credential (single-tenant per run). Any non-empty id + token work.
    client = HibobClient(
        base_url=hbase,
        api_base_url=hbase,
        company_id="alpen-hibob",            # only used to stamp _fyralis_company_id
        service_user_id="alpenlabs-svc-ingest",
        token="alpen-hibob-token",
    )

    # Monkeypatch the fetcher's client opener (returns (client, close) coroutine).
    fet._open_hibob_client = lambda _install: _return(client)

    # ONBOARDING: HiBob is an entity-type-sharded source (one shard per entity
    # type for the single company). The live mock serves exactly three entity
    # endpoints — employee (POST /v1/people/search), payroll (GET
    # /v1/bulk/people/salaries) and timeoff (GET /v1/timeoff/requests/changes).
    # DEFAULT_ENTITIES also lists "lifecycle" (GET /v1/bulk/people/work) but that
    # route returns 404 on the mock, so exclude it to avoid HibobApiError.
    entity_types = ["employee", "payroll", "timeoff"]

    # install/shard shape copied from preflight._hibob_records; IDs filled for the
    # live run rather than the fixture.
    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "company_id": "alpen-hibob",
        "service_user_id": "alpenlabs-svc-ingest",
        "base_url": hbase,
    }

    total = 0
    for entity_type in entity_types:
        shard = {
            "shard_kind": fet.SHARD_KIND_ENTITY,
            "entity_type": entity_type,
            "company_id": "alpen-hibob",
            "installation_id": str(install["id"]),
            "updated_cursor": None,
        }
        cursor = None
        records = []
        for _ in range(10_000):                      # paginate every page
            res = await fet.fetch_page_hibob(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "hibob", pool)
    return total


async def _jira_install_creds():
    """Real-token source: the Jira mock validates HTTP Basic auth
    (base64(account_email:api_token)) against the seeded install row, so we must
    use the EXACT email + token the mock expects. There is one install per run;
    the latest run is the active one. Also return the real site base_url so the
    fetcher's _site_of() namespaces external_ids identically to the live-webhook
    twin."""
    import asyncpg
    conn = await asyncpg.connect(
        "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    )
    try:
        row = await conn.fetchrow(
            "SELECT account_email, api_token, base_url "
            "FROM app_jira.installations ORDER BY run_id DESC LIMIT 1"
        )
    finally:
        await conn.close()
    if row is None:
        raise RuntimeError("jira: no app_jira.installations row in mock_orgs")
    return row["account_email"], row["api_token"], row["base_url"]


async def ingest_jira(pool) -> int:
    from services.ingest.ingestion.fetchers import jira as fet
    from services.ingest.integrations.jira.client import JiraClient

    # Real-token: pull the Basic-auth creds + site base_url the mock expects.
    account_email, api_token, site_base_url = await _jira_install_creds()

    # Client hits the mock (api_base_url=rebased) but presents the real creds.
    # jira_api prod path is "" -> rebased("jira","jira_api") == "http://localhost:7008",
    # which is exactly the host the client appends /rest/api/3/... to.
    jbase = rebased("jira", "jira_api")
    client = JiraClient(
        base_url=jbase,
        api_base_url=jbase,
        account_email=account_email,
        api_token=api_token,
        tenant_id=TENANT,
    )
    fet._open_jira_client = lambda _install: _return(client)

    # ONBOARDING: enumerate the projects visible to the token (startAt paging).
    project_keys: list = []
    start_at = 0
    for _ in range(10_000):  # page guard
        projects, next_start, _total = await client.list_projects(start_at=start_at)
        for p in projects:
            key = p.get("key")
            if isinstance(key, str) and key:
                project_keys.append(key)
        if next_start is None:
            break
        start_at = next_start
    print(f"  jira: {len(project_keys)} projects discovered")

    # The fetcher reads only install["base_url"] (for _site_of external_id
    # namespacing) — use the REAL site URL so backfill records dedup against
    # their live-webhook twins.
    install = {"id": uuid4(), "tenant_id": TENANT, "base_url": site_base_url}

    total = 0
    for project_key in project_keys:
        # Mirrors the fetcher's SHARD_KIND_PROJECT_ISSUES contract: one shard
        # per project, FULL backfill (no updated_cursor -> walks the whole
        # project, ORDER BY updated ASC). project_key fills from real onboarding.
        shard = {
            "shard_kind": fet.SHARD_KIND_PROJECT_ISSUES,
            "project_key": project_key,
        }
        cursor = None
        records: list = []
        for _ in range(10_000):  # token-paginate every page until is_last
            res = await fet.fetch_page_jira(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "jira", pool)
    return total


async def ingest_linkedin(pool) -> int:
    """Backfill LinkedIn (Community Management) for the Alpen run via the mock.

    Onboarding: LinkedIn is a single-organization (Carta firm_id-equivalent)
    source — the mock seeds exactly ONE org per run and gates every /rest finder
    on `author/organizationalEntity == org_urn`. We discover that org URN from
    the mock's root `/_health` probe (it reports `org_urn`), build one install
    scoped to it, then shard on the three Community-Management streams the
    fetcher knows (DEFAULT_ENTITIES = post / share_statistics /
    follower_statistics), one `linkedin_entity` shard each.
    """
    import httpx

    from services.ingest.ingestion.fetchers import linkedin as fet
    from services.ingest.integrations.linkedin.client import (
        LinkedinClient,
        DEFAULT_ENTITIES,
    )

    # --- ONBOARDING: discover the seeded organization URN from the live mock ---
    # The mock is single-org/run; its root /_health echoes the org_urn its /rest
    # finders will match. (base("linkedin") is the mock host:port with NO path;
    # the /rest prefix is added by rebased(...) for the client below.)
    org_urn = ""
    async with httpx.AsyncClient(timeout=30.0) as probe:
        resp = await probe.get(base("linkedin") + "/_health")
        if resp.status_code // 100 == 2:
            org_urn = str((resp.json() or {}).get("org_urn") or "")
    if not org_urn:
        # Fallback to the seed-stable identity (spammers/linkedin/seed.py).
        org_urn = "urn:li:organization:80411507"

    # --- CLIENT: point the REAL LinkedinClient at the mock's /rest surface -----
    # rebased("linkedin", "linkedin_api") = mock host:port + the prod base PATH
    # ("/rest" from endpoints._PROD["linkedin_api"]). api_base_url wins over
    # base_url inside the client, so both are set to the rebased mock URL.
    mbase = rebased("linkedin", "linkedin_api")
    client = LinkedinClient(
        base_url=mbase,
        api_base_url=mbase,
        organization_urn=org_urn,
        access_token="spam-linkedin",   # any-token mock; value is ignored
        tenant_id=TENANT,
    )
    # The opener takes the install record and returns (client, close); we ignore
    # the install and hand back our pre-built client.
    fet._open_linkedin_client = lambda _install: _return(client)

    # The fetcher reads install["organization_urn"] (via _org_urn_of) for the
    # author/entity URN — match the EXACT key the fetcher/builder read.
    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "organization_urn": org_urn,
        "base_url": mbase,
    }

    total = 0
    for entity_type in DEFAULT_ENTITIES:
        # shard_identifier shape per fetch_page_linkedin: {"entity_type": ...};
        # FULL backfill so we omit the optional "updated_cursor" warm-start.
        shard = {"entity_type": entity_type}
        cursor = None
        records = []
        for _ in range(10_000):
            res = await fet.fetch_page_linkedin(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "linkedin", pool)
    return total


async def ingest_miro(pool) -> int:
    from services.ingest.ingestion.fetchers import miro as fet
    from services.ingest.integrations.miro.client import MiroClient

    mbase = rebased("miro", "miro_api")  # http://localhost:7017/v2
    client = MiroClient(
        base_url=mbase,
        api_base_url=mbase,
        api_token="alpen-miro-token",  # any-token mock
    )
    fet._open_miro_client = lambda _install: _return(client)

    # The org id namespaces every observation's external_id (miro:{org_id}:...).
    # It is NOT on the board DTO (that carries team.id); read it from the mock's
    # /_health probe so external_id parity matches the seeded corpus. Fall back
    # to None (the fetcher then namespaces by board_id) if the probe fails.
    org_id = None
    try:
        import httpx as _httpx
        async with _httpx.AsyncClient(timeout=30.0) as hc:
            resp = await hc.get(f"{base('miro')}/_health")
            if resp.status_code // 100 == 2:
                body = resp.json()
                oid = body.get("org_id")
                if isinstance(oid, str) and oid:
                    org_id = oid
    except Exception:
        org_id = None

    # ONBOARDING: GET /v2/boards (offset-paged) enumerates every board the org
    # token can see; one shard per board.
    boards = await client.list_boards()
    print(f"  miro: {len(boards)} boards discovered (org_id={org_id})")

    install = {"id": uuid4(), "tenant_id": TENANT,
               "base_url": "https://api.miro.com/v2"}
    total = 0
    for board in boards:
        board_id = board.get("id") or board.get("board_id")
        if not board_id:
            continue
        board_id = str(board_id)
        shard = {"shard_kind": fet.SHARD_KIND_BOARD_ITEMS,
                 "board_id": board_id,
                 "board_name": board.get("name"),
                 "org_id": org_id,
                 "installation_id": str(install["id"]),
                 "item_cursor": None}
        cursor = None
        records: list = []
        for _ in range(10_000):  # page guard — items are cursor-paginated
            res = await fet.fetch_page_miro(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "miro", pool)
    return total


async def ingest_notion(pool) -> int:
    import asyncpg as _asyncpg
    from services.ingest.ingestion.fetchers import notion as fet
    from services.ingest.integrations.notion.client import NotionClient

    # Notion is a real-token mock: the mock's auth.authed() only accepts the
    # run's seeded bot token, so we pull it (and the workspace id) from the
    # mock_orgs seed DB rather than presetting an arbitrary token.
    conn = await _asyncpg.connect(
        "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    )
    try:
        row = await conn.fetchrow(
            "SELECT bot_token, workspace_id FROM app_notion.integrations "
            "ORDER BY run_id DESC LIMIT 1"
        )
    finally:
        await conn.close()
    if row is None:
        raise RuntimeError("notion: no app_notion.integrations row in mock_orgs")
    bot_token = row["bot_token"]
    workspace_id = str(row["workspace_id"])

    # rebased("notion","notion_api") = mock host:port + prod base PATH. The
    # prod base https://api.notion.com has no path, so this is just
    # http://localhost:7006; the client appends each /v1/... request path.
    nbase = rebased("notion", "notion_api")
    client = NotionClient(
        bot_token=bot_token,
        workspace_id=workspace_id,
        api_base_url=nbase,
    )
    fet._open_notion_client = lambda _install: _return(client)

    install = {"id": uuid4(), "tenant_id": TENANT}

    # ONBOARDING: enumerate every database in the workspace via search; each
    # becomes one notion_database shard (its rows -> blocks -> comments tree).
    # Then ONE notion_page_tree shard covers all LOOSE pages (pages not owned
    # by a database) -> their blocks -> comments. Together these cover the
    # whole workspace with no double-counting (the page_tree walk skips
    # _is_database_row pages, which the database shards already emit).
    databases: list = []
    start_cursor = None
    for _ in range(10_000):  # page guard
        dbs, next_cursor, has_more = await client.search(
            object_filter="database", start_cursor=start_cursor,
        )
        databases.extend(dbs)
        start_cursor = next_cursor
        if not (has_more and next_cursor):
            break
    print(f"  notion: {len(databases)} databases discovered (+1 page_tree shard)")

    # Build the shard list: one DATABASE shard per database, plus the single
    # PAGE_TREE shard. fetch_page_notion reads shard["shard_kind"],
    # shard["workspace_id"] (grounding) and shard["database_id"] (db_rows only;
    # ignored by the page_tree walk).
    shards: list[dict] = [
        {
            "shard_kind": fet.SHARD_KIND_DATABASE,
            "workspace_id": workspace_id,
            "database_id": str(db.get("id")),
        }
        for db in databases
        if db.get("id")
    ]
    shards.append({
        "shard_kind": fet.SHARD_KIND_PAGE_TREE,
        "workspace_id": workspace_id,
        "database_id": None,
    })

    total = 0
    for shard in shards:
        cursor = None
        records: list = []
        # The Notion cursor is a resumable work stack (db rows -> per-row
        # blocks + comments, paginated). Loop until end_of_data drains it.
        for _ in range(1_000_000):  # page guard (deep trees, many list calls)
            res = await fet.fetch_page_notion(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "notion", pool)
    return total


async def ingest_quickbooks(pool) -> int:
    """QuickBooks Online: entity-type-sharded (Invoice/Bill/BillPayment/Payment),
    offset-paginated via the QBO query endpoint, scoped to one company realm.

    IMPORTANT base-url note: the QBO mock serves its routes at the bare host
    (GET {host}/v3/company/{realm}/query) — it is NOT mounted under a /quickbooks
    sub-path. The real QuickBooksClient itself prepends the full realm-scoped
    /v3/company/{realm_id}/query path, so api_base_url must be JUST the mock
    host:port. We therefore use base("quickbooks") here, NOT
    rebased("quickbooks","quickbooks_api") — the latter would append the prod
    path "/quickbooks" and every call would 404. (Verified live: bare host -> 200
    with QueryResponse; /quickbooks -> 404.)
    """
    import httpx

    from services.ingest.ingestion.fetchers import quickbooks as fet
    from services.ingest.integrations.quickbooks.client import (
        QuickBooksClient,
        DEFAULT_ENTITIES,
    )

    qb_base = base("quickbooks")  # bare host:port; client adds /v3/company/{realm}/...

    # ---- ONBOARDING: discover the run's company realm_id ----------------
    # Primary: the mock's run-aware /_health returns the active run's realm_id.
    # Fallback: query mock_orgs (app_quickbooks.companies) if /_health is absent.
    realm_id = None
    try:
        async with httpx.AsyncClient(timeout=15) as probe:
            h = await probe.get(f"{qb_base}/_health")
            if h.status_code == 200:
                realm_id = (h.json() or {}).get("realm_id")
    except Exception:  # noqa: BLE001
        realm_id = None
    if not realm_id:
        import asyncpg

        conn = await asyncpg.connect(
            "postgresql://company_os:company_os@localhost:5432/mock_orgs"
        )
        try:
            realm_id = await conn.fetchval(
                "SELECT realm_id FROM app_quickbooks.companies "
                "ORDER BY created_at DESC LIMIT 1"
            )
        finally:
            await conn.close()
    if not realm_id:
        raise RuntimeError("quickbooks: could not discover a realm_id")
    realm_id = str(realm_id)
    print(f"  quickbooks: realm {realm_id}; entities {DEFAULT_ENTITIES}")

    # ---- Build the REAL client pointed at the mock ----------------------
    client = QuickBooksClient(
        base_url=qb_base,
        api_base_url=qb_base,          # spammer override wins over base_url
        realm_id=realm_id,
        access_token="alpen-quickbooks-token",  # any-token mock
        http_client=httpx.AsyncClient(timeout=30),
    )
    # Monkeypatch the fetcher's opener (must return a coroutine -> (client, close)).
    fet._open_quickbooks_client = lambda _install: _return(client)

    # The fetcher reads install["realm_id"] (via _realm_id_of) and
    # shard_identifier["entity_type"] (the four transactional entities).
    install = {"id": uuid4(), "tenant_id": TENANT, "realm_id": realm_id}

    total = 0
    try:
        for entity_type in DEFAULT_ENTITIES:
            shard = {
                "shard_kind": fet.SHARD_KIND_ENTITY,
                "entity_type": entity_type,
            }
            cursor = None
            records: list = []
            for _ in range(10_000):  # page guard (offset pagination)
                res = await fet.fetch_page_quickbooks(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "quickbooks", pool)
    finally:
        await client.aclose()
    return total


async def ingest_ramp(pool) -> int:
    from services.ingest.ingestion.fetchers import ramp as fet
    from services.ingest.integrations.ramp.client import RampClient, DEFAULT_ENTITIES

    rbase = rebased("ramp", "ramp_api")  # -> http://localhost:7018/developer/v1

    # business_id is only the external_id namespace (not auth). Prefer the live
    # seeded org row; fall back to the seed-stable Alpen constant.
    business_id = "11111111-2222-4333-8444-555566667777"
    try:
        import asyncpg
        conn = await asyncpg.connect(
            "postgresql://company_os:company_os@localhost:5432/mock_orgs"
        )
        try:
            row = await conn.fetchrow(
                "SELECT business_id FROM app_ramp.organizations "
                "ORDER BY created_at DESC LIMIT 1"
            )
            if row and row["business_id"]:
                business_id = str(row["business_id"])
        finally:
            await conn.close()
    except Exception:  # noqa: BLE001 — mock accepts any token; constant is fine
        pass

    # The mock accepts ANY non-empty Bearer; a preset access_token skips OAuth.
    client = RampClient(
        base_url=rbase,
        api_base_url=rbase,
        business_id=business_id,
        access_token="ramp-backfill-token",
    )
    fet._open_ramp_client = lambda _install: _return(client)

    # Install/shard shape copied from preflight._ramp_records: the fetcher reads
    # install["business_id"] (external_id namespace) and shard["entity_type"].
    install = {
        "id": uuid4(),
        "tenant_id": TENANT,
        "business_id": business_id,
        "base_url": "https://api.ramp.com/developer/v1",
    }

    total = 0
    # Entity-type-sharded onboarding: one ramp_entity shard per VERIFIED stream
    # (transaction / reimbursement / card / user) — the client's DEFAULT_ENTITIES.
    for entity_type in DEFAULT_ENTITIES:
        shard = {
            "shard_kind": fet.SHARD_KIND_ENTITY,
            "entity_type": entity_type,
        }
        cursor = None
        records = []
        for _ in range(10_000):  # paginate the keyset page.next walk to EOF
            res = await fet.fetch_page_ramp(install, shard, cursor)
            records.extend(res.records)
            cursor = res.next_cursor
            if res.end_of_data:
                break
        total += await _write_records(records, "ramp", pool)
    return total


async def ingest_signal(pool) -> int:
    """Signal (gateway / Telegram archetype): shard per thread, backward-paged
    history. ONE record per message.

    IMPORTANT — why this builds a bespoke HTTP adapter instead of the real
    SignalClient (the Mercury template's "construct the real client" step):
    services/ingest/integrations/signal/client.py is a DOCUMENTED STUB. Signal
    has no server API; the real client's transport is a signal-cli JSON-RPC
    SOCKET (SIGNAL_JSONRPC_ENDPOINT) and its `_connect()` deliberately RAISES
    SignalApiError until an operator wires a daemon — it cannot talk HTTP and
    cannot reach the mock. The saas-api-mocks signal mock (port 7025) instead
    serves the SignalClient METHOD CONTRACT over HTTP (POST /v1/iter_threads,
    POST /v1/get_history) returning genuine signal-cli envelopes. So the adapter
    below implements exactly the two methods the fetcher's `_open_signal_client`
    seam needs (iter_threads + get_history), translating envelopes -> the raw
    message dict `integrations/signal/records.build_message_record` consumes.

    Shape notes:
      * The mock's thread_id is a STRING (contact uuid | base64 groupId), but
        fetch_page_signal REQUIRES an int thread_id (it guards
        `isinstance(thread_id, int)` and that int is the external_id thread grain
        `signal:{install}:{thread}:{id}:none`). So each thread is assigned a
        STABLE deterministic int (blake2b of the string id); the adapter keeps an
        int->string map to resolve get_history back to the mock's id.
      * A Signal message has NO separate integer id — its id IS its `timestamp`
        in MILLISECONDS. So offset_id/min_id (the fetcher's int cursor) map
        IDENTITY-wise onto the mock's offset_ts/min_ts. Verified end-to-end
        against the live mock: 12 threads, 2715 records, exact pagination (1207
        msgs in the hero group across 121 pages of 10, all unique).
    """
    import hashlib
    import httpx

    from services.ingest.ingestion.fetchers import signal as fet

    sbase = base("signal")  # http://localhost:7025 — mock serves /v1/... directly

    # Session credential: prefer the seeded value from mock_orgs, else the
    # spammer-mode constant preset (seed.py SESSION_STRING).
    session = "spam-signal"
    try:
        import asyncpg
        conn = await asyncpg.connect(
            "postgresql://company_os:company_os@localhost:5432/mock_orgs"
        )
        try:
            row = await conn.fetchrow(
                "SELECT session_string FROM app_signal.installations "
                "WHERE disabled_at IS NULL ORDER BY created_at DESC LIMIT 1"
            )
            if row and row["session_string"]:
                session = str(row["session_string"])
        finally:
            await conn.close()
    except Exception:  # noqa: BLE001 — fall back to the known preset
        pass

    def _str_to_int(s: str) -> int:
        # Stable 56-bit int from the mock's string thread_id. Deterministic ->
        # the external_id thread grain is consistent across runs / dedup.
        return int.from_bytes(
            hashlib.blake2b(s.encode("utf-8"), digest_size=7).digest(), "big"
        )

    class _SignalMockAdapter:
        """Implements the fetcher's get_history/iter_threads surface over the
        mock's HTTP method-contract (see ingest_signal docstring)."""

        def __init__(self, base_url: str, session_str: str) -> None:
            self._base = base_url.rstrip("/")
            self._headers = {
                "Authorization": f"Bearer {session_str}",
                "Content-Type": "application/json",
            }
            self._client = httpx.AsyncClient(timeout=60.0)
            self._int_to_str: dict[int, str] = {}

        async def iter_threads(self, *, limit: int = 200) -> list:
            r = await self._client.post(
                f"{self._base}/v1/iter_threads", headers=self._headers, json={}
            )
            r.raise_for_status()
            out = []
            for t in r.json().get("threads", []) or []:
                sid = t.get("thread_id")
                if not isinstance(sid, str):
                    continue
                iid = _str_to_int(sid)
                self._int_to_str[iid] = sid
                out.append({
                    "thread_id": iid,
                    "thread_kind": t.get("thread_kind") or "direct",
                    "title": t.get("thread_title"),
                })
            return out[:limit]

        @staticmethod
        def _env_to_raw(env: dict) -> dict:
            # signal-cli envelope -> the raw dict build_message_record consumes.
            ts = int(env.get("timestamp") or 0)
            sync = env.get("syncMessage") or {}
            sent = sync.get("sentMessage")
            data = env.get("dataMessage")
            is_out = sent is not None  # own/linked-device send (out=True)
            body = (sent or data or {}).get("message") or ""
            from_id = None
            src_uuid = env.get("sourceUuid")
            if not is_out and isinstance(src_uuid, str):
                # build_message_record wants from_id={"user_id": int}; derive a
                # stable int sender id from the source uuid (attribution only —
                # NOT part of the external_id, so a stable surrogate is fine).
                from_id = {"user_id": _str_to_int(src_uuid)}
            return {
                "id": ts,                         # message id == timestamp (ms)
                "date": ts // 1000 if ts else None,   # epoch SECONDS
                "edit_date": None,                # Signal v1: no edits
                "message": body,
                "out": is_out,
                "from_id": from_id,
                "sender_username": env.get("sourceName"),
            }

        async def get_history(
            self,
            *,
            thread_id: int,
            thread_kind: str = "direct",
            offset_id: int = 0,
            min_id: int = 0,
            limit: int = 100,
        ) -> tuple:
            sid = self._int_to_str.get(thread_id)
            if sid is None:
                return [], None, True
            page_limit = min(100, max(1, int(limit)))
            payload = {
                "thread": {"thread_id": sid},
                "offset_ts": int(offset_id or 0),  # id==ts -> identity mapping
                "min_ts": int(min_id or 0),
                "limit": page_limit,
            }
            r = await self._client.post(
                f"{self._base}/v1/get_history", headers=self._headers, json=payload
            )
            r.raise_for_status()
            envs = r.json().get("messages", []) or []
            messages = [self._env_to_raw(e) for e in envs]
            ids = [
                m["id"] for m in messages
                if isinstance(m.get("id"), int) and m["id"] > 0
            ]
            next_offset_id = min(ids) if ids else None
            is_last = len(messages) < page_limit or next_offset_id is None
            return messages, next_offset_id, is_last

        async def aclose(self) -> None:
            await self._client.aclose()

    client = _SignalMockAdapter(sbase, session)
    fet._open_signal_client = lambda _install: _return(client)

    try:
        # ONBOARDING: discover the linked account's threads -> one shard each.
        threads = await client.iter_threads(limit=1000)
        print(f"  signal: {len(threads)} threads discovered")
        install = {"id": uuid4(), "tenant_id": TENANT}
        total = 0
        for t in threads:
            shard = {
                "shard_kind": fet.SHARD_KIND_THREAD_HISTORY,
                "thread_id": t["thread_id"],            # int (required by fetcher)
                "thread_kind": t.get("thread_kind") or "direct",
                "thread_title": t.get("title"),
                "installation_id": str(install["id"]),
                "offset_id_cursor": None,               # full backfill (no warm start)
            }
            cursor = None
            records: list = []
            for _ in range(100_000):  # page guard (hero group ~1200 msgs)
                res = await fet.fetch_page_signal(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "signal", pool)
        return total
    finally:
        await client.aclose()


async def ingest_slack(pool) -> int:
    from services.ingest.ingestion.fetchers import slack as fet
    from services.ingest.integrations.slack.client import SlackClient

    # The standalone Slack mock mounts its routes at /api/conversations.* (NOT
    # /slack/api). rebased("slack","slack_api") grafts the PATH of the prod base
    # https://slack.com/api -> "/api" onto the mock port, giving exactly
    # http://localhost:7001/api — the URL the mock serves. (The endpoints _ENV
    # "/slack/api" sub-path convention is for a unified-gateway mode and 404s on
    # the per-source mock; do NOT use it here.)
    sbase = rebased("slack", "slack_api")  # -> http://localhost:7001/api

    # ONBOARDING (real-token): the SlackClient bot token is lazily resolved from
    # the secret store, which we don't have in the driver. Pull the REAL seeded
    # xoxb token (+ team_id) straight from the mock_orgs DB and preset it so the
    # client never touches pool/secret_store. One workspace per run (Alpen Labs).
    conn = await asyncpg.connect(
        "postgresql://company_os:company_os@localhost:5432/mock_orgs"
    )
    try:
        run_id = await conn.fetchval(
            "SELECT id FROM org.runs ORDER BY created_at DESC LIMIT 1"
        )
        ws_rows = await conn.fetch(
            "SELECT team_id, bot_token "
            "  FROM app_slack.workspaces WHERE run_id = $1",
            run_id,
        )
    finally:
        await conn.close()
    print(f"  slack: {len(ws_rows)} workspace(s) discovered")

    total = 0
    for ws in ws_rows:
        team_id = str(ws["team_id"])
        bot_token = str(ws["bot_token"])

        # Real SlackClient pointed at the mock; bot token preset so _resolve_token
        # short-circuits (no pool/secret_store needed).
        client = SlackClient(
            pool=None,
            secret_store=None,
            tenant_id=TENANT,
            installation_row_id=uuid4(),
            team_id=team_id,
            base_url=sbase,
        )
        client._bot_token = bot_token

        # Monkeypatch the fetcher's bot-channel opener to return this client.
        # Bind per-iteration via a default arg to avoid late-binding closure bugs
        # if there were ever >1 workspace.
        fet._open_slack_client = lambda _install, _c=client: _return(_c)

        # ONBOARDING: enumerate the workspace's public channels (planner shard
        # source). The bot is a member of all 15 seeded public channels, so each
        # serves conversations.history. (DM/im/mpim shards need an xoxp USER
        # token + the _open_slack_user_client seam — out of scope for the bot
        # backfill path here; see risks.)
        channels = await client.conversations_list()
        print(f"  slack: team {team_id} -> {len(channels)} public channels")

        # Install/shard dicts mirror preflight._slack_records exactly, with IDs
        # filled from real onboarding.
        install = {"id": uuid4(), "tenant_id": TENANT, "installation_id": team_id}
        for ch in channels:
            channel_id = ch.get("id")
            if not channel_id:
                continue
            shard = {
                "shard_kind": fet.SHARD_KIND_CHANNEL_WINDOW,
                "channel_id": channel_id,
                "team_id": team_id,
                "installation_id": team_id,
            }
            cursor = None
            records: list = []
            for _ in range(10_000):  # page guard
                res = await fet.fetch_page_slack(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "slack", pool)

        await client.aclose()
    return total


async def ingest_telegram(pool) -> int:
    # Telegram is special: the REAL integration client
    # (services/ingest/integrations/telegram/client.py) is a Telethon/MTProto
    # client whose _connect() imports telethon and dials the binary MTProto
    # transport — it cannot talk to the saas-api-mocks HTTP shim. The mock instead
    # reproduces the Telethon METHOD contract over HTTP (POST /messages.getDialogs,
    # POST /messages.getHistory) at the BARE port root (there is no telegram_api
    # key in endpoints._PROD and no /telegram sub-path), authenticated by the
    # persisted StringSession ('spam-telegram', the spammer preset) presented as
    # `Authorization: Session <s>`.
    #
    # So we use a thin in-process HTTP client that implements exactly the two
    # methods the backfill fetcher calls — iter_dialogs() for onboarding and
    # get_history(...) for paging — returning the SAME shapes the real client's
    # Telethon->dict conversion yields (from_id flattened to {"user_id": int},
    # epoch-second dates), so fetchers/telegram.py + build_message_record + the
    # real handler run unmodified.
    import httpx

    from services.ingest.ingestion.fetchers import telegram as fet

    tbase = base("telegram")  # http://localhost:7024 — mock at the bare root
    SESSION = "spam-telegram"  # the seeded install's session_string / spammer preset

    class _TelegramMockClient:
        """HTTP stand-in for integrations.telegram.client.TelegramClient that
        speaks the mock's MTProto-method-over-HTTP contract. Implements only the
        backfill surface (iter_dialogs, get_history) the fetcher exercises."""

        def __init__(self, base_url: str, session: str) -> None:
            self._base = base_url.rstrip("/")
            self._headers = {"Authorization": f"Session {session}"}
            self._http = httpx.AsyncClient(timeout=30.0)

        @staticmethod
        def _flatten(msg: dict) -> dict:
            # Mirror integrations.telegram.client._message_to_dict: collapse the TL
            # `from_id` Peer to {"user_id": int} (or None for channel-broadcast /
            # self-sent), keep id/date/edit_date/message/out as the raw record the
            # build_message_record builder + parse_message_record handler consume.
            from_id = msg.get("from_id")
            sender = None
            if isinstance(from_id, dict) and isinstance(from_id.get("user_id"), int):
                sender = {"user_id": from_id["user_id"]}
            return {
                "id": int(msg.get("id") or 0),
                "date": msg.get("date"),
                "edit_date": msg.get("edit_date"),
                "message": msg.get("message") or "",
                "out": bool(msg.get("out", False)),
                "from_id": sender,
            }

        async def iter_dialogs(self, *, limit: int = 500) -> list[dict]:
            r = await self._http.post(
                f"{self._base}/messages.getDialogs",
                headers=self._headers,
                json={"limit": limit},
            )
            r.raise_for_status()
            out = []
            for d in r.json().get("dialogs", []):
                did = d.get("dialog_id")
                if not isinstance(did, int):
                    continue
                out.append({
                    "dialog_id": did,
                    "dialog_kind": d.get("dialog_kind") or "chat",
                    "access_hash": d.get("access_hash"),
                    "title": d.get("title"),
                })
            return out

        async def get_history(
            self,
            *,
            dialog_id: int,
            access_hash,
            dialog_kind: str,
            offset_id: int = 0,
            min_id: int = 0,
            limit: int = 100,
        ):
            # Returns (messages, next_offset_id, is_last) exactly like the real
            # client: next_offset_id = MIN id of the page (the backward cursor),
            # is_last = short page. The mock pages messages.getHistory BACKWARD:
            # id < offset_id (0 = newest), min_id is an EXCLUSIVE floor.
            peer = {"dialog_id": int(dialog_id), "dialog_kind": dialog_kind}
            if access_hash is not None:
                peer["access_hash"] = int(access_hash)
            limit = min(100, max(1, int(limit)))
            r = await self._http.post(
                f"{self._base}/messages.getHistory",
                headers=self._headers,
                json={
                    "peer": peer,
                    "offset_id": int(offset_id or 0),
                    "min_id": int(min_id or 0),
                    "limit": limit,
                },
            )
            r.raise_for_status()
            raw = r.json().get("messages", [])
            messages = [self._flatten(m) for m in raw]
            ids = [m["id"] for m in messages if isinstance(m.get("id"), int) and m["id"] > 0]
            next_offset_id = min(ids) if ids else None
            is_last = len(messages) < limit or next_offset_id is None
            return messages, next_offset_id, is_last

        async def aclose(self) -> None:
            await self._http.aclose()

    client = _TelegramMockClient(tbase, SESSION)
    # Monkeypatch the fetcher's opener seam: it is awaited and unpacked as
    # `client, close = await _open_telegram_client(install)`; _return yields
    # (client, _noop) so close() is the no-op the harness uses everywhere.
    fet._open_telegram_client = lambda _install: _return(client)

    # ONBOARDING: enumerate the account's dialogs (one shard per dialog). This is
    # the iter_dialogs shape — there is no separate "list installs" call; the mock
    # has exactly one seeded install (session 'spam-telegram').
    dialogs = await client.iter_dialogs(limit=500)
    print(f"  telegram: {len(dialogs)} dialogs discovered")

    install = {"id": uuid4(), "tenant_id": TENANT}
    installation_id = str(install["id"])
    total = 0
    try:
        for dlg in dialogs:
            dialog_id = dlg["dialog_id"]
            if not isinstance(dialog_id, int):
                continue
            # Shard shape copied from preflight._signal_records (the Telegram
            # archetype: one shard per conversation, backward-paged history),
            # remapped to telegram's dialog_* fields the fetcher reads.
            shard = {
                "shard_kind": fet.SHARD_KIND_DIALOG_HISTORY,
                "dialog_id": dialog_id,
                "dialog_kind": dlg.get("dialog_kind") or "chat",
                "access_hash": dlg.get("access_hash"),
                "dialog_title": dlg.get("title"),
                "installation_id": installation_id,
                "offset_id_cursor": None,  # full initial backfill (no warm-start)
            }
            cursor = None
            records: list = []
            for _ in range(10_000):  # page guard — backward-walk to start of history
                res = await fet.fetch_page_telegram(install, shard, cursor)
                records.extend(res.records)
                cursor = res.next_cursor
                if res.end_of_data:
                    break
            total += await _write_records(records, "telegram", pool)
    finally:
        await client.aclose()
    return total


# ---- dynamic registry: every module-level ingest_<src> ----
INGESTERS = {
    name[len("ingest_"):]: fn
    for name, fn in dict(globals()).items()
    if name.startswith("ingest_") and callable(fn)
    and name[len("ingest_"):] in PORTS   # only real source adapters (excl. imported ingest_from_draft)
}
ALL = sorted(INGESTERS)


async def main(sources):
    dsn = os.environ["DATABASE_URL"]
    await init_pool(dsn=dsn)
    pool = get_pool()
    async with pool.acquire() as conn:
        created = await ensure_test_partition_window(conn, months_back=30, months_ahead=3)
    print(f"partitions ensured ({len(created)} created); sources={len(sources)}\n")
    results = {}
    for s in sources:
        if s not in INGESTERS:
            print(f"[skip] {s}"); continue
        print(f"[{s}] ingesting ...")
        try:
            results[s] = await INGESTERS[s](pool)
            print(f"[{s}] OK -> {results[s]} obs\n")
        except Exception as exc:
            import traceback as tb
            results[s] = f"ERROR: {type(exc).__name__}: {exc}"
            print(f"[{s}] FAILED: {type(exc).__name__}: {exc}")
            tb.print_exc(); print()
    print("=== SUMMARY ===")
    tot = 0
    for s in ALL:
        r = results.get(s, "(not run)")
        if isinstance(r, int): tot += r
        print(f"  {s:12} {r}")
    print(f"  {'TOTAL':12} {tot}")
    await close_pool()


if __name__ == "__main__":
    args = sys.argv[1:]
    srcs = ALL if (not args or args == ["all"]) else args
    asyncio.run(main(srcs))
