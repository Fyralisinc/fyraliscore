#!/usr/bin/env python3
"""tests/e2e/test_google_workspace_e2e.py — end-to-end ingestion of an entire
Google Workspace organization over Domain-Wide Delegation, with NO real Google
credentials.

WHY THIS EXISTS
===============
Gmail, Google Calendar, and Google Drive are not three independent integrations
— in a real deployment they are ONE Workspace domain behind ONE service account
with domain-wide delegation. A Workspace super-admin grants that SA the right to
impersonate users domain-wide; ingestion then:

  1. enumerates the domain through the Admin SDK Directory API (users, groups,
     org units) to resolve an admin-authored inclusion_spec into a concrete set
     of mailboxes/calendars/drives — minus opt-outs and suspended accounts;
  2. for each resolved user, mints a per-user, scope-bound bearer token via the
     JWT-bearer grant (the DWD flow), and reads that user's Gmail / Calendar /
     Drive with the SAME service account.

The per-source sandboxes (`scripts/sandbox_google_*.py`) skip step 1 — they hand
the resolved emails in directly. This test exercises the org-level path proper:
a single mock that behaves like one Workspace domain across the Directory API,
the DWD token endpoint, and all three data APIs, driving the REAL minter +
clients + planners + fetchers + ingest pipeline.

WHAT IT PROVES
==============
- DWD directory resolution: one inclusion_spec (explicit users + a group + an
  org unit) resolves correctly across all three sources, with group-member
  expansion, org-unit expansion, opt-out subtraction, and suspended-user
  filtering.
- Per-user impersonation is faithful: the mock binds each minted token to the
  JWT `sub`, so Gmail/Drive `me` requests route to the right user — proving the
  pipeline impersonates each user distinctly, not a single shared identity.
- All three sources ingest end-to-end into `observations`: Gmail messages,
  Calendar events (incl. an incremental cancellation → state_change), Drive
  files across users' My Drives + a Shared Drive (incl. content extraction).
- Cross-path dedup holds (re-ingesting an already-seen item is a no-op).

Run as a test (needs DATABASE_URL):
    DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os \
        python -m pytest tests/e2e/test_google_workspace_e2e.py -v

Or standalone (prints a PASS/FAIL checklist like the sandbox scripts):
    DATABASE_URL=... python tests/e2e/test_google_workspace_e2e.py
"""
from __future__ import annotations

import base64
import json
import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# services.synthetic refuses to import under a prod env; this is a dev/test
# harness. Declare before any service import.
os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg  # noqa: E402


_TENANT_ID = UUID("00000000-0000-0000-0000-00000000600c")  # "GOOG"
_DOMAIN = "acme.com"
_ADMIN = "admin@acme.com"
_SA_EMAIL = "fyralis-workspace@fyralis-sandbox.iam.gserviceaccount.com"
_SHARED_DRIVE_ID = "0AItHelios"

# The admin-authored inclusion spec — the ONE spec that drives all three
# sources. Resolves (via the Directory API mock) to: alice (explicit + group),
# bob (group), carol (org unit). dave is opted out; erin is suspended.
_INCLUSION_SPEC = {
    "users": ["alice@acme.com"],
    "groups": ["eng@acme.com"],
    "org_units": ["/Sales"],
}
_OPTOUTS = {"dave@acme.com"}
_EXPECTED_USERS = ["alice@acme.com", "bob@acme.com", "carol@acme.com"]


# =====================================================================
# Org fixture
# =====================================================================
def _iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _user(email: str, ou: str, *, suspended: bool = False) -> dict:
    return {
        "primaryEmail": email,
        "name": {"fullName": email.split("@")[0].title()},
        "orgUnitPath": ou,
        "suspended": suspended,
        "isMailboxSetup": True,
    }


def _gmail_msg(mid: str, thread: str, frm: str, to: str, subject: str,
               body: str, internal_ms: int) -> dict:
    body_b64 = base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")
    return {
        "id": mid,
        "threadId": thread,
        "labelIds": ["INBOX"],
        "snippet": body[:60],
        "internalDate": str(internal_ms),
        "sizeEstimate": len(body),
        "payload": {
            "mimeType": "text/plain",
            "headers": [
                {"name": "Message-ID", "value": f"<{mid}@acme.com>"},
                {"name": "From", "value": frm},
                {"name": "To", "value": to},
                {"name": "Subject", "value": subject},
                {"name": "Date", "value": "Tue, 20 May 2026 10:00:00 +0000"},
            ],
            "body": {"data": body_b64},
        },
    }


def _cal_event(eid: str, cal: str, summary: str, start: datetime, *,
               attendees=None, status: str = "confirmed", updated: datetime | None = None) -> dict:
    obj = {
        "kind": "calendar#event", "id": eid, "status": status,
        "summary": summary, "eventType": "default",
        "start": {"dateTime": _iso(start)},
        "end": {"dateTime": _iso(start + timedelta(minutes=30))},
        "organizer": {"email": cal}, "creator": {"email": cal},
        "htmlLink": f"https://calendar.google.com/event?eid={eid}",
        "updated": _iso(updated or datetime.now(timezone.utc)),
    }
    if attendees is not None:
        obj["attendees"] = attendees
    return obj


_DOC_MIME = "application/vnd.google-apps.document"


def _drive_file(fid: str, name: str, owner: str, version: int, *,
                mime: str = _DOC_MIME, trashed: bool = False) -> dict:
    return {
        "id": fid, "name": name, "mimeType": mime, "version": str(version),
        "trashed": trashed,
        "createdTime": "2026-05-01T09:00:00.000Z",
        "modifiedTime": "2026-05-20T10:00:00.000Z",
        "webViewLink": f"https://docs.google.com/d/{fid}",
        "owners": [{"emailAddress": owner, "displayName": owner.split("@")[0].title()}],
        "lastModifyingUser": {"emailAddress": owner},
        "permissions": [{"emailAddress": owner, "role": "owner", "type": "user"}],
        "shared": False,
    }


def build_org():
    """Construct the WorkspaceOrg fixture for acme.com."""
    from services.synthetic.mock_servers.google_workspace import WorkspaceOrg

    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)

    users = [
        _user("alice@acme.com", "/Engineering"),
        _user("bob@acme.com", "/Engineering"),
        _user("carol@acme.com", "/Sales"),
        _user("dave@acme.com", "/Sales"),               # opted out
        _user("erin@acme.com", "/Sales", suspended=True),  # filtered (suspended)
    ]
    groups = [{"email": "eng@acme.com", "name": "Engineering", "directMembersCount": "2"}]
    group_members = {
        "eng@acme.com": [
            {"type": "USER", "email": "alice@acme.com", "role": "MEMBER"},
            {"type": "USER", "email": "bob@acme.com", "role": "MEMBER"},
        ],
    }
    org_units = [
        {"orgUnitPath": "/Engineering", "name": "Engineering"},
        {"orgUnitPath": "/Sales", "name": "Sales"},
    ]

    # Per-user Gmail mailboxes (Message-ID drives external_id).
    gmail = {
        "alice@acme.com": {"history_id": "1001", "messages": [
            _gmail_msg("a-msg-1", "t-a1", "ext@partner.com", "alice@acme.com",
                       "Series B term sheet", "Attaching the revised term sheet.", now_ms),
            _gmail_msg("a-msg-2", "t-a2", "alice@acme.com", "bob@acme.com",
                       "Re: standup", "Pushed the fix to main.", now_ms),
        ]},
        "bob@acme.com": {"history_id": "2002", "messages": [
            _gmail_msg("b-msg-1", "t-b1", "ci@acme.com", "bob@acme.com",
                       "Build green", "All checks passed on PR #412.", now_ms),
        ]},
        "carol@acme.com": {"history_id": "3003", "messages": [
            _gmail_msg("c-msg-1", "t-c1", "lead@bigco.com", "carol@acme.com",
                       "Renewal", "Happy to renew at the current tier.", now_ms),
        ]},
    }

    # Per-user calendars; alice gets an incremental delta (new + cancellation).
    calendar = {
        "alice@acme.com": {
            "events": [
                _cal_event("a-standup", "alice@acme.com", "Eng standup", now + timedelta(days=1),
                           attendees=[{"email": "alice@acme.com", "responseStatus": "accepted"},
                                      {"email": "bob@acme.com", "responseStatus": "accepted"}]),
                _cal_event("a-investor", "alice@acme.com", "Investor sync", now + timedelta(days=2)),
            ],
            "delta": [
                _cal_event("a-board", "alice@acme.com", "Board meeting", now + timedelta(days=5),
                           updated=now + timedelta(minutes=10)),
                {"kind": "calendar#event", "id": "a-standup", "status": "cancelled",
                 "updated": _iso(now + timedelta(minutes=11))},
            ],
        },
        "bob@acme.com": {
            "events": [_cal_event("b-1on1", "bob@acme.com", "1:1 with Alice", now + timedelta(days=1, hours=3))],
            "delta": [],
        },
        "carol@acme.com": {
            "events": [_cal_event("c-qbr", "carol@acme.com", "QBR with BigCo", now + timedelta(days=3))],
            "delta": [],
        },
    }

    # Per-user My Drive (one Google Doc each, with extractable text).
    drive_my = {
        "alice@acme.com": {
            "files": [_drive_file("d-alice-roadmap", "Roadmap", "alice@acme.com", 3)],
            "exports": {"d-alice-roadmap": "Roadmap: ship Atlas in Q3, Helios in Q4."},
        },
        "bob@acme.com": {
            "files": [_drive_file("d-bob-notes", "Arch notes", "bob@acme.com", 2)],
            "exports": {"d-bob-notes": "Arch: move to event sourcing for the ledger."},
        },
        "carol@acme.com": {
            "files": [_drive_file("d-carol-deck", "BigCo deck", "carol@acme.com", 1)],
            "exports": {"d-carol-deck": "BigCo renewal: upsell to enterprise tier."},
        },
    }
    # One org-wide Shared Drive, enumerated via drives.list, owned-by-impersonation.
    shared_drives = [{"id": _SHARED_DRIVE_ID, "name": "Helios Program"}]
    drive_shared = {
        _SHARED_DRIVE_ID: {
            "files": [_drive_file("d-helios-charter", "Helios charter", "alice@acme.com", 1)],
            "exports": {"d-helios-charter": "Helios charter: cross-functional, GA target Q4."},
        },
    }

    return WorkspaceOrg(
        domain=_DOMAIN, users=users, groups=groups, group_members=group_members,
        org_units=org_units, gmail=gmail, calendar=calendar, drive_my=drive_my,
        shared_drives=shared_drives, drive_shared=drive_shared,
    )


# =====================================================================
# Fake service account (real RSA key → genuinely-signed JWT assertion).
# =====================================================================
def _write_fake_sa(token_url: str) -> str:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    sa = {
        "type": "service_account", "project_id": "fyralis-sandbox",
        "private_key_id": "ws-k1", "private_key": pem,
        "client_email": _SA_EMAIL, "client_id": "100000000000000000009",
        "token_uri": token_url,
    }
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".sa.json", delete=False, prefix="ws_e2e_")
    json.dump(sa, f)
    f.close()
    return f.name


# =====================================================================
# DB helpers — a THROWAWAY database, created fresh and dropped on exit.
#
# This test must NOT run against the long-lived dev DB: (1) the live dev-stack
# workers (tenant_onboarding, source_onboarding, …) poll that DB and would
# consume the onboarding_triggers this test writes, spawning google_* rows in
# the source-CHECK-narrowed tables (source_onboarding_runs, onboarding_shards)
# that then break every other integration test's migration re-application (the
# source-CHECK re-run landmine); (2) a fresh DB lets migrations apply with the
# strict on_error="stop" policy. This mirrors scripts/sandbox_google_*.py.
# =====================================================================
def _admin_url() -> str:
    return os.environ.get(
        "SANDBOX_ADMIN_URL",
        os.environ.get("DATABASE_URL")
        or "postgresql://company_os:company_os@localhost:5434/company_os",
    )


async def _create_throwaway_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()


async def _drop_throwaway_db(admin_url: str, name: str) -> None:
    admin = await asyncpg.connect(admin_url)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = $1 AND pid <> pg_backend_pid()", name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await admin.close()


async def _open_pool(db_url: str) -> asyncpg.Pool:
    from services.gateway.db_bootstrap import _register_codecs
    return await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5, init=_register_codecs)


async def _bootstrap_schema(pool: asyncpg.Pool) -> None:
    """Fresh DB: apply migrations strictly, ensure observation partitions,
    seed the tenant."""
    from lib.shared.migrations import apply_migrations_dir
    from services.observations.partitions import ensure_partitions
    async with pool.acquire() as conn:
        await apply_migrations_dir(conn, _REPO_ROOT / "db" / "migrations")
    await ensure_partitions(pool, months_ahead=3)
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'workspace-e2e') "
        "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
    )


async def _seed_gmail_install(pool: asyncpg.Pool, org, emails: list[str]) -> UUID:
    """Gmail has no finalize_install helper (its watch lifecycle is push-driven);
    seed the install + active mailbox watches directly from the resolved emails,
    as a completed onboarding would leave them."""
    from lib.shared.ids import uuid7
    install_id = uuid7()
    await pool.execute(
        """INSERT INTO gmail_installations
             (id, tenant_id, workspace_domain, service_account_email, scope,
              inclusion_spec, resolved_user_count, resolved_at)
           VALUES ($1,$2,$3,$4,'gmail.readonly',$5::jsonb,$6, now())""",
        install_id, _TENANT_ID, _DOMAIN, _SA_EMAIL,
        json.dumps(_INCLUSION_SPEC), len(emails),
    )
    for email in emails:
        hist = str(org.gmail.get(email, {}).get("history_id", "1"))
        await pool.execute(
            """INSERT INTO gmail_mailbox_watches
                 (id, tenant_id, gmail_installation_id, email_address,
                  google_user_id, history_id, state)
               VALUES ($1,$2,$3,$4,$5,$6,'active')""",
            uuid7(), _TENANT_ID, install_id, email, f"uid-{email}", hist,
        )
    return install_id


# =====================================================================
# Pipeline drive (real fetcher loop → real ingest), per shard.
# =====================================================================
async def _drain(pool, fetch_fn, channel, install_row, shard_identifier) -> list[str]:
    from services.ingestion.core import ingest
    ingested: list[str] = []
    cursor, guard = None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError(f"fetch loop did not terminate for {shard_identifier}")
        result = await fetch_fn(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest(channel, record, pool=pool, tenant_id=_TENANT_ID, enqueue_trigger=True)
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if result.end_of_data:
            break
    return ingested


# =====================================================================
# The end-to-end test.
# =====================================================================
@pytest.mark.integration
@pytest.mark.asyncio
async def test_google_workspace_org_ingestion_e2e():
    if not os.environ.get("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set")
    await _run_e2e(verbose=False)


async def _run_e2e(*, verbose: bool, keep: bool = False) -> None:
    from uuid import uuid4

    from services.integrations.gmail import dwd as _dwd
    from services.integrations.gmail.client import (
        DirectoryClient, GoogleHttpClient,
    )
    from services.integrations.gmail.dwd import get_minter
    from services.integrations.gmail.directory import resolve_inclusion
    from services.integrations.google_drive.client import GoogleDriveClient, resolve_scope
    from services.synthetic.mock_servers.google_workspace import start_mock_workspace

    org = build_org()
    server, env = start_mock_workspace(org)
    sa_path = _write_fake_sa(env["GOOGLE_TOKEN_URI"])

    # Snapshot + apply env so we can restore afterward.
    saved = {k: os.environ.get(k) for k in (
        *env, "GMAIL_SERVICE_ACCOUNT_JSON_FILE",
        "GOOGLE_DRIVE_FETCH_COMMENTS", "GOOGLE_DRIVE_FETCH_REVISIONS",
    )}
    os.environ.update(env)
    os.environ["GMAIL_SERVICE_ACCOUNT_JSON_FILE"] = sa_path
    # Keep the drive assertions focused on file ingestion + extraction.
    os.environ["GOOGLE_DRIVE_FETCH_COMMENTS"] = "0"
    os.environ["GOOGLE_DRIVE_FETCH_REVISIONS"] = "0"
    _dwd._reset_minter_for_tests()

    # Throwaway DB on the same server (isolated from the live dev-stack workers).
    admin_url = _admin_url()
    created_db = f"gws_e2e_{uuid4().hex[:8]}"
    await _create_throwaway_db(admin_url, created_db)
    db_url = admin_url.rsplit("/", 1)[0] + "/" + created_db

    pool = await _open_pool(db_url)
    try:
        await _bootstrap_schema(pool)

        # -- 1. DWD directory resolution (Admin SDK), shared across all 3 sources.
        http = GoogleHttpClient(get_minter())
        await http.__aenter__()
        try:
            directory = DirectoryClient(http, _ADMIN)
            resolved = await resolve_inclusion(
                directory, workspace_domain=_DOMAIN,
                inclusion_spec=_INCLUSION_SPEC, optouts=_OPTOUTS,
            )
            assert resolved == _EXPECTED_USERS, (
                f"directory resolution wrong: {resolved} != {_EXPECTED_USERS}"
            )
            assert "dave@acme.com" not in resolved, "opt-out not subtracted"
            assert "erin@acme.com" not in resolved, "suspended user not filtered"

            # Shared-drive enumeration uses the SA impersonating the first user.
            drive_client = GoogleDriveClient(http, scope=resolve_scope("drive.readonly"))
        finally:
            pass  # keep http open; drive_client shares it for shared-drive enum

        # -- 2. Onboard all three sources from the SAME resolved set.
        from services.integrations.google_calendar.onboarding import (
            finalize_install as finalize_calendar,
            resolve_calendar_targets,
        )
        from services.integrations.google_drive.onboarding import (
            finalize_install as finalize_drive,
            resolve_drive_targets,
        )

        cal_emails = await resolve_calendar_targets(
            directory, workspace_domain=_DOMAIN,
            inclusion_spec=_INCLUSION_SPEC, optouts=_OPTOUTS,
        )
        cal_install = await finalize_calendar(
            pool, tenant_id=_TENANT_ID, workspace_domain=_DOMAIN,
            service_account_email=_SA_EMAIL, calendar_emails=cal_emails,
            inclusion_spec=_INCLUSION_SPEC,
        )

        drive_targets = await resolve_drive_targets(
            directory, workspace_domain=_DOMAIN, inclusion_spec=_INCLUSION_SPEC,
            optouts=_OPTOUTS, include_shared_drives=True, drive_client=drive_client,
        )
        assert len(drive_targets.my_drives) == 3, "one My Drive per resolved user"
        assert len(drive_targets.shared_drives) == 1, "one Shared Drive enumerated"
        await finalize_drive(
            pool, tenant_id=_TENANT_ID, workspace_domain=_DOMAIN,
            service_account_email=_SA_EMAIL, targets=drive_targets.all(),
            inclusion_spec=_INCLUSION_SPEC, include_shared_drives=True,
        )
        gmail_install = await _seed_gmail_install(pool, org, resolved)
        await http.__aexit__(None, None, None)

        # -- 3. Plan shards exactly as SourceOnboarding does (loader SQL → planner).
        from services.ingestion.planners.context import PlannerContext
        from services.ingestion.planners.gmail import plan_shards_gmail
        from services.ingestion.planners.google_calendar import plan_shards_google_calendar
        from services.ingestion.planners.google_drive import plan_shards_google_drive
        from services.ingestion.workflows.source_onboarding import (
            _LOAD_GCAL_INSTALL_SQL, _LOAD_GDRIVE_INSTALL_SQL, _LOAD_GMAIL_INSTALL_SQL,
        )

        def _ctx(row):
            return PlannerContext(tenant_id=_TENANT_ID, install=row, conn=None, source_client=None)

        gmail_row = await pool.fetchrow(_LOAD_GMAIL_INSTALL_SQL, _TENANT_ID)
        cal_row = await pool.fetchrow(_LOAD_GCAL_INSTALL_SQL, _TENANT_ID)
        drive_row = await pool.fetchrow(_LOAD_GDRIVE_INSTALL_SQL, _TENANT_ID)

        gmail_shards = await plan_shards_gmail(_ctx(gmail_row))
        cal_shards = await plan_shards_google_calendar(_ctx(cal_row))
        drive_shards = await plan_shards_google_drive(_ctx(drive_row))
        assert len(gmail_shards) == 3, "one mailbox shard per resolved user"
        assert len(cal_shards) == 3, "one calendar shard per resolved user"
        assert len(drive_shards) == 4, "3 My Drives + 1 Shared Drive"

        # -- 4. Backfill: real fetcher → real ingest, per source, per shard.
        from services.ingestion.fetchers.gmail import fetch_page_gmail
        from services.ingestion.fetchers.google_calendar import fetch_page_google_calendar
        from services.ingestion.fetchers.google_drive import fetch_page_google_drive

        for s in gmail_shards:
            await _drain(pool, fetch_page_gmail, "gmail:", gmail_row, s.shard_identifier)
        for s in cal_shards:
            await _drain(pool, fetch_page_google_calendar, "google_calendar:event", cal_row, s.shard_identifier)
        for s in drive_shards:
            await _drain(pool, fetch_page_google_drive, "google_drive:file", drive_row, s.shard_identifier)

        # -- 5. Calendar incremental delta (warm-start alice from her syncToken).
        incr_shard = {
            "shard_kind": "google_calendar_events", "calendar_id": "alice@acme.com",
            "owner_email": "alice@acme.com", "installation_id": str(cal_install),
            "sync_token": "sync-1",
        }
        await _drain(pool, fetch_page_google_calendar, "google_calendar:event", cal_row, incr_shard)

        # =============================================================
        # Assertions: observations landed for every source.
        # =============================================================
        async def count(channel: str) -> int:
            return await pool.fetchval(
                "SELECT count(*) FROM observations WHERE tenant_id=$1 AND source_channel=$2",
                _TENANT_ID, channel,
            )

        gmail_n = await count("gmail:")
        cal_n = await count("google_calendar:event")
        drive_n = await count("google_drive:file")

        # Gmail: 2 (alice) + 1 (bob) + 1 (carol) = 4.
        assert gmail_n == 4, f"gmail observations: {gmail_n} != 4"
        # Calendar: backfill 2+1+1=4, plus delta (new board signal + standup cancel) = 6.
        assert cal_n == 6, f"calendar observations: {cal_n} != 6"
        # Drive: 3 My Drive docs + 1 Shared Drive doc = 4.
        assert drive_n == 4, f"drive observations: {drive_n} != 4"

        # Per-user impersonation: each mailbox was minted+read under its OWN
        # bearer (the mock counts a token mint per impersonated user).
        for email in _EXPECTED_USERS:
            assert server.request_hits.get(f"token:{email}", 0) > 0, (
                f"no DWD token minted for {email} — impersonation collapsed"
            )
            assert server.request_hits.get(f"gmail.list:{email}", 0) > 0, (
                f"mailbox {email} never listed"
            )

        # Gmail external_id is install-namespaced + message-id based.
        alice_ext = await pool.fetchval(
            "SELECT external_id FROM observations WHERE tenant_id=$1 "
            "AND source_channel='gmail:' AND content->>'subject'=$2",
            _TENANT_ID, "Series B term sheet",
        )
        assert alice_ext == f"gmail:{gmail_install}:a-msg-1@acme.com", alice_ext

        # Calendar mutable-source semantics: the cancellation is a distinct
        # state_change observation (versioned external_id keeps both).
        standup_kinds = await pool.fetch(
            "SELECT kind FROM observations WHERE tenant_id=$1 AND content->>'event_id'=$2",
            _TENANT_ID, "a-standup",
        )
        kinds = sorted(r["kind"] for r in standup_kinds)
        assert kinds == ["signal", "state_change"], f"a-standup kinds: {kinds}"

        # Drive content extraction reached content_text.
        roadmap_text = await pool.fetchval(
            "SELECT content_text FROM observations WHERE tenant_id=$1 "
            "AND external_id LIKE 'gdrive:d-alice-roadmap:%'",
            _TENANT_ID,
        )
        assert roadmap_text and "Atlas in Q3" in roadmap_text, roadmap_text
        # Shared-drive file ingested too.
        helios = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND external_id LIKE 'gdrive:d-helios-charter:%'",
            _TENANT_ID,
        )
        assert helios == 1, "shared-drive file not ingested"

        # Cross-path dedup: re-ingesting an already-seen calendar event no-ops.
        from services.ingestion.core import ingest
        twin = dict(org.calendar["bob@acme.com"]["events"][0])
        twin["_fyralis_calendar_id"] = "bob@acme.com"
        twin["_fyralis_owner_email"] = "bob@acme.com"
        res = await ingest("google_calendar:event", twin, pool=pool, tenant_id=_TENANT_ID)
        assert res.deduped is True, "re-ingest should dedup"

        total = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1", _TENANT_ID,
        )
        assert total == gmail_n + cal_n + drive_n == 14, f"total observations: {total}"

        if verbose:
            print(f"\n  gmail={gmail_n} calendar={cal_n} drive={drive_n} total={total}")
            print(f"  resolved users: {resolved}")
            print("  token mints per user: " + ", ".join(
                f"{u}={server.request_hits.get(f'token:{u}', 0)}" for u in _EXPECTED_USERS))
            print("  ALL CHECKS PASSED")
    finally:
        await pool.close()
        if keep:
            _print_keep_banner(admin_url, created_db)
        else:
            await _drop_throwaway_db(admin_url, created_db)
        server.shutdown()
        try:
            os.unlink(sa_path)
        except OSError:
            pass
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        _dwd._reset_minter_for_tests()


# =====================================================================
# Standalone runner (prints a checklist; same path as the test).
# =====================================================================
def _print_keep_banner(admin_url: str, db: str) -> None:
    """Print pgAdmin / psql connection details for the retained DB."""
    from urllib.parse import urlsplit

    parts = urlsplit(admin_url)
    host = parts.hostname or "localhost"
    port = parts.port or 5432
    user = parts.username or "company_os"
    pw = parts.password or "company_os"
    print("\n" + "=" * 72)
    print("  KEPT the throwaway database — inspect it in pgAdmin:")
    print(f"    Host     : {host}")
    print(f"    Port     : {port}")
    print(f"    Username : {user}")
    print(f"    Password : {pw}")
    print(f"    Database : {db}")
    print(f"\n  psql:  PGPASSWORD={pw} psql -h {host} -p {port} -U {user} -d {db}")
    print(f"  tenant_id for filtering: {_TENANT_ID}")
    print("\n  Try in pgAdmin's Query Tool (after connecting to the DB above):")
    print("    SELECT source_channel, kind, external_id, content_text")
    print(f"    FROM observations WHERE tenant_id = '{_TENANT_ID}'")
    print("    ORDER BY source_channel, occurred_at;")
    print(f"\n  Drop it when done:  DROP DATABASE \"{db}\";")
    print("=" * 72)


def main() -> int:
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(description="Google Workspace org ingestion e2e")
    parser.add_argument(
        "--keep", action="store_true",
        help="keep the throwaway database on exit (prints pgAdmin connection info)",
    )
    args = parser.parse_args()
    try:
        asyncio.run(_run_e2e(verbose=True, keep=args.keep))
    except AssertionError as exc:
        print(f"\n  FAIL: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
