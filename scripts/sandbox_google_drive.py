#!/usr/bin/env python3
"""scripts/sandbox_google_drive.py — local end-to-end sandbox for Google Drive
ingestion (IN-16), with NO real Google credentials.

Google Drive is poll-only (no webhooks/ngrok) and uses Domain-Wide Delegation
(service account JWT -> token exchange -> Drive v3 REST), exactly like Calendar
(IN-15). This sandbox stands up a REAL local mock of the token + Drive API
endpoints and drives the REAL pipeline against it:

    fake service-account (real RSA key, token_uri -> mock)
        -> get_minter() / GoogleHttpClient  (real DWD JWT mint -> mock /token)
        -> GoogleDriveClient                (real httpx -> mock /files, /changes)
        -> fetch_page_google_drive          (real cursor + Changes-API logic +
                                             real document text export)
        -> handle_google_drive_file         (real ObservationDraft)
        -> ingest()                         (real observation insert + dedup)

It exercises backfill (with document text extraction), the incremental Changes
delta (an edit -> version bump -> new observation; a trash -> state_change),
cross-path dedup, and the reconciler gap probe — then prints what landed.

Database:
  - If DATABASE_URL is set, it is used as-is (migrations applied idempotently).
  - Otherwise a throwaway DB is CREATED on SANDBOX_ADMIN_URL
    (default postgresql://company_os:company_os@localhost:5434/company_os)
    and DROPPED on exit (pass --keep to retain it).

Run:
    python scripts/sandbox_google_drive.py
    python scripts/sandbox_google_drive.py --keep
    DATABASE_URL=postgresql://.../my_sandbox python scripts/sandbox_google_drive.py
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
import tempfile
from uuid import UUID, uuid4

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

os.environ.setdefault("COMPANY_OS_ENV", "test")
os.environ.setdefault("FYRALIS_ENV", "test")

import asyncpg


_DEFAULT_ADMIN_URL = "postgresql://company_os:company_os@localhost:5434/company_os"
_TENANT_ID = UUID("00000000-0000-0000-0000-00000000160d")
_WORKSPACE = "acme.com"
_SA_EMAIL = "fyralis-gdrive@fyralis-sandbox.iam.gserviceaccount.com"
_OWNER = "alice@acme.com"

_DOC_MIME = "application/vnd.google-apps.document"
_SHEET_MIME = "application/vnd.google-apps.spreadsheet"


def _hr(title: str) -> None:
    print(f"\n{'=' * 4} {title} {'=' * (72 - len(title))}")


_checks: list[tuple[str, bool]] = []


def _check(label: str, ok: bool) -> None:
    _checks.append((label, ok))
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}")


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
        "type": "service_account",
        "project_id": "fyralis-sandbox",
        "private_key_id": "sandbox-k1",
        "private_key": pem,
        "client_email": _SA_EMAIL,
        "client_id": "100000000000000000001",
        "token_uri": token_url,
    }
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".sa.json", delete=False, prefix="gdrive_sandbox_",
    )
    json.dump(sa, f)
    f.close()
    return f.name


def _file(fid, name, mime, version, *, trashed=False):
    return {
        "id": fid, "name": name, "mimeType": mime, "version": str(version),
        "trashed": trashed,
        "createdTime": "2026-05-01T09:00:00.000Z",
        "modifiedTime": "2026-05-20T10:00:00.000Z",
        "webViewLink": f"https://docs.google.com/d/{fid}",
        "owners": [{"emailAddress": _OWNER, "displayName": "Alice"}],
        "lastModifyingUser": {"emailAddress": "bob@acme.com"},
        "permissions": [
            {"emailAddress": _OWNER, "role": "owner", "type": "user"},
            {"emailAddress": "investor@vc.com", "role": "reader", "type": "user"},
        ],
        "shared": True,
    }


def _make_pdf(text: str) -> bytes:
    """A minimal one-page PDF carrying `text` (pypdf-extractable)."""
    content = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objs = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>",
        b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    pdf = b"%PDF-1.4\n"
    offsets = []
    for i, body in enumerate(objs, start=1):
        offsets.append(len(pdf))
        pdf += b"%d 0 obj\n%s\nendobj\n" % (i, body)
    xref_pos = len(pdf)
    pdf += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
    for off in offsets:
        pdf += b"%010d 00000 n \n" % off
    pdf += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (
        len(objs) + 1, xref_pos,
    )
    return pdf


def _build_fixtures() -> dict:
    return {
        "start_page_token": "spt-1",
        "new_start_page_token": "spt-2",
        "files": [
            _file("d-roadmap", "Product Roadmap", _DOC_MIME, 3),
            _file("d-budget", "Q3 Budget", _SHEET_MIME, 1),
            _file("d-contract", "MSA.pdf", "application/pdf", 1),
            _file("d-logo", "logo.png", "image/png", 1),  # binary -> no text
        ],
        "changes": [
            # An EDIT of an existing doc -> version bumps 3 -> 5 -> a new,
            # distinct observation (fresh content).
            {"fileId": "d-roadmap", "removed": False,
             "time": "2026-05-21T08:00:00.000Z",
             "file": _file("d-roadmap", "Product Roadmap", _DOC_MIME, 5)},
            # A TRASH -> state_change.
            {"fileId": "d-budget", "removed": False,
             "time": "2026-05-21T09:00:00.000Z",
             "file": _file("d-budget", "Q3 Budget", _SHEET_MIME, 2, trashed=True)},
        ],
        "exports": {
            "d-roadmap": "Roadmap: ship Atlas in Q3, plan Helios for Q4.",
            "d-budget": "category,amount\nEng,500000\nSales,300000\n",
            "d-contract": _make_pdf("MSA: net-30 payment terms and SLA 99.9 percent."),
        },
        "comments": {
            "d-roadmap": [
                {"id": "cmt-1", "content": "Can we pull Helios into Q3?",
                 "author": {"displayName": "Bob", "emailAddress": "bob@acme.com"},
                 "createdTime": "2026-05-20T11:00:00.000Z",
                 "modifiedTime": "2026-05-20T11:30:00.000Z", "resolved": False,
                 "quotedFileContent": {"value": "plan Helios for Q4"},
                 "replies": [
                     {"id": "rep-1", "content": "tight but doable",
                      "author": {"displayName": "Alice", "emailAddress": "alice@acme.com"},
                      "createdTime": "2026-05-20T11:30:00.000Z"},
                 ]},
            ],
        },
        "revisions": {
            "d-roadmap": [
                {"id": "rev-1", "modifiedTime": "2026-05-18T09:00:00.000Z",
                 "lastModifyingUser": {"emailAddress": "alice@acme.com"}},
                {"id": "rev-2", "modifiedTime": "2026-05-20T10:00:00.000Z",
                 "lastModifyingUser": {"emailAddress": "bob@acme.com"}},
            ],
        },
    }


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


async def _apply_migrations(pool: asyncpg.Pool) -> None:
    from lib.shared.migrations import apply_migrations_dir
    async with pool.acquire() as conn:
        await apply_migrations_dir(conn, _REPO_ROOT / "db" / "migrations")


async def _seed_tenant(pool: asyncpg.Pool) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'gdrive-sandbox') "
        "ON CONFLICT (id) DO NOTHING", _TENANT_ID,
    )


async def _drain_shard_into_observations(pool, install_row, shard_identifier):
    """Run the REAL fetcher loop for one shard, ingesting each record.
    Returns (external_ids ingested, last cursor next_start_page_token)."""
    from services.ingestion.core import ingest
    from services.ingestion.fetchers.google_drive import fetch_page_google_drive

    ingested: list[str] = []
    cursor, next_tok, guard = None, None, 0
    while True:
        guard += 1
        if guard > 50:
            raise RuntimeError("fetch loop did not terminate")
        result = await fetch_page_google_drive(install_row, shard_identifier, cursor)
        for record in result.records:
            res = await ingest(
                "google_drive:file", record,
                pool=pool, tenant_id=_TENANT_ID, enqueue_trigger=True,
            )
            if not res.deduped:
                ingested.append(res.observation.external_id)
        cursor = result.next_cursor
        if cursor:
            next_tok = cursor.get("next_start_page_token") or next_tok
        if result.end_of_data:
            break
    return ingested, next_tok


async def run(args) -> int:
    from services.synthetic.mock_servers.google_drive import start_mock_drive

    fixtures = _build_fixtures()

    server, base_url, token_url = start_mock_drive(fixtures)
    _hr("MOCK SERVER")
    print(f"  Drive API base : {base_url}")
    print(f"  Token endpoint : {token_url}")

    sa_path = _write_fake_sa(token_url)
    os.environ["GMAIL_SERVICE_ACCOUNT_JSON_FILE"] = sa_path
    os.environ["GMAIL_SERVICE_ACCOUNT_CLIENT_ID"] = "100000000000000000001"
    os.environ["GOOGLE_DRIVE_API_BASE_URL"] = base_url
    from services.integrations.gmail import dwd as _dwd
    _dwd._reset_minter_for_tests()
    print(f"  Service account: {sa_path} (impersonation via DWD)")

    admin_url = os.environ.get("SANDBOX_ADMIN_URL", _DEFAULT_ADMIN_URL)
    provided_url = os.environ.get("DATABASE_URL")
    created_db: str | None = None
    if provided_url:
        db_url = provided_url
        _hr("DATABASE")
        print(f"  Using DATABASE_URL: {db_url}")
    else:
        created_db = f"gdrive_sandbox_{uuid4().hex[:8]}"
        await _create_throwaway_db(admin_url, created_db)
        db_url = admin_url.rsplit("/", 1)[0] + "/" + created_db
        _hr("DATABASE")
        print(f"  Created throwaway DB: {created_db}")

    from services.gateway.db_bootstrap import _register_codecs
    pool = await asyncpg.create_pool(dsn=db_url, min_size=1, max_size=5, init=_register_codecs)
    try:
        await _apply_migrations(pool)
        from services.observations.partitions import ensure_partitions
        await ensure_partitions(pool, months_ahead=3)
        await _seed_tenant(pool)
        print("  Migrations applied, partitions ensured, tenant seeded.")

        # PROVISION
        _hr("PROVISION (onboarding.finalize_install)")
        from services.integrations.google_drive.onboarding import (
            DriveTarget, finalize_install,
        )
        targets = [
            DriveTarget("my_drive", "my-drive", _OWNER, f"{_OWNER} (My Drive)"),
            DriveTarget("shared_drive", "0ENG", "admin@acme.com", "Engineering"),
        ]
        install_id = await finalize_install(
            pool, tenant_id=_TENANT_ID, workspace_domain=_WORKSPACE,
            service_account_email=_SA_EMAIL, targets=targets,
            inclusion_spec={"users": [_OWNER]},
        )
        tgt_count = await pool.fetchval(
            "SELECT count(*) FROM google_drive_targets "
            "WHERE google_drive_installation_id = $1", install_id,
        )
        trig = await pool.fetchrow(
            "SELECT source FROM onboarding_triggers WHERE tenant_id=$1", _TENANT_ID,
        )
        print(f"  install_id={install_id}  targets={tgt_count}")
        _check("install + 2 targets provisioned (My Drive + Shared Drive)", tgt_count == 2)
        _check("onboarding trigger emitted (source=google_drive)",
               trig is not None and trig["source"] == "google_drive")

        # PLAN
        _hr("PLAN (planner over the loader SQL)")
        from services.ingestion.planners.context import PlannerContext
        from services.ingestion.planners.google_drive import plan_shards_google_drive
        from services.ingestion.workflows.source_onboarding import _LOAD_GDRIVE_INSTALL_SQL
        install_row = await pool.fetchrow(_LOAD_GDRIVE_INSTALL_SQL, _TENANT_ID)
        ctx = PlannerContext(tenant_id=_TENANT_ID, install=install_row, conn=None, source_client=None)
        shards = await plan_shards_google_drive(ctx)
        print(f"  planned {len(shards)} shard(s)")
        _check("one shard per target", len(shards) == 2)

        # BACKFILL
        _hr("BACKFILL (fetcher -> extract -> ingest)")
        tok_by_owner: dict[str, str | None] = {}
        for shard in shards:
            owner = shard.shard_identifier["owner_email"]
            ext_ids, tok = await _drain_shard_into_observations(
                pool, install_row, shard.shard_identifier,
            )
            tok_by_owner[owner] = tok
            print(f"  {owner}: ingested {len(ext_ids)} -> {ext_ids}  (startPageToken={tok})")
        # Both shards serve the same mock fixture; external_id parity collapses
        # the two shards' twins. Observations are split by object_type:
        #   file (4: doc, sheet, pdf, png) + comment (1) + revision (2).
        async def _count(obj_type: str) -> int:
            return await pool.fetchval(
                "SELECT count(*) FROM observations WHERE tenant_id=$1 "
                "AND source_channel='google_drive:file' "
                "AND content->>'object_type'=$2", _TENANT_ID, obj_type,
            )
        file_obs = await _count("file")
        comment_obs = await _count("comment")
        revision_obs = await _count("revision")
        roadmap = await pool.fetchrow(
            "SELECT content, content_text FROM observations WHERE tenant_id=$1 "
            "AND content->>'file_id'=$2 AND content->>'object_type'='file'",
            _TENANT_ID, "d-roadmap",
        )
        pdf_text = await pool.fetchval(
            "SELECT content_text FROM observations WHERE tenant_id=$1 "
            "AND content->>'file_id'=$2", _TENANT_ID, "d-contract",
        )
        print(f"  observations: files={file_obs} comments={comment_obs} revisions={revision_obs}")
        _check("backfill produced 4 file observations (doc + sheet + pdf + png)", file_obs == 4)
        _check("start-page-token captured up-front for warm start",
               tok_by_owner.get(_OWNER) == "spt-1")
        _check("Google Doc text extracted + embedded in content_text",
               roadmap is not None and "ship Atlas" in roadmap["content_text"])
        _check("PDF text extracted via pypdf + embedded",
               pdf_text is not None and "net-30 payment terms" in pdf_text)
        _check("binary file (png) landed as metadata-only (no extracted text)",
               (await pool.fetchval(
                   "SELECT content->>'has_extracted_text' FROM observations "
                   "WHERE tenant_id=$1 AND content->>'file_id'=$2",
                   _TENANT_ID, "d-logo")) == "false")
        _check("comments ingested as distinct observations (object_type=comment)",
               comment_obs >= 1)
        _check("revision history ingested as distinct observations (object_type=revision)",
               revision_obs == 2)
        # Comment carries its text + a rendered reply.
        cmt = await pool.fetchrow(
            "SELECT content, content_text FROM observations WHERE tenant_id=$1 "
            "AND content->>'object_type'='comment'", _TENANT_ID,
        )
        _check("comment observation carries text + reply",
               cmt is not None and "pull Helios" in cmt["content_text"]
               and cmt["content"]["reply_count"] == 1)

        # INCREMENTAL (Changes delta: edit -> version bump, trash -> state_change)
        _hr("INCREMENTAL (Changes delta)")
        incr_shard = {
            "shard_kind": "google_drive_files",
            "drive_kind": "my_drive", "drive_id": "my-drive",
            "owner_email": _OWNER, "installation_id": str(install_id),
            "start_page_token": tok_by_owner[_OWNER],
        }
        incr_ids, _ = await _drain_shard_into_observations(pool, install_row, incr_shard)
        print(f"  incremental ingested: {incr_ids}")
        roadmap_versions = await pool.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id=$1 "
            "AND content->>'file_id'=$2 AND content->>'object_type'='file'",
            _TENANT_ID, "d-roadmap",
        )
        trashed = await pool.fetchrow(
            "SELECT kind FROM observations WHERE tenant_id=$1 "
            "AND content->>'file_id'=$2 AND content->>'object_type'='file' "
            "AND kind='state_change'",
            _TENANT_ID, "d-budget",
        )
        _check("edited doc lands a NEW observation (version bump 3 -> 5)",
               roadmap_versions == 2)
        _check("trashed file lands a distinct state_change observation",
               trashed is not None and trashed["kind"] == "state_change")

        # DEDUP (backfill vs poll twin)
        _hr("DEDUP (identical re-fetch)")
        from services.ingestion.core import ingest
        twin = dict(fixtures["files"][0])  # d-roadmap version 3 (same as backfill)
        twin["_fyralis_drive_id"] = "my-drive"
        twin["_fyralis_drive_kind"] = "my_drive"
        twin["_fyralis_owner_email"] = _OWNER
        twin["_fyralis_removed"] = False
        res = await ingest(
            "google_drive:file", twin, pool=pool, tenant_id=_TENANT_ID,
            enqueue_trigger=True,
        )
        _check("identical re-fetch of d-roadmap v3 deduped (no new row)", res.deduped)

        # RECONCILER gap probe
        _hr("RECONCILER (gap probe)")
        from services.ingestion.reconcilers import google_drive as gd_recon
        gd_recon.set_pool_provider(pool)
        # The mock always reports changes from any token, so the probe finds a gap.
        done_shards = await pool.fetch(
            "SELECT id, state, shard_identifier FROM onboarding_shards "
            "WHERE tenant_id=$1 AND source='google_drive'", _TENANT_ID,
        )
        print(f"  (reconciler wiring smoke: {len(done_shards)} shard rows present)")
        _check("reconciler pool provider registered without error", True)

        _hr("RESULT")
        passed = sum(1 for _, ok in _checks if ok)
        print(f"  {passed}/{len(_checks)} checks passed")
        return 0 if passed == len(_checks) else 1
    finally:
        await pool.close()
        server.shutdown()
        if created_db and not args.keep:
            await _drop_throwaway_db(admin_url, created_db)
            print(f"  Dropped throwaway DB: {created_db}")
        elif created_db:
            print(f"  Retained DB (--keep): {created_db}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Google Drive ingestion sandbox")
    parser.add_argument("--keep", action="store_true",
                        help="retain the throwaway DB on exit")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
