#!/usr/bin/env python3
"""Disable a WhatsApp installation and zeroize its stored credential refs."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import pathlib
import sys
from dataclasses import asdict, dataclass
from typing import Any, Sequence

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.shared.errors import SecretNotFoundError
from lib.shared.ids import uuid7
from lib.shared.secrets import build_secret_store


_REF_FIELDS = ("app_secret_ref", "verify_token_ref", "access_token_ref")


@dataclass(frozen=True, slots=True)
class WhatsAppUninstallResult:
    found: bool
    phone_number_id: str
    enabled_before: bool | None = None
    refs_seen: int = 0
    refs_deleted: int = 0
    secret_delete_errors: int = 0
    plaintext_cleared: bool = False
    audit_written: bool = False
    dry_run: bool = False


def _phone_id_hash(phone_number_id: str) -> str:
    return hashlib.sha256(phone_number_id.encode("utf-8")).hexdigest()[:16]


async def _load_installation(
    pool: asyncpg.Pool,
    *,
    phone_number_id: str,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        """
        SELECT id, tenant_id, phone_number_id, enabled,
               app_secret, verify_token, access_token,
               app_secret_ref, verify_token_ref, access_token_ref
          FROM whatsapp_installations
         WHERE phone_number_id = $1
        """,
        phone_number_id,
    )


async def _write_audit(
    pool: asyncpg.Pool,
    *,
    tenant_id: Any,
    status: str,
    phone_number_id: str,
    refs_seen: int,
    refs_deleted: int,
    secret_delete_errors: int,
) -> bool:
    context = {
        "phone_number_id_hash": _phone_id_hash(phone_number_id),
        "refs_seen": refs_seen,
        "refs_deleted": refs_deleted,
        **(
            {"secret_delete_errors": secret_delete_errors}
            if secret_delete_errors
            else {}
        ),
    }
    await pool.execute(
        """
        INSERT INTO installation_audit_log
            (id, tenant_id, installation_row_id, provider, action, status, context)
        VALUES ($1, $2, NULL, 'whatsapp', 'uninstall', $3, $4::jsonb)
        """,
        uuid7(),
        tenant_id,
        status,
        json.dumps(context),
    )
    return True


async def uninstall_whatsapp_installation(
    pool: asyncpg.Pool,
    *,
    phone_number_id: str,
    secret_store: Any | None = None,
    dry_run: bool = False,
) -> WhatsAppUninstallResult:
    """Disable one WhatsApp installation and remove all stored credential refs."""

    row = await _load_installation(pool, phone_number_id=phone_number_id)
    if row is None:
        return WhatsAppUninstallResult(
            found=False,
            phone_number_id=phone_number_id,
            dry_run=dry_run,
        )

    refs = [row[field] for field in _REF_FIELDS if row[field]]
    plaintext_present = any(
        row[field] is not None
        for field in ("app_secret", "verify_token", "access_token")
    )
    if dry_run:
        return WhatsAppUninstallResult(
            found=True,
            phone_number_id=phone_number_id,
            enabled_before=bool(row["enabled"]),
            refs_seen=len(refs),
            refs_deleted=0,
            plaintext_cleared=False,
            dry_run=True,
        )

    store = secret_store or build_secret_store(pool)
    refs_deleted = 0
    secret_delete_errors = 0
    for ref in refs:
        try:
            await store.delete(str(ref), tenant_id=row["tenant_id"])
            refs_deleted += 1
        except SecretNotFoundError:
            refs_deleted += 1
        except Exception:
            secret_delete_errors += 1

    clear_refs = secret_delete_errors == 0
    await pool.execute(
        """
        UPDATE whatsapp_installations
           SET enabled = false,
               app_secret = NULL,
               verify_token = NULL,
               access_token = NULL,
               app_secret_ref = CASE WHEN $2 THEN NULL ELSE app_secret_ref END,
               verify_token_ref = CASE WHEN $2 THEN NULL ELSE verify_token_ref END,
               access_token_ref = CASE WHEN $2 THEN NULL ELSE access_token_ref END,
               updated_at = now()
         WHERE id = $1
        """,
        row["id"],
        clear_refs,
    )
    audit_written = await _write_audit(
        pool,
        tenant_id=row["tenant_id"],
        status="ok" if clear_refs else "error",
        phone_number_id=phone_number_id,
        refs_seen=len(refs),
        refs_deleted=refs_deleted,
        secret_delete_errors=secret_delete_errors,
    )
    return WhatsAppUninstallResult(
        found=True,
        phone_number_id=phone_number_id,
        enabled_before=bool(row["enabled"]),
        refs_seen=len(refs),
        refs_deleted=refs_deleted,
        secret_delete_errors=secret_delete_errors,
        plaintext_cleared=plaintext_present,
        audit_written=audit_written,
        dry_run=False,
    )


async def _amain(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument("--phone-number-id", required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report the matching row without disabling or deleting refs.",
    )
    args = parser.parse_args(argv)
    if not args.dsn:
        print(json.dumps({"ok": False, "error": "DATABASE_URL is not set"}))
        return 2

    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)
    try:
        result = await uninstall_whatsapp_installation(
            pool,
            phone_number_id=args.phone_number_id,
            dry_run=args.dry_run,
        )
    finally:
        await pool.close()

    print(json.dumps({"ok": True, **asdict(result)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
