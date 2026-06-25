#!/usr/bin/env python3
"""Migrate legacy WhatsApp plaintext credentials into encrypted secret refs.

This is an idempotent rollout helper for migration 0166. It leaves rows with no
legacy plaintext untouched, creates encrypted_secrets rows only for missing refs,
and clears legacy plaintext columns once refs are present.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict, dataclass
from typing import Any, Sequence

import asyncpg

from lib.shared.secrets import build_secret_store


_SECRET_FIELDS = (
    ("app_secret", "app_secret_ref", "whatsapp_app_secret"),
    ("verify_token", "verify_token_ref", "whatsapp_verify_token"),
    ("access_token", "access_token_ref", "whatsapp_access_token"),
)


@dataclass(frozen=True, slots=True)
class WhatsAppSecretRefMigrationResult:
    rows_seen: int = 0
    rows_updated: int = 0
    refs_created: int = 0
    dry_run: bool = False


async def _rows_with_legacy_plaintext(
    pool: asyncpg.Pool,
    *,
    limit: int | None,
) -> list[asyncpg.Record]:
    sql = """
        SELECT id, tenant_id, phone_number_id,
               app_secret, app_secret_ref,
               verify_token, verify_token_ref,
               access_token, access_token_ref
          FROM whatsapp_installations
         WHERE app_secret IS NOT NULL
            OR verify_token IS NOT NULL
            OR access_token IS NOT NULL
         ORDER BY updated_at, id
    """
    if limit is not None:
        sql += " LIMIT $1"
        return list(await pool.fetch(sql, limit))
    return list(await pool.fetch(sql))


async def _delete_created_refs(
    secret_store: Any,
    *,
    tenant_id: Any,
    refs: list[str],
) -> None:
    for ref in refs:
        await secret_store.delete(ref, tenant_id=tenant_id)


async def migrate_whatsapp_secret_refs(
    pool: asyncpg.Pool,
    *,
    secret_store: Any | None = None,
    dry_run: bool = False,
    limit: int | None = None,
) -> WhatsAppSecretRefMigrationResult:
    """Move legacy WhatsApp plaintext columns into encrypted secret refs."""

    rows = await _rows_with_legacy_plaintext(pool, limit=limit)
    if dry_run:
        return WhatsAppSecretRefMigrationResult(
            rows_seen=len(rows),
            rows_updated=0,
            refs_created=0,
            dry_run=True,
        )

    store = secret_store or build_secret_store(pool)
    rows_updated = 0
    refs_created = 0

    for row in rows:
        tenant_id = row["tenant_id"]
        phone_number_id = row["phone_number_id"]
        new_refs: dict[str, str | None] = {}
        created_refs: list[str] = []
        try:
            for plaintext_field, ref_field, label_prefix in _SECRET_FIELDS:
                plaintext = row[plaintext_field]
                existing_ref = row[ref_field]
                if plaintext is None or existing_ref is not None:
                    new_refs[ref_field] = None
                    continue
                ref = await store.put(
                    str(plaintext),
                    label=f"{label_prefix}:{phone_number_id}",
                    tenant_id=tenant_id,
                )
                new_refs[ref_field] = ref
                created_refs.append(ref)

            status = await pool.execute(
                """
                UPDATE whatsapp_installations
                   SET app_secret_ref = COALESCE(app_secret_ref, $2),
                       verify_token_ref = COALESCE(verify_token_ref, $3),
                       access_token_ref = COALESCE(access_token_ref, $4),
                       app_secret = NULL,
                       verify_token = NULL,
                       access_token = NULL,
                       updated_at = now()
                 WHERE id = $1
                """,
                row["id"],
                new_refs.get("app_secret_ref"),
                new_refs.get("verify_token_ref"),
                new_refs.get("access_token_ref"),
            )
        except Exception:
            await _delete_created_refs(store, tenant_id=tenant_id, refs=created_refs)
            raise

        if status == "UPDATE 1":
            rows_updated += 1
            refs_created += len(created_refs)

    return WhatsAppSecretRefMigrationResult(
        rows_seen=len(rows),
        rows_updated=rows_updated,
        refs_created=refs_created,
        dry_run=False,
    )


async def _amain(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report matching rows without creating refs or clearing plaintext.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of installation rows to process.",
    )
    args = parser.parse_args(argv)
    if not args.dsn:
        print(
            json.dumps({"ok": False, "error": "DATABASE_URL is not set"}),
            flush=True,
        )
        return 2
    if args.limit is not None and args.limit <= 0:
        print(json.dumps({"ok": False, "error": "--limit must be positive"}))
        return 2

    pool = await asyncpg.create_pool(args.dsn, min_size=1, max_size=2)
    try:
        result = await migrate_whatsapp_secret_refs(
            pool,
            dry_run=args.dry_run,
            limit=args.limit,
        )
    finally:
        await pool.close()

    print(json.dumps({"ok": True, **asdict(result)}, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
