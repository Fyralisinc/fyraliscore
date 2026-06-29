#!/usr/bin/env python3
"""Operator tool: inspect and re-enable tenants the cutover circuit breaker
tripped off the Kafka path.

The breaker is deliberately one-directional — on sustained per-source lag it
flips `ingestion.kafka_path_enabled` to FALSE (pulling the tenant to the inline
path) and never flips it back, to avoid flapping during an incident. Recovery
is an explicit operator action, and there is no UI for it; this is that action.

After you flip a tenant back to TRUE, the breaker's NEXT tick observes
flag=TRUE on a tenant whose state row still says tripped=TRUE and auto-resets
its own bookkeeping (counter→0, tripped→FALSE) so it can trip again later — you
do NOT need to touch `circuit_breaker_state` yourself.

Usage:
    # Show every tenant currently forced onto the inline path:
    DATABASE_URL=postgresql://... python scripts/reenable_kafka_path.py --list

    # Re-enable a tenant after you've confirmed the broker/normalizer recovered:
    DATABASE_URL=postgresql://... python scripts/reenable_kafka_path.py \
        <tenant-uuid> --operator-actor <actor-uuid> \
        --note "slack lane drained, broker healthy"
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.ingest.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
)
from services.platform.operator_auth import require_tenant_operator


class KafkaPathCliError(ValueError):
    """Operator-facing validation error."""

_LIST_SQL = """
SELECT tf.tenant_id,
       tf.set_by,
       tf.note,
       tf.set_at,
       cb.tripped,
       cb.tripped_at,
       cb.consecutive_breach_ticks
  FROM tenant_flags tf
  LEFT JOIN circuit_breaker_state cb ON cb.tenant_id = tf.tenant_id
 WHERE tf.flag_name = $1
   AND tf.flag_value = FALSE
 ORDER BY tf.set_at DESC
"""


async def _list_disabled(pool: asyncpg.Pool) -> int:
    rows = await pool.fetch(_LIST_SQL, KAFKA_PATH_ENABLED)
    if not rows:
        print("No tenants have ingestion.kafka_path_enabled=FALSE — "
              "all on the Kafka path.")
        return 0
    print(f"{len(rows)} tenant(s) currently forced to the inline path:\n")
    for r in rows:
        tripped = r["tripped"]
        marker = "auto-tripped" if tripped else "operator/other"
        print(f"  • {r['tenant_id']}  [{marker}]")
        print(f"      set_by={r['set_by']!r}  set_at={r['set_at']}")
        if r["note"]:
            print(f"      note={r['note']!r}")
        if tripped is not None:
            print(f"      breaker_state: tripped={tripped} "
                  f"tripped_at={r['tripped_at']} "
                  f"breach_ticks={r['consecutive_breach_ticks']}")
        print()
    return 0


async def _reenable(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    operator_actor_id: UUID,
    note: str | None,
) -> int:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1, false)",
                str(tenant_id),
            )
            await require_tenant_operator(
                conn,
                tenant_id=tenant_id,
                actor_id=operator_actor_id,
                error_type=KafkaPathCliError,
            )
            flags = TenantFlags(conn)
            before = await conn.fetchrow(
                "SELECT flag_value, set_by FROM tenant_flags "
                "WHERE tenant_id = $1 AND flag_name = $2",
                tenant_id, KAFKA_PATH_ENABLED,
            )
            if before is None:
                await _record_operator_action(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=operator_actor_id,
                    metadata={
                        "flag_name": KAFKA_PATH_ENABLED,
                        "changed": False,
                        "reason": "missing_row_default_true",
                        "note": note,
                    },
                )
                print(f"{tenant_id} has no kafka_path_enabled row — it is already "
                      "kafka-first (default). Nothing to do.")
                return 0
            if before["flag_value"] is True:
                await _record_operator_action(
                    conn,
                    tenant_id=tenant_id,
                    actor_id=operator_actor_id,
                    metadata={
                        "flag_name": KAFKA_PATH_ENABLED,
                        "changed": False,
                        "reason": "already_enabled",
                        "set_by_before": before["set_by"],
                        "note": note,
                    },
                )
                print(f"{tenant_id} is already on the Kafka path "
                      f"(flag=TRUE, set_by={before['set_by']!r}). Nothing to do.")
                return 0

            await flags.set_bool(
                tenant_id, KAFKA_PATH_ENABLED, True,
                set_by=f"operator:{operator_actor_id}",
                note=note or "operator re-enable after circuit-breaker trip",
            )
            after = await conn.fetchrow(
                "SELECT flag_value, set_by, note FROM tenant_flags "
                "WHERE tenant_id = $1 AND flag_name = $2",
                tenant_id, KAFKA_PATH_ENABLED,
            )
            await _record_operator_action(
                conn,
                tenant_id=tenant_id,
                actor_id=operator_actor_id,
                metadata={
                    "flag_name": KAFKA_PATH_ENABLED,
                    "changed": True,
                    "previous_value": before["flag_value"],
                    "set_by_before": before["set_by"],
                    "set_by_after": after["set_by"],
                    "note": after["note"],
                },
            )
    print(f"Re-enabled {tenant_id}: flag_value={after['flag_value']} "
          f"set_by={after['set_by']!r}")
    print("The breaker will auto-reset its bookkeeping on its next tick; "
          "ingress/writer pick up the change within the 30s flag-cache TTL.")
    return 0


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    metadata: dict[str, object],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES (
            $1, $2, $3, 'kafka_path.reenable', 'tenant_flag', $4,
            $5::jsonb, now()
        )
        """,
        uuid7(),
        tenant_id,
        actor_id,
        tenant_id,
        json.dumps(metadata, sort_keys=True),
    )


async def _amain(args: argparse.Namespace) -> int:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2
    # statement_cache_size=0 for pgbouncer transaction-mode compatibility
    # (matches make_breaker_pool / the rest of the ingestion pools).
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, statement_cache_size=0)
    try:
        if args.list:
            return await _list_disabled(pool)
        return await _reenable(
            pool,
            args.tenant_id,
            args.operator_actor,
            args.note,
        )
    finally:
        await pool.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("tenant_id", nargs="?", type=UUID,
                   help="tenant UUID to re-enable onto the Kafka path")
    p.add_argument("--list", action="store_true",
                   help="list every tenant with kafka_path_enabled=FALSE and exit")
    p.add_argument("--operator-actor", type=UUID,
                   help="tenant admin/leadership actor UUID performing the action")
    p.add_argument("--note", default=None, help="audit note for the flip")
    args = p.parse_args()
    if not args.list and args.tenant_id is None:
        p.error("provide a tenant UUID to re-enable, or --list")
    if not args.list and args.operator_actor is None:
        p.error("--operator-actor is required when re-enabling a tenant")
    try:
        return asyncio.run(_amain(args))
    except KafkaPathCliError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
