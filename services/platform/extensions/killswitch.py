"""services/platform/extensions/killswitch.py — global extension kill-switch (E3.5).

A hard, instant, global disable: while an extension is killed, the host issues it
no tokens and serves no read/write/stream for ANY tenant, regardless of grants.
Per-tenant disable is grant revocation (``ExtensionGrantsRepo.revoke``); this is
the break-glass for an extension misbehaving across all tenants.

Checked at the cheap chokepoints (token issuance + the per-request authz) so a
kill takes effect within one token TTL at worst, immediately for new requests.
"""
from __future__ import annotations

import logging
from typing import Any

import asyncpg

log = logging.getLogger("extensions.killswitch")


class KillSwitch:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def is_killed(self, extension_id: str) -> bool:
        try:
            async with self.pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM extension_killswitch WHERE extension_id=$1)",
                    extension_id,
                )
        except asyncpg.UndefinedTableError:
            return False  # migration 0130 not applied here → nothing killed
        except Exception:  # noqa: BLE001
            # Fail OPEN for the check itself (don't lock everyone out on a DB blip);
            # the grant gate is the primary access control.
            log.warning("killswitch_check_failed ext=%s", extension_id, exc_info=True)
            return False

    async def disable(self, extension_id: str, *, disabled_by: str, reason: str | None = None) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO extension_killswitch (extension_id, reason, disabled_by) "
                "VALUES ($1,$2,$3) ON CONFLICT (extension_id) DO UPDATE SET "
                "reason=EXCLUDED.reason, disabled_by=EXCLUDED.disabled_by, disabled_at=now()",
                extension_id, reason, disabled_by,
            )

    async def enable(self, extension_id: str) -> bool:
        async with self.pool.acquire() as conn:
            status = await conn.execute(
                "DELETE FROM extension_killswitch WHERE extension_id=$1", extension_id
            )
        return status.endswith(("1",))


__all__ = ["KillSwitch"]
