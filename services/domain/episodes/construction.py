"""Episode settlement policies and snapshot construction coordination."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

import asyncpg

from .contracts import EpisodeSettlement, EpisodeSnapshot
from .handoff import EpisodeSnapshotOutboxRepository
from .lifecycle import EpisodeLifecycleRepository
from .snapshot import EpisodeSnapshotService


class EpisodeConstructionService:
    settlement_rule_version = "1.0.0"

    def __init__(self) -> None:
        self._lifecycle = EpisodeLifecycleRepository()
        self._snapshots = EpisodeSnapshotService()
        self._handoff = EpisodeSnapshotOutboxRepository()

    async def ensure_opened(
        self, episode_id: UUID, *, tenant_id: UUID, conn: asyncpg.Connection
    ) -> None:
        row = await conn.fetchrow(
            "SELECT opened_at,last_event_at,last_ingested_at FROM episodes "
            "WHERE id=$1 AND tenant_id=$2",
            episode_id, tenant_id,
        )
        if row is None:
            raise ValueError("episode not found")
        if not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM episode_lifecycle_events "
            "WHERE tenant_id=$1 AND episode_id=$2)",
            tenant_id, episode_id,
        ):
            await self._lifecycle.transition(
                episode_id, tenant_id=tenant_id, to_state="open", event_kind="opened",
                event_time_watermark=row["last_event_at"],
                ingestion_time_watermark=row["last_ingested_at"],
                rule_name="first_membership", rule_version=self.settlement_rule_version,
                cause_ref={"kind": "episode_created"}, conn=conn,
            )

    async def mark_dormant(
        self,
        episode_id: UUID,
        *,
        tenant_id: UUID,
        quiet_period: timedelta,
        evaluated_at: datetime | None = None,
        conn: asyncpg.Connection,
    ) -> bool:
        now = evaluated_at or datetime.now(UTC)
        row = await conn.fetchrow(
            "SELECT lifecycle_state,last_event_at,last_ingested_at FROM episodes "
            "WHERE id=$1 AND tenant_id=$2",
            episode_id, tenant_id,
        )
        if row is None or row["lifecycle_state"] not in {"open", "reopened"}:
            return False
        if now - row["last_ingested_at"] < quiet_period:
            return False
        await self._lifecycle.transition(
            episode_id, tenant_id=tenant_id, to_state="dormant", event_kind="dormant",
            event_time_watermark=row["last_event_at"],
            ingestion_time_watermark=row["last_ingested_at"],
            rule_name="quiet_period_candidate", rule_version=self.settlement_rule_version,
            cause_ref={"quiet_seconds": int(quiet_period.total_seconds())}, conn=conn,
        )
        return True

    async def settle(
        self,
        episode_id: UUID,
        *,
        tenant_id: UUID,
        reason: Literal["quiet_period", "explicit_close", "query_scope_satisfied", "superseded"],
        evaluated_at: datetime | None = None,
        conn: asyncpg.Connection,
    ) -> EpisodeSnapshot:
        now = evaluated_at or datetime.now(UTC)
        row = await conn.fetchrow(
            "SELECT lifecycle_state,last_event_at,last_ingested_at FROM episodes "
            "WHERE id=$1 AND tenant_id=$2",
            episode_id, tenant_id,
        )
        if row is None:
            raise ValueError("episode not found")
        await self.ensure_opened(episode_id, tenant_id=tenant_id, conn=conn)
        state = str(row["lifecycle_state"])
        if state == "settled":
            existing = await conn.fetchrow(
                "SELECT manifest,snapshot_hash FROM episode_snapshots "
                "WHERE tenant_id=$1 AND episode_id=$2 ORDER BY version DESC LIMIT 1",
                tenant_id, episode_id,
            )
            if existing is not None:
                manifest = existing["manifest"]
                if isinstance(manifest, str):
                    manifest = json.loads(manifest)
                snapshot = EpisodeSnapshot.model_validate(
                    {**manifest, "snapshot_hash": existing["snapshot_hash"]}
                )
                await self._handoff.enqueue(snapshot, conn=conn)
                return snapshot
        if state != "settled":
            await self._lifecycle.transition(
                episode_id, tenant_id=tenant_id, to_state="settled", event_kind="settled",
                event_time_watermark=row["last_event_at"],
                ingestion_time_watermark=row["last_ingested_at"],
                rule_name=reason, rule_version=self.settlement_rule_version,
                cause_ref={"kind": "settlement_policy"}, conn=conn,
            )
        settlement = EpisodeSettlement(
            reason=reason,
            rule_version=self.settlement_rule_version,
            event_time_watermark=row["last_event_at"],
            ingestion_time_watermark=row["last_ingested_at"],
            settled_at=now,
        )
        snapshot = await self._snapshots.seal(
            episode_id, tenant_id=tenant_id, settlement=settlement,
            conn=conn, created_at=now,
        )
        await self._handoff.enqueue(snapshot, conn=conn)
        return snapshot

    async def reopen_for_late_evidence(
        self,
        episode_id: UUID,
        *,
        tenant_id: UUID,
        membership_id: UUID,
        conn: asyncpg.Connection,
    ) -> bool:
        row = await conn.fetchrow(
            "SELECT lifecycle_state,last_event_at,last_ingested_at FROM episodes "
            "WHERE id=$1 AND tenant_id=$2",
            episode_id, tenant_id,
        )
        if row is None or row["lifecycle_state"] != "settled":
            return False
        await self._lifecycle.transition(
            episode_id, tenant_id=tenant_id, to_state="reopened", event_kind="reopened",
            event_time_watermark=row["last_event_at"],
            ingestion_time_watermark=row["last_ingested_at"],
            rule_name="material_late_evidence", rule_version=self.settlement_rule_version,
            cause_ref={"kind": "episode_membership", "id": str(membership_id)},
            conn=conn,
        )
        return True


__all__ = ["EpisodeConstructionService"]
