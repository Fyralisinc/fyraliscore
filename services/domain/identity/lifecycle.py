"""Targeted re-resolution after identity corrections or cluster events."""

from __future__ import annotations

import json
from uuid import UUID

import asyncpg

from services.domain.observations.repo import ObservationRepository

from .intake import IdentityIntakeRepository, IdentityOutboxRow
from .repo import IdentityAssertionRepository


class IdentityLifecycleService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._assertions = IdentityAssertionRepository()
        self._intake = IdentityIntakeRepository()

    async def request_reresolution(
        self,
        assertion_ids: list[UUID],
        *,
        tenant_id: UUID,
        reason: str,
        conn: asyncpg.Connection,
    ) -> list[IdentityOutboxRow]:
        dependents = await self._assertions.list_dependents(
            assertion_ids, tenant_id=tenant_id, conn=conn
        )
        observation_ids = sorted(
            {
                item["dependent_id"]
                for item in dependents
                if item["dependent_kind"] == "observation"
            },
            key=str,
        )
        rows: list[IdentityOutboxRow] = []
        observation_repo = ObservationRepository(self._pool)
        for observation_id in observation_ids:
            observation = await observation_repo.get_by_id(
                observation_id, tenant_id, conn=conn
            )
            if observation is None:
                continue
            rows.append(
                await self._intake.enqueue_reprocess(
                    observation,
                    reason=reason,
                    cause_assertion_ids=tuple(sorted(set(assertion_ids), key=str)),
                    conn=conn,
                )
            )
        dedupe = (
            f"reresolution:{reason}:"
            + ",".join(str(value) for value in sorted(set(assertion_ids), key=str))
        )
        await conn.execute(
            """
            INSERT INTO identity_change_events (
              id, tenant_id, event_kind, aggregate_ref, payload, dedupe_key
            ) VALUES (
              gen_random_uuid(), $1, 'identity.reresolution_requested',
              $2::jsonb, $3::jsonb, $4
            ) ON CONFLICT (tenant_id, dedupe_key) DO NOTHING
            """,
            tenant_id,
            json.dumps(
                {"kind": "identity_assertions", "ids": [str(v) for v in assertion_ids]},
                sort_keys=True,
            ),
            json.dumps(
                {
                    "reason": reason,
                    "dependent_count": len(dependents),
                    "observation_ids": [str(value) for value in observation_ids],
                },
                sort_keys=True,
            ),
            dedupe,
        )
        return rows


__all__ = ["IdentityLifecycleService"]
