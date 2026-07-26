"""AwsPollGenerator — synthetic live CloudTrail poll events (IN-AWS).

Drives the PRODUCTION AWS live POLL path in-process, the way
TelegramGatewayGenerator does for Telegram and DiscordGatewayGenerator does for
Discord:

  Generator → mint one fresh CloudTrail-shaped event (unique eventId + a current
              partition-window timestamp)
            → resolve the tenant's aws_installations row (account_id + region)
              from the pool
            → tag the event with `_fyralis_account_id` / `_fyralis_region`
            → build `live_poll.PollDeps`
            → `live_poll.handle_polled_event(event, deps)`
            → cutover `shadow_write_raw(source="aws", ingress_kind="poll")`
              → ingestion.raw.aws → normalizer → observation_writer.

AWS is NOT an HTTP-webhook provider; its live path is an SQS / EventBridge POLL.
So — exactly like Telegram's gateway dispatch — the live observation flows through
the SAME normalizer→observation_writer Kafka chain as backfill, landing in
`observations` while backfill is still in flight. There is no HTTP status to
assert (the generator returns None for it, like Telegram/Discord).

The live event is namespaced by the REAL aws_installations (account_id, region)
for the tenant, so the live external_id is derived through the same `aws_event`
constructor as backfill — giving genuine cross-path parity AND per-tenant
uniqueness. Each `simulate_event` uses a globally-unique eventId (a per-call UUID
disjoint from the fixture's deterministic ids) and a 2026-06 timestamp, so N live
events ⇒ N distinct observations.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


log = logging.getLogger(__name__)

# Current-window base (2026-06-xx, epoch MILLISECONDS — the handler reads the
# synthetic `eventTime` as epoch ms) so live timestamps land inside the
# observations partition coverage and are distinct from the 2026-01 backfill
# window.
_LIVE_BASE_MS = 1781000000000
# Live event sequence steps forward 1s per call so timestamps stay distinct.
_LIVE_STEP_MS = 1000


@dataclass
class AwsPollResult:
    http_status: int | None  # always None — poll dispatch has no HTTP status
    external_hint: str
    tenant_id: UUID | None = None


@dataclass(frozen=True)
class _ResolvedInstall:
    installation_id: str
    account_id: str
    region: str


class AwsPollGenerator:
    """Drives AWS live poll events for all aws targets via direct dispatch."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        kafka_producer: Any = None,
        s3_raw_client: Any = None,
        tenant_flags: Any = None,
    ) -> None:
        self._pool = pool
        self._producer = kafka_producer
        self._s3 = s3_raw_client
        self._flags = tenant_flags
        self._seq = 0
        self._install_cache: dict[tuple[UUID, str, str], _ResolvedInstall] = {}
        self._actor_repo: Any = None
        self._alias_repo: Any = None

    async def __aenter__(self) -> "AwsPollGenerator":
        from services.domain.actors.repo import ActorRepo
        from services.domain.entity_aliases.repo import EntityAliasRepo
        self._actor_repo = ActorRepo(self._pool)
        self._alias_repo = EntityAliasRepo(self._pool)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        return None

    async def _resolve_install(
        self,
        tenant_id: UUID,
        account_id: str,
        region: str,
    ) -> _ResolvedInstall:
        """Resolve exactly one active install for the target AWS scope."""
        cache_key = (tenant_id, account_id, region)
        cached = self._install_cache.get(cache_key)
        if cached is not None:
            return cached
        rows = await self._pool.fetch(
            "SELECT id, account_id, region FROM aws_installations "
            "WHERE tenant_id = $1 AND account_id = $2 AND region = $3 "
            "AND disabled_at IS NULL",
            tenant_id,
            account_id,
            region,
        )
        if len(rows) != 1:
            raise ValueError(
                "aws target must resolve to exactly one active installation: "
                f"tenant_id={tenant_id}, account_id={account_id!r}, "
                f"region={region!r}, matches={len(rows)}",
            )
        row = rows[0]
        resolved = _ResolvedInstall(
            installation_id=str(row["id"]),
            account_id=str(row["account_id"]),
            region=str(row["region"]),
        )
        self._install_cache[cache_key] = resolved
        return resolved

    def _mint_event(
        self, resolved: _ResolvedInstall, content: str,
    ) -> dict[str, Any]:
        self._seq += 1
        event_id = str(uuid.uuid4())
        return {
            "eventId": event_id,
            "eventName": "StartInstances",
            "eventSource": "ec2.amazonaws.com",
            "eventTime": _LIVE_BASE_MS + self._seq * _LIVE_STEP_MS,
            "awsRegion": resolved.region,
            "recipientAccountId": resolved.account_id,
            "userIdentity": {
                "type": "AssumedRole",
                "arn": f"arn:aws:iam::{resolved.account_id}:role/live-ops",
                "userName": f"live_ops_{self._seq}",
            },
            "cloudTrailEvent": {
                "eventVersion": "1.08",
                "eventID": event_id,
                "managementEvent": True,
                "message": content,
            },
            # Pre-tag the namespace so handle_polled_event resolves the install
            # back to this tenant (account_id, region).
            "_fyralis_account_id": resolved.account_id,
            "_fyralis_region": resolved.region,
        }

    async def simulate_event(
        self, *, target: "Any", content: str = "hello",
    ) -> AwsPollResult:
        """Mint one fresh live event + dispatch it through the production path."""
        from services.ingest.integrations.aws.live_poll import (
            PollDeps,
            handle_polled_event,
        )

        account_id = target.aws_account_id
        region = target.aws_region
        if account_id is None or region is None:
            raise ValueError(
                "aws target is missing aws_account_id or aws_region",
            )
        resolved = await self._resolve_install(
            target.tenant_id,
            account_id,
            region,
        )
        event = self._mint_event(resolved, content)
        deps = PollDeps(
            pool=self._pool,
            tenant_id=target.tenant_id,
            installation_id=resolved.installation_id,
            actor_repo=self._actor_repo,
            alias_repo=self._alias_repo,
            embedder=None,
            s3_raw_client=self._s3,
            kafka_producer=self._producer,
            tenant_flags=self._flags,
        )
        await handle_polled_event(event, deps)
        return AwsPollResult(
            http_status=None,
            external_hint=(
                f"aws:{resolved.account_id}:{resolved.region}:event:{event['eventId']}"
            ),
            tenant_id=getattr(target, "tenant_id", None),
        )


__all__ = ["AwsPollGenerator", "AwsPollResult"]
