"""Fail-closed queue-membership evidence for core fast-path receipts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import asyncpg


def _payload(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, Mapping) else {}
    return {}


def _uuid_set(value: Any) -> set[UUID] | None:
    if not isinstance(value, list):
        return None
    try:
        result = {UUID(str(item)) for item in value}
    except (TypeError, ValueError, AttributeError):
        return None
    return result if len(result) == len(value) else None


def _member_batch_label(payload: Mapping[str, Any]) -> str | None:
    probe = payload.get("mega_probe")
    if isinstance(probe, Mapping) and probe.get("run_id") is not None:
        return str(probe["run_id"])
    return str(payload["run_id"]) if payload.get("run_id") is not None else None


async def proven_batch_observation_ids(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    run_id: UUID,
    expected_observation_ids: set[UUID] | frozenset[UUID],
    batch_label: str,
) -> tuple[UUID, ...]:
    """Return exact completed queue membership, or no evidence on any mismatch."""

    expected = set(expected_observation_ids)
    if not expected or not batch_label:
        return ()
    run = await conn.fetchrow(
        """SELECT trigger_id,status,trigger_kind
             FROM think_runs WHERE tenant_id=$1 AND id=$2""",
        tenant_id,
        run_id,
    )
    if (
        run is None
        or run["status"] != "success"
        or run["trigger_kind"] != "T1:event_batch"
    ):
        return ()
    parent = await conn.fetchrow(
        """SELECT id,payload,completed_at,trigger_kind,trigger_subkind
             FROM think_trigger_queue WHERE tenant_id=$1 AND id=$2""",
        tenant_id,
        run["trigger_id"],
    )
    if (
        parent is None
        or parent["completed_at"] is None
        or parent["trigger_kind"] != "T1"
        or parent["trigger_subkind"] != "event_batch"
    ):
        return ()
    members = await conn.fetch(
        """SELECT id,observation_id,payload,completed_at,batch_parent_id
             FROM think_trigger_queue
            WHERE tenant_id=$1 AND batch_parent_id=$2 ORDER BY id""",
        tenant_id,
        parent["id"],
    )
    if not members:
        return ()
    member_ids = {row["id"] for row in members}
    observations = {row["observation_id"] for row in members}
    if (
        len(member_ids) != len(members)
        or None in observations
        or observations != expected
        or len(observations) != len(members)
        or any(row["completed_at"] is None for row in members)
        or any(row["batch_parent_id"] != parent["id"] for row in members)
        or any(
            _member_batch_label(_payload(row["payload"])) != batch_label
            for row in members
        )
    ):
        return ()
    parent_payload = _payload(parent["payload"])
    declared = _uuid_set(parent_payload.get("batch_member_trigger_ids"))
    aliases = _uuid_set(parent_payload.get("member_trigger_ids"))
    if declared != member_ids or (aliases is not None and aliases != member_ids):
        return ()
    return tuple(sorted(observations, key=str))


__all__ = ["proven_batch_observation_ids"]
