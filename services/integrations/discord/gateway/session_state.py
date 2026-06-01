"""services/integrations/discord/gateway/session_state.py — Discord
   Gateway session state persistence.

Per ingestion LLD §1.5 + M4.2 work order.

Replaces in-memory `session_id` / `last_seq` storage in
[client.py:94](services/integrations/discord/gateway/client.py#L94) with
Postgres durability so a pod crash + restart can RESUME the Discord
session instead of starting fresh and silently dropping the messages
Discord buffered for the dead session (Phase 1 risk #3, N1).

=== Path A — pgbouncer-compatible pool ===
Second activation of M1.3's `pgbouncer_compatible` ADR flag in
production code. First was M3.1's DLQ writer
([services/ingestion/writers/dlq_writer/dlq_writer.py:345-351](../../ingestion/writers/dlq_writer/dlq_writer.py#L345-L351)).
The `make_session_state_pool` helper here mirrors that init shape
exactly (`statement_cache_size=0`, command_timeout, min/max).

=== Staleness threshold — load-bearing operational claim ===
Discord retains a disconnected session's frame buffer for ~4-5
minutes server-side. Sessions older than that have been torn down;
RESUMing a stale session_id produces an Invalid Session opcode and
forces fresh IDENTIFY anyway (losing whatever Discord had still been
holding). We use **4 minutes** (the conservative end of the
documented window) as the staleness cutoff: state older than that
returns None from `load_session_state` so the worker IDENTIFYs fresh
on the spot rather than discovering the staleness via a roundtrip
to Discord.

If a future change wants a different threshold, surface as an LLD
amendment, not a quiet edit — `test_load_returns_none_when_stale`
locks the contract.

=== Save-after-handle ordering (N1 contract) ===
The save call site in [client.py / worker.py] (M4.3 wires this) MUST
fire AFTER the dispatch handler returns durably (after the shadow
write succeeds OR fails non-fatally), NOT before. Reasoning at the
call site, also restated here for the implementer:

  - "Save before handle" would mean a crash between save and handle
    leaves persisted state pointing past a frame that was never
    processed. The next worker RESUMEs from seq=N and Discord never
    re-delivers frame N — silent data loss.
  - "Save after handle" means a crash loses at most one save, which
    means re-processing one frame on the next session. Re-processing
    is safe under M2's content_hash dedup (the observation UNIQUE
    on (source_channel, external_id) absorbs the duplicate).

This is N1 (Never lose data) enforced at the per-frame level.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict

from lib.shared.ids import uuid7


log = logging.getLogger(__name__)


# Per the M4.2 work order + Discord session retention docs. Load-bearing
# — see module docstring. Test_load_returns_none_when_stale asserts this.
STALENESS_THRESHOLD = dt.timedelta(minutes=4)


class PersistedGatewaySession(BaseModel):
    """Pydantic mapping of the `gateway_session_state` row (LLD §1.5).

    Named distinctly from the in-memory dataclass
    [client.py::GatewaySessionState](client.py) to keep the runtime
    state (mutable, struct-of-arrays) and the persistence row (Pydantic,
    versioned) as separate types. M4.3 will translate between them at
    the worker's startup and at the per-frame save site.
    """

    model_config = ConfigDict(extra="forbid")

    id: UUID
    shard_id: int = 0
    application_id: str
    session_id: str | None = None
    resume_gateway_url: str | None = None
    last_seq: int | None = None
    heartbeat_interval_ms: int | None = None
    last_heartbeat_ack_at: dt.datetime | None = None
    last_dispatched_at: dt.datetime | None = None
    leader_lease_holder: str | None = None
    leader_lease_expires_at: dt.datetime | None = None
    updated_at: dt.datetime


# ---------------------------------------------------------------------
# Pool helper — Path A pgbouncer-compatible.
# ---------------------------------------------------------------------
async def make_session_state_pool(
    dsn: str,
    *,
    max_size: int = 5,
    command_timeout: float = 10.0,
) -> asyncpg.Pool:
    """Construct an asyncpg pool sized for the gateway worker's
    session-state UPSERTs (one per dispatched frame).

    Per M1.3 ADR Q1 (pgbouncer sidecar in transaction mode):
    `statement_cache_size=0` disables asyncpg's per-connection prepared
    statement cache so a pgbouncer-pooled connection (which may swap
    underlying backend connections between calls) doesn't see stale
    statement IDs.

    Mirrors M3.1's pool init at
    [services/ingestion/writers/dlq_writer/dlq_writer.py:345-351](../../ingestion/writers/dlq_writer/dlq_writer.py#L345-L351).
    """
    return await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=max_size,
        command_timeout=command_timeout,
        statement_cache_size=0,  # pgbouncer transaction mode (M1.3 ADR Q1)
    )


# ---------------------------------------------------------------------
# Load.
# ---------------------------------------------------------------------
_LOAD_SQL = """
SELECT
    id, shard_id, application_id, session_id, resume_gateway_url,
    last_seq, heartbeat_interval_ms, last_heartbeat_ack_at,
    last_dispatched_at, leader_lease_holder, leader_lease_expires_at,
    updated_at
FROM gateway_session_state
WHERE application_id = $1 AND shard_id = $2
LIMIT 1
"""


async def load_session_state(
    pool: asyncpg.Pool,
    *,
    application_id: str,
    shard_id: int = 0,
    now: dt.datetime | None = None,
) -> PersistedGatewaySession | None:
    """Read the persisted session state for `(application_id, shard_id)`.

    Returns None if:
      - no row exists, OR
      - the row's `updated_at` is older than `STALENESS_THRESHOLD`
        (4 minutes). Discord has already torn down the buffered
        session at the 4-5 min mark; returning the row anyway would
        cause the worker to attempt RESUME, receive Invalid Session,
        and IDENTIFY fresh — a wasted roundtrip. Better to short-
        circuit at load time.

    `now` is overridable for tests. Production always passes None
    (defaults to `datetime.now(UTC)`).
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    row = await pool.fetchrow(_LOAD_SQL, application_id, shard_id)
    if row is None:
        return None

    state = PersistedGatewaySession(
        id=row["id"],
        shard_id=row["shard_id"],
        application_id=row["application_id"],
        session_id=row["session_id"],
        resume_gateway_url=row["resume_gateway_url"],
        last_seq=row["last_seq"],
        heartbeat_interval_ms=row["heartbeat_interval_ms"],
        last_heartbeat_ack_at=row["last_heartbeat_ack_at"],
        last_dispatched_at=row["last_dispatched_at"],
        leader_lease_holder=row["leader_lease_holder"],
        leader_lease_expires_at=row["leader_lease_expires_at"],
        updated_at=row["updated_at"],
    )

    # The staleness check. 4 minutes is the conservative cutoff
    # documented at STALENESS_THRESHOLD; see module docstring.
    age = now - state.updated_at
    if age > STALENESS_THRESHOLD:
        log.info(
            "session_state.stale",
            extra={
                "application_id": application_id,
                "shard_id": shard_id,
                "age_seconds": age.total_seconds(),
                "threshold_seconds": STALENESS_THRESHOLD.total_seconds(),
            },
        )
        return None

    return state


# ---------------------------------------------------------------------
# Save (UPSERT).
# ---------------------------------------------------------------------
# Per LLD §1.5: `UPSERT via ON CONFLICT (application_id, shard_id) DO
# UPDATE`. The id column is set on INSERT only (via COALESCE pattern:
# preserve the existing id if the row already exists).
_SAVE_SQL = """
INSERT INTO gateway_session_state (
    id, shard_id, application_id, session_id, resume_gateway_url,
    last_seq, heartbeat_interval_ms, last_heartbeat_ack_at,
    last_dispatched_at, leader_lease_holder, leader_lease_expires_at,
    updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (application_id, shard_id) DO UPDATE SET
    session_id              = EXCLUDED.session_id,
    resume_gateway_url      = EXCLUDED.resume_gateway_url,
    last_seq                = EXCLUDED.last_seq,
    heartbeat_interval_ms   = EXCLUDED.heartbeat_interval_ms,
    last_heartbeat_ack_at   = EXCLUDED.last_heartbeat_ack_at,
    last_dispatched_at      = EXCLUDED.last_dispatched_at,
    leader_lease_holder     = EXCLUDED.leader_lease_holder,
    leader_lease_expires_at = EXCLUDED.leader_lease_expires_at,
    updated_at              = EXCLUDED.updated_at
"""


async def save_session_state(
    pool: asyncpg.Pool,
    *,
    application_id: str,
    session_id: str | None,
    resume_gateway_url: str | None,
    last_seq: int | None,
    shard_id: int = 0,
    heartbeat_interval_ms: int | None = None,
    last_heartbeat_ack_at: dt.datetime | None = None,
    last_dispatched_at: dt.datetime | None = None,
    leader_lease_holder: str | None = None,
    leader_lease_expires_at: dt.datetime | None = None,
    now: dt.datetime | None = None,
) -> None:
    """UPSERT the persisted session for `(application_id, shard_id)`.

    Callers MUST invoke this AFTER the frame has been durably handled
    (after the M2.2 shadow write returned, success or non-fatal
    failure), NOT before. See module docstring "Save-after-handle
    ordering (N1 contract)." The M4.3 call site documents this at the
    invocation point.

    `now` is overridable for tests — production passes None.
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)
    # uuid7() is only used on first INSERT; ON CONFLICT preserves
    # the existing id. We still need to pass *some* uuid7 for the
    # INSERT path (positional argument).
    new_id = uuid7()
    await pool.execute(
        _SAVE_SQL,
        new_id,
        shard_id,
        application_id,
        session_id,
        resume_gateway_url,
        last_seq,
        heartbeat_interval_ms,
        last_heartbeat_ack_at,
        last_dispatched_at,
        leader_lease_holder,
        leader_lease_expires_at,
        now,
    )


__all__ = [
    "PersistedGatewaySession",
    "STALENESS_THRESHOLD",
    "load_session_state",
    "make_session_state_pool",
    "save_session_state",
]
