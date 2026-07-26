"""services/ingest/ingestion/workflows/shard_fetch.py
   — M6.2a ShardFetch service. The N1 primitive's first real
     production consumer.

Per ingestion LLD §2 (ShardFetchWorkflow shape, ported to asyncio
per [05-lld-amendments.md A11]) + §3.1 (cursor-data ordering
invariant — the N1 primitive's contract) + A15 (onboarding_shards
column-naming map; cursor home is workflow_states.state_data, not
onboarding_shards.cursor_token).

============================================================
THE N1 INVARIANT — ShardFetch IS THE PRIMARY CONSUMER
============================================================
This service is the first real production consumer of
`state.advance_cursor_atomic_with_kafka_publish`. The N1 invariant
(LLD §3.1; M6.0 substrate):

  1. Publish every Kafka record in the page.
  2. Flush the producer; await broker-acks.
  3. ONLY IF flush returned 0 (all messages broker-acked): UPDATE
     the workflow_states row with the new cursor.

If step 2 fails, `CursorAdvanceFlushFailure` is raised and the state
row is UNCHANGED. ShardFetch catches this, exits the fetch loop
without marking the shard 'done', and leaves the shard in
'in_progress' state. The shard's `workflow_states.state_data["cursor"]`
holds the LAST successfully-advanced cursor (i.e., the one before
the failed page). The next service tick (or restart) resumes the
fetch loop from that cursor and re-attempts the failed page.

The Kafka idempotent-producer dedups the broker side; the
downstream observation UNIQUE constraint dedups the writer side.
The N1 invariant — "publish-then-advance, never advance-then-publish"
— holds end-to-end.

============================================================
S3-WRITE-BEFORE-PUBLISH (M6.7 / A27.1)
============================================================
M6.7 makes ShardFetch a real backfill PRODUCER: each fetched record
is written to the raw tier (S3, content-addressed via PutIfAbsent),
then a `RawEnvelope(ingress_kind="backfill", raw_s3_key, content_hash)`
pointer is published to `ingestion.raw` — the SAME envelope shape the
webhook/gateway/pubsub shadow path publishes (see
`services/ingest/ingestion/shadow_write.py`). The normalizer consumes the
pointer, fetches the blob, and dispatches it through the handler
registry exactly as for live traffic.

Ordering extends N1 to "S3-write → publish → flush → advance":
  1. For each record: write the content-addressed blob to S3
     (PutIfAbsent) and build the RawEnvelope KafkaMessage. This
     happens BEFORE step 2 — the cursor never advances until the
     blobs are durable AND the broker has acked the pointers.
  2/3. advance_cursor_atomic_with_kafka_publish (unchanged N1).

Content-addressing makes the S3 write idempotent under Kafka-retry:
if the flush fails and the next tick re-fetches the same page, the
re-write is a no-op PutIfAbsent (same content_hash → same key) and
the re-published pointer is deduped by the idempotent producer +
the observation UNIQUE index. The N1 primitive's contract is
UNCHANGED — it still receives opaque `KafkaMessage` bytes and owns
the publish→flush→advance barrier. S3 failures become durable
recoverable retries; they never advance the cursor or discard the
shard's remaining history.

If the N1 primitive's contract is wrong for this use, that is a
substrate finding (per the M6.2a prompt's discipline: "If the
primitive needs amendment, STOP and surface as a substrate finding").
M6.2a is the verification round for whether M6.0's primitive holds
under production-shaped use.

============================================================
CURSOR HOME (LOAD-BEARING INVARIANT per A15)
============================================================
**The N1 home — `workflow_states.state_data["cursor"]` — IS THE
SOURCE OF TRUTH for cursors. The legacy `onboarding_shards.cursor_token`
column (M1-shipped 0045) stays NULL under M6.2a and is operator-
visible diagnostic only; production code MUST NOT read from it.**

This invariant is per A15 + the M6.2a Phase 1 acceptance Decision 3.
The reason: M6.2's N1 primitive postdates M1's cursor_token column;
both are present in the schema; one must be authoritative. The N1
home is the one updated atomically with Kafka publish — it is the
load-bearing surface. The cursor_token is the legacy column whose
LLD §1.2 semantics predate the M6.0 substrate.

M6.3-M6.6 fetchers reading this file should treat the cursor as
"read from workflow_states.state_data; never from
onboarding_shards.cursor_token." If a future per-source fetcher
needs the legacy column populated for ops visibility, it MAY mirror
the cursor on each advance — but the mirror is the diagnostic, not
the source of truth.

============================================================
TRANSACTIONAL DISCIPLINE — DIFFERENT FROM M6.1 SERVICES
============================================================
**The fetch loop is NOT one transaction.** This is deliberate and
different from M6.1's per-signal-per-transaction discipline.

  - **Signal claim transaction** (signal claim + mark shard
    'in_progress' + bootstrap workflow_states row) commits as one
    unit at the START of fetch.
  - **The fetch loop itself** runs OUTSIDE the claim transaction.
    Per-page atomicity is owned by the N1 primitive
    (`advance_cursor_atomic_with_kafka_publish` opens its own
    connection internally). The loop can run for minutes across
    many cursor advances.
  - **Completion transaction** (mark shard 'done'/'failed' + emit
    `shard_fetch_completed`) commits at the END.

This is the FETCH-LOOP-VS-SINGLE-TRANSACTION pattern. The shape is
necessary because: (a) a single transaction can't span multi-minute
external API calls without locking issues, (b) the N1 primitive
needs to own its own connection to enforce broker-ack ordering,
(c) per-page atomicity is sufficient — the LOOP's overall progress
survives crashes via the durable `onboarding_shards.state` +
`workflow_states.state_data["cursor"]` surfaces.

Future engineers MUST NOT "fix" this to one transaction. Same shape
precedent as M6.0 Phase 2's FeelsOnboardedMonitor surfacing the
N1-vs-CLAIM-VIA-UPDATE distinction; M6.2a Phase 2 surfaces the
FETCH-LOOP-VS-SINGLE-TRANSACTION distinction.

============================================================
TWO CLAIM MECHANISMS COEXIST (load-bearing for M6.3-M6.6 readers)
============================================================
ShardFetch's tick() does TWO things, each with its own claim
mechanism. Both are CLAIM-VIA-UPDATE at the per-shard level;
concurrent replicas are safe under either.

  (a) **Signal-driven claim** (`_process_signal_wave`, with
      `_process_one_signal` as the one-item compatibility entry point).
      Used when SourceOnboarding emits `shard_fetch_requested` for a NEW
      shard. The mechanism:
        1. `claim_signals(conn, ...)` — SKIP LOCKED on the inbox.
        2. `_claim_shard_for_fetch(conn, shard_id)` — UPDATE
           onboarding_shards SET state='in_progress' WHERE id=$1
           AND state='pending'; atomically set owner/expiry and increment
           `lease_version`. Returns the winning lease generation.
        3. `_bootstrap_workflow_state(conn, lease)` — owner/version-fenced
           INSERT of the N1 home row.
      All three commit atomically as the signal-claim transaction.

  (b) **Orphan-scan claim** (`_scan_and_resume_orphans`). Used to
      recover shards whose previous owner crashed mid-fetch. The
      mechanism:
        1. `_load_orphan_shards(pool, lease_timeout, limit)` —
           LEFT JOIN onboarding_shards ⨝ workflow_states; find
           rows where state='in_progress', retry is due, and both the
           explicit shard lease and workflow heartbeat are stale.
        2. For each: `_refresh_shard_lease(conn, shard_id)` —
           atomically acquire only an expired row, set owner/expiry, and
           increment `lease_version`.
        3. Run the fetch loop (which calls `load_state` to read
           the persisted cursor; if no row, the fetch loop
           defensively bootstraps).

The two mechanisms prevent concurrent commits:
  - In (a), state='pending' guard prevents claiming a shard
    already 'in_progress' (which would be served by mechanism (b)
    on a different replica).
  - A background heartbeat refreshes the explicit lease throughout slow
    provider calls, bounded client waits, S3 writes, and Kafka flushes.
  - Every cursor, retry, done, failed, and bootstrap mutation matches both
    `lease_owner` and `lease_version`; cursor commits row-lock the shard
    generation against a concurrent reclaim.
  - A stale worker may duplicate an idempotent S3/Kafka publish if handoff
    happens after upstream I/O, but it cannot advance the cursor or terminate
    the shard.

The lease timeout (`lease_timeout_seconds`, default 30s) is the
tunable knob. Tighter means faster crash recovery and more heartbeat
traffic; looser means a longer worst-case handoff. Provider latency does
not itself force a larger lease because the heartbeat runs independently.

============================================================
RESTART RESUMPTION (where the two mechanisms compose)
============================================================
On SIGTERM/SIGKILL mid-fetch, the durable surfaces are:
  - `onboarding_shards.state = 'in_progress'`.
  - `onboarding_shards.(lease_owner, lease_version, lease_expires_at)`.
  - `workflow_states.state_data["cursor"]` — most-recent N1 advance.
  - `workflow_states.last_advanced_at` — cursor-progress timestamp.

A restart's first tick(): mechanism (a) sees an empty inbox (the
signal was consumed at first claim); mechanism (b) sees the
in-progress shard with stale last_advanced_at and resumes.

============================================================
SIGNAL ADDRESSING (per A13)
============================================================
Inbox: `(kind="shard_fetch", id="shard_fetch")`. Consumes
`shard_fetch_requested` (from M6.2a's SourceOnboarding). Emits
`shard_fetch_completed` to `(source_onboarding, source_onboarding)`
(M6.2a's SourceOnboarding inbox). Idempotency key on both sides:
`str(shard_id)`.

============================================================
PATTERN-ALIGNMENT MAPPING
============================================================
  Rule 1 (orchestration separated from side effects):
    `tick()` and `_run_fetch_loop()` are orchestration; the
    module-level `_load_*` / `_mark_*` / `_write_record_and_build_message`
    functions own DB/Kafka/S3 I/O. The class methods pass the pool /
    connection / producer / s3_client through; no `await self._pool.X(...)`
    or `await self._kafka_producer.X(...)` in class bodies.

  Rule 2 (state in Postgres, not memory):
    A lease-fenced bootstrap creates the state row; the N1 primitive publishes
    and flushes every page before its final state update, which ShardFetch
    replaces with an owner/version-fenced executor.

  Rule 3 (retry in named functions):
    The fetch loop never sleeps for a durable cooldown. `RetryLater` and
    recoverable infrastructure failures flow through
    `_schedule_retry_for_context`, which persists the deadline and releases
    ownership. Provider clients own bounded per-call retry policy.

  Rule 4 (signals via Postgres polling):
    The service is a consumer (`shard_fetch_requested`) and a
    producer (`shard_fetch_completed`). All via the substrate.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state in this file. Fetcher callables are
    resolved lazily from immutable SourceDefinition bindings.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator
from uuid import UUID

import asyncpg
import orjson

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
    ZeroProgressError,
    validate_fetch_progress,
)
from services.ingest.ingestion.kafka.topics import topic_for
from services.ingest.ingestion.installations import (
    InstallationIdentityError,
    require_installation_row_id,
)
from services.ingest.ingestion.progress.events import ProgressEvent, ShardFetched
from services.ingest.ingestion.progress.publisher import publish_progress_events
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope
from services.ingest.ingestion.raw_tier.s3 import (
    S3Client,
    build_raw_s3_key,
    compute_content_hash,
)
from services.ingest.source_contract.runtime import (
    resolve_fetcher,
    resolve_installation_loader,
)
from services.ingest.ingestion.workflows.runtime import LongRunningService
from services.ingest.ingestion.workflows.signals import (
    claim_signals,
    emit_signal,
    signal_count,
)
from services.ingest.ingestion.workflows.state import (
    CursorAdvanceFlushFailure,
    CursorAdvancePublishFailure,
    KafkaMessage,
    WorkflowState,
    advance_cursor_atomic_with_kafka_publish,
    load_state,
    persist_state,
)


log = logging.getLogger(__name__)


WORKFLOW_KIND = "shard_fetch"
WORKFLOW_ID_INBOX = "shard_fetch"  # per A13: workflow_id = inbox
WORKFLOW_ID_DEFAULT = "default"  # diagnostic instance name

# Signal kinds.
SIGNAL_KIND_REQUESTED = "shard_fetch_requested"   # consumed from SourceOnboarding
SIGNAL_KIND_COMPLETED = "shard_fetch_completed"   # emitted to SourceOnboarding

# Downstream inbox (per M6.2a's SourceOnboarding).
SOURCE_ONBOARDING_INBOX_KIND = "source_onboarding"
SOURCE_ONBOARDING_INBOX_ID = "source_onboarding"

# Kafka topic for fetched records (LLD §4).
RAW_TOPIC = "ingestion.raw"

# Raw-tier (S3) defaults — mirror services/ingest/ingestion/shadow_write.py so
# the backfill producer and the webhook shadow path land bodies under
# the same key scheme + bucket (A27.1).
DEFAULT_S3_BUCKET = "fyralis-raw"
DEFAULT_INGESTION_ENV = "dev"

DEFAULT_TICK_INTERVAL_SECONDS = 5.0
# 0 means auto: count currently pending shard_fetch_requested signals and let
# that count set the tick budget. This keeps Discord's "one shard per channel"
# behavior from being capped by a static worker batch.
DEFAULT_MAX_SIGNALS_PER_TICK = 0
DEFAULT_AUTO_CONCURRENCY_LIMIT = 32
DEFAULT_LEASE_TIMEOUT_SECONDS = 30.0
# Sub-second values are useful in tests for making an unowned orphan eligible
# immediately, but they are too short to remain live across one PostgreSQL
# round trip under load. Detection may still use the configured window; an
# acquired ownership lease always receives at least this much TTL.
MIN_EFFECTIVE_LEASE_TIMEOUT_SECONDS = 1.0
DEFAULT_FLUSH_TIMEOUT_SECONDS = 5.0

# How long the diagnostic instance string is allowed to be on
# workflow_states.workflow_id. Per the substrate model, this is the
# instance's audit name (separate from WORKFLOW_ID_INBOX which is the
# routing partition key).
DEFAULT_DIAGNOSTIC_INSTANCE = "default"


# ---------------------------------------------------------------------
# SQL.
# ---------------------------------------------------------------------
_LOAD_SHARD_SQL = """
SELECT id, onboarding_run_id, tenant_id, source, shard_kind,
       shard_identifier, installation_row_id, state, started_at, next_attempt_at,
       attempt_count, retry_reason, lease_owner, lease_version,
       lease_expires_at
  FROM onboarding_shards
 WHERE id = $1
"""

_MARK_SHARD_IN_PROGRESS_SQL = """
UPDATE onboarding_shards
   SET state = 'in_progress',
       started_at = COALESCE(started_at, now()),
       attempt_count = attempt_count + 1,
       retry_reason = NULL,
       lease_owner = $2,
       lease_version = lease_version + 1,
       lease_expires_at = now() + ($3 * interval '1 second')
 WHERE id = $1
   AND state = 'pending'
   AND next_attempt_at <= now()
RETURNING lease_version
"""

# CLAIM-VIA-UPDATE for orphan re-acquire. Only succeeds if the shard
# is in_progress AND no recent N1 advance (heartbeat). The next-tick
# scan finds shards whose N1 home's last_advanced_at is older than
# the lease threshold; this UPDATE re-stamps started_at to extend
# the lease (using started_at as the lease timestamp anchor on the
# shard row — workflow_states.last_advanced_at is the more granular
# heartbeat but is on a different table).
_REFRESH_SHARD_LEASE_SQL = """
UPDATE onboarding_shards
   SET started_at = now(),
       attempt_count = attempt_count + 1,
       retry_reason = NULL,
       lease_owner = $2,
       lease_version = lease_version + 1,
       lease_expires_at = now() + ($3 * interval '1 second')
 WHERE id = $1
   AND state = 'in_progress'
   AND next_attempt_at <= now()
   AND (lease_expires_at IS NULL OR lease_expires_at <= now())
RETURNING lease_version
"""

_MARK_SHARD_DONE_SQL = """
UPDATE onboarding_shards
   SET state = 'done',
       completed_at = now(),
       retry_reason = NULL,
       lease_owner = NULL,
       lease_expires_at = NULL
 WHERE id = $1
   AND state = 'in_progress'
   AND lease_owner = $2
   AND lease_version = $3
   AND lease_expires_at > now()
RETURNING id
"""

_MARK_SHARD_FAILED_SQL = """
UPDATE onboarding_shards
   SET state = 'failed',
       completed_at = now(),
       last_error = $2,
       retry_reason = NULL,
       lease_owner = NULL,
       lease_expires_at = NULL
 WHERE id = $1
   AND state = 'in_progress'
   AND lease_owner = $3
   AND lease_version = $4
   AND lease_expires_at > now()
RETURNING id
"""

_HEARTBEAT_SHARD_LEASE_SQL = """
UPDATE onboarding_shards
   SET lease_expires_at = now() + ($4 * interval '1 second')
 WHERE id = $1
   AND state = 'in_progress'
   AND lease_owner = $2
   AND lease_version = $3
RETURNING lease_version
"""

_SCHEDULE_SHARD_RETRY_SQL = """
UPDATE onboarding_shards
   SET next_attempt_at = $2,
       retry_reason = $3,
       last_error = $4,
       lease_owner = NULL,
       lease_expires_at = NULL
 WHERE id = $1
   AND state = 'in_progress'
   AND lease_owner = $5
   AND lease_version = $6
   AND lease_expires_at > now()
RETURNING id
"""

# The workflow-state cursor update is fenced by the same shard lease as every
# other mutation.  The MATERIALIZED CTE takes a row lock, serializing this
# commit with an orphan re-claim.  Therefore either the old owner commits the
# cursor first, or the new owner increments lease_version first; both cannot
# commit against the same lease generation.
_ADVANCE_STATE_WITH_SHARD_LEASE_SQL = """
WITH held_lease AS MATERIALIZED (
    SELECT id
      FROM onboarding_shards
     WHERE id = $4
       AND state = 'in_progress'
       AND lease_owner = $5
       AND lease_version = $6
       AND lease_expires_at > now()
     FOR UPDATE
)
UPDATE workflow_states
   SET state_data = $1::jsonb,
       last_advanced_at = now()
 WHERE workflow_kind = $2
   AND workflow_id = $3
   AND EXISTS (SELECT 1 FROM held_lease)
RETURNING workflow_id
"""

_WORKFLOW_STATE_EXISTS_SQL = """
SELECT EXISTS (
    SELECT 1
      FROM workflow_states
     WHERE workflow_kind = $1 AND workflow_id = $2
)
"""

_INSERT_INITIAL_STATE_WITH_SHARD_LEASE_SQL = """
WITH held_lease AS MATERIALIZED (
    SELECT id
      FROM onboarding_shards
     WHERE id = $1
       AND state = 'in_progress'
       AND lease_owner = $2
       AND lease_version = $3
       AND lease_expires_at > now()
     FOR UPDATE
)
INSERT INTO workflow_states
    (workflow_kind, workflow_id, tenant_id, state_data,
     last_advanced_at, paused_at)
SELECT 'shard_fetch', $1::text, NULL,
       '{"cursor": null, "pages_fetched": 0}'::jsonb,
       now(), NULL
  FROM held_lease
ON CONFLICT (workflow_kind, workflow_id) DO NOTHING
RETURNING workflow_id
"""

# Find orphan in-progress shards: those whose workflow_states row is
# missing OR whose last_advanced_at is older than the lease timeout.
# The LEFT JOIN treats "workflow_states row absent" as "stale-since-
# beginning-of-time" so first-page bootstraps are caught too.
_LOAD_ORPHAN_SHARDS_SQL = """
WITH eligible AS MATERIALIZED (
    SELECT s.id, s.onboarding_run_id, s.tenant_id, s.source, s.shard_kind,
           s.shard_identifier, s.installation_row_id, s.state,
           s.next_attempt_at, s.started_at,
           COALESCE(
               s.installation_row_id::text,
               s.onboarding_run_id::text
           ) AS installation_key,
           tenant_history.last_served_at AS tenant_last_served_at,
           installation_history.last_served_at
               AS installation_last_served_at,
           row_number() OVER (
               PARTITION BY s.tenant_id,
                            COALESCE(
                                s.installation_row_id::text,
                                s.onboarding_run_id::text
                            )
               ORDER BY s.next_attempt_at, s.started_at NULLS FIRST, s.id
           ) AS installation_turn
      FROM onboarding_shards s
      LEFT JOIN workflow_states ws
        ON ws.workflow_kind = 'shard_fetch'
       AND ws.workflow_id = s.id::text
      LEFT JOIN LATERAL (
          SELECT max(prior.started_at) AS last_served_at
            FROM onboarding_shards prior
           WHERE prior.tenant_id = s.tenant_id
      ) tenant_history ON TRUE
      LEFT JOIN LATERAL (
          SELECT max(prior.started_at) AS last_served_at
            FROM onboarding_shards prior
           WHERE prior.tenant_id = s.tenant_id
             AND (
                  (
                    s.installation_row_id IS NOT NULL
                    AND prior.installation_row_id = s.installation_row_id
                  )
                  OR (
                    s.installation_row_id IS NULL
                    AND prior.installation_row_id IS NULL
                    AND prior.onboarding_run_id = s.onboarding_run_id
                  )
             )
      ) installation_history ON TRUE
     WHERE s.state = 'in_progress'
       AND s.next_attempt_at <= now()
       AND (s.lease_expires_at IS NULL OR s.lease_expires_at <= now())
       AND (ws.last_advanced_at IS NULL OR ws.last_advanced_at < $1)
),
ranked AS MATERIALIZED (
    SELECT eligible.*,
           row_number() OVER (
               PARTITION BY tenant_id
               ORDER BY installation_turn,
                        installation_last_served_at NULLS FIRST,
                        installation_key,
                        next_attempt_at,
                        started_at NULLS FIRST,
                        id
           ) AS tenant_turn
      FROM eligible
)
SELECT id, onboarding_run_id, tenant_id, source, shard_kind,
       shard_identifier, installation_row_id, state
  FROM ranked
 ORDER BY tenant_turn,
          tenant_last_served_at NULLS FIRST,
          tenant_id,
          installation_turn,
          installation_last_served_at NULLS FIRST,
          installation_key,
          next_attempt_at,
          started_at NULLS FIRST,
          id
 LIMIT $2
"""

_COUNT_ORPHAN_SHARDS_SQL = """
SELECT count(*)
  FROM onboarding_shards s
  LEFT JOIN workflow_states ws
    ON ws.workflow_kind = 'shard_fetch'
   AND ws.workflow_id   = s.id::text
 WHERE s.state = 'in_progress'
   AND s.next_attempt_at <= now()
   AND (s.lease_expires_at IS NULL OR s.lease_expires_at <= now())
   AND (ws.last_advanced_at IS NULL OR ws.last_advanced_at < $1)
"""

# ---------------------------------------------------------------------
# Config.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class ShardFetchConfig:
    """Configuration knobs. Test injection + env-driven production."""

    tick_interval_seconds: float = DEFAULT_TICK_INTERVAL_SECONDS
    max_signals_per_tick: int = DEFAULT_MAX_SIGNALS_PER_TICK
    lease_timeout_seconds: float = DEFAULT_LEASE_TIMEOUT_SECONDS
    flush_timeout_seconds: float = DEFAULT_FLUSH_TIMEOUT_SECONDS
    instance_name: str = DEFAULT_DIAGNOSTIC_INSTANCE
    # Raw-tier env prefix for S3 keys (A27.1). Mirrors INGESTION_ENV.
    ingestion_env: str = DEFAULT_INGESTION_ENV
    # Independent shards can drain concurrently; each shard still preserves
    # its internal page order and N1 cursor barrier. 0 means auto: derive the
    # width from the backlog, bounded by auto_concurrency_limit.
    max_concurrent_shards: int = 0
    # Safety ceiling for auto mode. This bounds task, HTTP, and DB pressure for
    # organization-wide installs with thousands of (repo, event-type) shards.
    # 0 explicitly restores unbounded auto fan-out.
    auto_concurrency_limit: int = DEFAULT_AUTO_CONCURRENCY_LIMIT
    # Per-page raw-tier S3 write fan-out. All writes must finish before the
    # page's Kafka flush/cursor advance barrier runs.
    s3_write_concurrency: int = 1
    # Transitional safety boundary for legacy fetchers that still encode a
    # provider cooldown as an empty, unchanged, nonterminal page. The workflow
    # converts that forbidden shape into durable RetryLater instead of spinning.
    zero_progress_retry_seconds: float = 60.0


class ShardLeaseLost(RuntimeError):
    """The caller no longer owns the shard lease generation."""


@dataclass(frozen=True, slots=True)
class _ShardLease:
    shard_id: UUID
    owner: str
    version: int


def _effective_lease_timeout_seconds(configured_seconds: float) -> float:
    if configured_seconds <= 0:
        raise ValueError("lease_timeout_seconds must be > 0")
    return max(MIN_EFFECTIVE_LEASE_TIMEOUT_SECONDS, configured_seconds)


async def _advance_state_under_lease(
    pool: asyncpg.Pool,
    lease: _ShardLease,
    *,
    state_data_json: str,
    workflow_kind: str,
    workflow_id: str,
) -> asyncpg.Record | None:
    return await pool.fetchrow(
        _ADVANCE_STATE_WITH_SHARD_LEASE_SQL,
        state_data_json,
        workflow_kind,
        workflow_id,
        lease.shard_id,
        lease.owner,
        lease.version,
    )


async def _workflow_state_exists(
    pool: asyncpg.Pool,
    *,
    workflow_kind: str,
    workflow_id: str,
) -> bool:
    return bool(
        await pool.fetchval(
            _WORKFLOW_STATE_EXISTS_SQL,
            workflow_kind,
            workflow_id,
        ),
    )


@dataclass(frozen=True, slots=True)
class _LeaseFencedStateExecutor:
    """Duck-typed pool used by the N1 primitive for its final state UPDATE.

    Publishing and flushing remain owned by
    ``advance_cursor_atomic_with_kafka_publish``.  Its final ``fetchrow`` is
    replaced with an UPDATE that row-locks and verifies the shard lease before
    changing the cursor.  This preserves the existing N1 API while adding a
    generation fence local to ShardFetch.
    """

    pool: asyncpg.Pool
    lease: _ShardLease

    async def fetchrow(
        self,
        _sql: str,
        state_data_json: str,
        workflow_kind: str,
        workflow_id: str,
    ) -> asyncpg.Record | None:
        row = await _advance_state_under_lease(
            self.pool,
            self.lease,
            state_data_json=state_data_json,
            workflow_kind=workflow_kind,
            workflow_id=workflow_id,
        )
        if row is not None:
            return row

        # Let the substrate preserve its precise missing-state error. If the
        # state exists, the only failed predicate is the owner/version/expiry
        # fence, so surface lease loss directly.
        state_exists = await _workflow_state_exists(
            self.pool,
            workflow_kind=workflow_kind,
            workflow_id=workflow_id,
        )
        if state_exists:
            raise ShardLeaseLost(
                f"lease lost for shard {self.lease.shard_id} "
                f"(owner={self.lease.owner!r}, "
                f"version={self.lease.version})",
            )
        return None


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_shard(
    executor: asyncpg.Pool | asyncpg.Connection, shard_id: UUID,
) -> asyncpg.Record | None:
    return await executor.fetchrow(_LOAD_SHARD_SQL, shard_id)


async def _claim_shard_for_fetch(
    conn: asyncpg.Connection,
    shard_id: UUID,
    *,
    lease_owner: str = DEFAULT_DIAGNOSTIC_INSTANCE,
    lease_timeout_seconds: float = DEFAULT_LEASE_TIMEOUT_SECONDS,
) -> _ShardLease | None:
    """CLAIM-VIA-UPDATE: mark shard 'in_progress' if it's currently
    'pending'. Returns True iff this caller won the claim.
    """
    version = await conn.fetchval(
        _MARK_SHARD_IN_PROGRESS_SQL,
        shard_id,
        lease_owner,
        lease_timeout_seconds,
    )
    if version is None:
        return None
    return _ShardLease(
        shard_id=shard_id,
        owner=lease_owner,
        version=int(version),
    )


async def _refresh_shard_lease(
    conn: asyncpg.Connection,
    shard_id: UUID,
    *,
    lease_owner: str = DEFAULT_DIAGNOSTIC_INSTANCE,
    lease_timeout_seconds: float = DEFAULT_LEASE_TIMEOUT_SECONDS,
) -> _ShardLease | None:
    """Extend the lease on an orphan in-progress shard. Returns True
    iff this caller now holds the lease (the UPDATE matched a row
    still in 'in_progress' state)."""
    version = await conn.fetchval(
        _REFRESH_SHARD_LEASE_SQL,
        shard_id,
        lease_owner,
        lease_timeout_seconds,
    )
    if version is None:
        return None
    return _ShardLease(
        shard_id=shard_id,
        owner=lease_owner,
        version=int(version),
    )


async def _heartbeat_shard_lease(
    executor: asyncpg.Pool | asyncpg.Connection,
    shard_id: UUID,
    *,
    lease_owner: str,
    lease_version: int,
    lease_timeout_seconds: float,
) -> bool:
    """Extend only the lease generation still owned by this worker.

    Expiry makes a generation eligible for takeover; it does not by itself
    supersede the generation.  Renewal and orphan takeover both update the
    same row, so PostgreSQL serializes the race: renewal wins and makes the
    lease live again, or takeover increments ``lease_version`` and this
    fenced update matches nothing.  Allowing the current generation to renew
    after a short scheduler/DB delay avoids making very small lease windows
    impossible to heartbeat without weakening the owner/version fence.
    """
    row = await executor.fetchval(
        _HEARTBEAT_SHARD_LEASE_SQL,
        shard_id,
        lease_owner,
        lease_version,
        lease_timeout_seconds,
    )
    return row is not None


async def _schedule_shard_retry(
    executor: asyncpg.Pool | asyncpg.Connection,
    shard_id: UUID,
    *,
    not_before: dt.datetime,
    reason: str,
    detail: str,
    lease: _ShardLease,
) -> bool:
    """Persist a retry deadline and relinquish the shard lease."""
    if not_before.tzinfo is None or not_before.utcoffset() is None:
        raise ValueError("not_before must be timezone-aware")
    retry_at = not_before.astimezone(dt.timezone.utc)
    row = await executor.fetchval(
        _SCHEDULE_SHARD_RETRY_SQL,
        shard_id,
        retry_at,
        reason,
        detail[:1000],
        lease.owner,
        lease.version,
    )
    return row is not None


async def _mark_shard_done(
    executor: asyncpg.Pool | asyncpg.Connection,
    shard_id: UUID,
    *,
    lease: _ShardLease,
) -> bool:
    row = await executor.fetchval(
        _MARK_SHARD_DONE_SQL,
        shard_id,
        lease.owner,
        lease.version,
    )
    return row is not None


async def _mark_shard_failed(
    executor: asyncpg.Pool | asyncpg.Connection,
    shard_id: UUID,
    last_error: str,
    *,
    lease: _ShardLease,
) -> bool:
    row = await executor.fetchval(
        _MARK_SHARD_FAILED_SQL,
        shard_id,
        last_error,
        lease.owner,
        lease.version,
    )
    return row is not None


async def _run_shard_lease_heartbeat(
    pool: asyncpg.Pool,
    lease: _ShardLease,
    *,
    lease_timeout_seconds: float,
    stop: asyncio.Event,
    lost: asyncio.Event,
) -> None:
    """Refresh one lease until stopped; fail closed on any refresh error."""
    interval_seconds = max(0.001, lease_timeout_seconds / 3.0)
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            return
        except TimeoutError:
            pass

        try:
            held = await _heartbeat_shard_lease(
                pool,
                lease.shard_id,
                lease_owner=lease.owner,
                lease_version=lease.version,
                lease_timeout_seconds=lease_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - ownership must fail closed
            log.warning(
                "shard_fetch.lease_heartbeat_failed",
                extra={
                    "shard_id": str(lease.shard_id),
                    "lease_owner": lease.owner,
                    "lease_version": lease.version,
                    "error": f"{type(exc).__name__}: {exc}"[:200],
                },
            )
            lost.set()
            return

        if not held:
            log.warning(
                "shard_fetch.lease_heartbeat_lost",
                extra={
                    "shard_id": str(lease.shard_id),
                    "lease_owner": lease.owner,
                    "lease_version": lease.version,
                },
            )
            lost.set()
            return


@asynccontextmanager
async def _maintain_shard_lease(
    pool: asyncpg.Pool,
    lease: _ShardLease,
    *,
    lease_timeout_seconds: float,
) -> AsyncIterator[asyncio.Event]:
    """Keep a claimed lease alive for the whole provider-side operation."""
    if lease_timeout_seconds <= 0:
        raise ValueError("lease_timeout_seconds must be > 0")

    # Verify and extend before starting any external work. An expired
    # generation can renew only while it remains the current owner/version;
    # an orphan takeover increments the version and makes this fail closed.
    held = await _heartbeat_shard_lease(
        pool,
        lease.shard_id,
        lease_owner=lease.owner,
        lease_version=lease.version,
        lease_timeout_seconds=lease_timeout_seconds,
    )
    if not held:
        raise ShardLeaseLost(
            f"lease expired before fetch start for shard {lease.shard_id}",
        )

    stop = asyncio.Event()
    lost = asyncio.Event()
    task = asyncio.create_task(
        _run_shard_lease_heartbeat(
            pool,
            lease,
            lease_timeout_seconds=lease_timeout_seconds,
            stop=stop,
            lost=lost,
        ),
        name=f"shard-fetch-lease-{lease.shard_id}",
    )
    try:
        yield lost
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _raise_if_lease_lost(
    lost: asyncio.Event,
    lease: _ShardLease,
) -> None:
    if lost.is_set():
        raise ShardLeaseLost(
            f"lease heartbeat lost for shard {lease.shard_id} "
            f"(owner={lease.owner!r}, version={lease.version})",
        )


async def _load_orphan_shards(
    pool: asyncpg.Pool, *, lease_timeout_seconds: float, limit: int,
) -> list[asyncpg.Record]:
    """Find in-progress shards whose N1 heartbeat is stale."""
    cutoff = (
        dt.datetime.now(tz=dt.timezone.utc)
        - dt.timedelta(seconds=lease_timeout_seconds)
    )
    return await pool.fetch(_LOAD_ORPHAN_SHARDS_SQL, cutoff, limit)


async def _count_orphan_shards(
    pool: asyncpg.Pool,
    *,
    lease_timeout_seconds: float,
) -> int:
    cutoff = (
        dt.datetime.now(tz=dt.timezone.utc)
        - dt.timedelta(seconds=lease_timeout_seconds)
    )
    value = await pool.fetchval(_COUNT_ORPHAN_SHARDS_SQL, cutoff)
    return int(value or 0)


def parse_auto_parallelism(value: str | None, *, default: str = "auto") -> int:
    """Parse worker parallelism/batch env.

    Returns 0 for auto/dynamic/all/unbounded, which the service resolves from
    the current shard-fetch backlog at runtime.
    """
    raw = (value if value is not None else default).strip().lower()
    if raw in ("", "auto", "dynamic", "all", "unbounded"):
        return 0
    parsed = int(raw)
    if parsed < 0:
        raise ValueError("parallelism must be >= 0 or 'auto'")
    return parsed


async def _load_install(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source: str,
    installation_row_id: UUID,
) -> asyncpg.Record | None:
    """Load only the exact enabled installation bound to this shard."""
    loader = resolve_installation_loader(source)
    return await loader(
        pool,
        tenant_id=tenant_id,
        installation_id=installation_row_id,
    )


async def _write_record_and_build_message(
    s3_client: S3Client,
    *,
    tenant_id: UUID,
    source: str,
    shard_id: UUID,
    installation_row_id: UUID,
    cursor: dict[str, Any] | None,
    record: dict[str, Any],
    env: str,
    now: dt.datetime | None = None,
) -> KafkaMessage:
    """Backfill producer (A27.1): write one fetched record's blob to
    S3, then build the `RawEnvelope` pointer KafkaMessage.

    The S3 blob wraps three things:
      - `record`: the handler-conformant body (A27.3) — exactly what a
        webhook for the same event would deliver, so the normalizer's
        handler derives the SAME external_id.
      - `shard_context`: `{shard_id, cursor}` — backfill provenance for
        operators / replay; not read by the handler.
      - `webhook_metadata`: the webhook-equivalent headers the handler
        needs (e.g. `{"X-GitHub-Event": "issues"}`). The fetcher emits
        these under a reserved `webhook_metadata` key on its record;
        this function LIFTS that key out so `record` is the bare body.

    The S3 write (content-addressed PutIfAbsent) happens HERE, before
    the caller's `advance_cursor_atomic_with_kafka_publish`. See the
    module docstring's S3-WRITE-BEFORE-PUBLISH section + A27.1. Raises
    a durable `RetryLater` on S3 failure.

    Partition key = tenant_id bytes (LLD §5.2 partition affinity).
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)

    record_body = dict(record)
    webhook_metadata = record_body.pop("webhook_metadata", {})
    blob = {
        "record": record_body,
        "shard_context": {
            "shard_id": str(shard_id),
            "installation_row_id": str(installation_row_id),
            "cursor": cursor,
        },
        "webhook_metadata": webhook_metadata,
    }
    blob_bytes = orjson.dumps(blob)
    content_hash = compute_content_hash(blob_bytes)
    s3_key = build_raw_s3_key(
        env=env,
        source=source,
        tenant_id=tenant_id,
        ymd=now.date(),
        content_hash=content_hash,
    )

    # S3 write BEFORE the N1 publish (A27.1). PutIfAbsent is idempotent
    # under Kafka-retry because the key encodes the content hash.
    await s3_client.put_if_absent(s3_key, blob_bytes)

    envelope = RawEnvelope(
        source=source,  # type: ignore[arg-type]  # shard.source ∈ SourceLiteral
        tenant_id=tenant_id,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=now,
        ingress_kind="backfill",
        ingress_metadata={
            "installation_row_id": str(installation_row_id),
        },
    )
    return KafkaMessage(
        # Per-source raw topic so backfill traffic for one source cannot
        # head-of-line block another's lane (source-isolation.md).
        topic=topic_for("raw", source),
        value=orjson.dumps(envelope.model_dump(mode="json")),
        key=str(tenant_id).encode("utf-8"),
    )


@dataclass
class _FetchLoopContext:
    shard_id: UUID
    tenant_id: UUID
    source: str
    shard_identifier: dict[str, Any]
    installation_row_id: UUID
    lease: _ShardLease
    loop_started_at: dt.datetime
    records_fetched: int = 0

    @classmethod
    def from_shard(
        cls,
        shard: asyncpg.Record,
        *,
        lease: _ShardLease,
    ) -> "_FetchLoopContext":
        ident_raw = shard["shard_identifier"]
        shard_identifier = (
            orjson.loads(ident_raw) if isinstance(ident_raw, (str, bytes))
            else dict(ident_raw)
        )
        try:
            installation_row_id = require_installation_row_id(
                shard["installation_row_id"],
            )
        except (KeyError, InstallationIdentityError) as exc:
            raise InstallationIdentityError(
                "shard is missing its exact installation_row_id",
            ) from exc
        try:
            descriptor_installation_id = require_installation_row_id(
                shard_identifier.get("installation_row_id"),
            )
        except InstallationIdentityError as exc:
            raise InstallationIdentityError(
                "shard descriptor is missing a valid installation_row_id",
            ) from exc
        if descriptor_installation_id != installation_row_id:
            raise InstallationIdentityError(
                "shard column and descriptor reference different "
                "installation rows",
            )
        if shard["id"] != lease.shard_id:
            raise ValueError(
                f"lease shard {lease.shard_id} does not match "
                f"loaded shard {shard['id']}",
            )
        return cls(
            shard_id=shard["id"],
            tenant_id=shard["tenant_id"],
            source=shard["source"],
            shard_identifier=shard_identifier,
            installation_row_id=installation_row_id,
            lease=lease,
            loop_started_at=dt.datetime.now(tz=dt.timezone.utc),
        )

    def fetched_in_seconds(self) -> float:
        return (
            dt.datetime.now(tz=dt.timezone.utc) - self.loop_started_at
        ).total_seconds()


async def _persist_initial_workflow_state(
    conn: asyncpg.Connection,
    lease: _ShardLease,
) -> bool:
    row = await conn.fetchval(
        _INSERT_INITIAL_STATE_WITH_SHARD_LEASE_SQL,
        lease.shard_id,
        lease.owner,
        lease.version,
    )
    if row is not None:
        return True
    # ON CONFLICT means an existing cursor home is also a successful
    # bootstrap, provided the lease itself still matches.
    lease_held = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
              FROM onboarding_shards
             WHERE id = $1
               AND state = 'in_progress'
               AND lease_owner = $2
               AND lease_version = $3
               AND lease_expires_at > now()
        )
        """,
        lease.shard_id,
        lease.owner,
        lease.version,
    )
    return bool(lease_held)


def _log_fetch_install_unavailable(ctx: _FetchLoopContext) -> None:
    log.warning(
        "shard_fetch.install_unavailable_park",
        extra={
            "shard_id": str(ctx.shard_id),
            "source": ctx.source,
            "tenant_id": str(ctx.tenant_id),
        },
    )


async def _ensure_fetch_loop_state(
    pool: asyncpg.Pool, ctx: _FetchLoopContext,
) -> None:
    # Ensure the N1 home exists before the first advance. Two paths reach this
    # point: signal-driven start bootstraps in the claim transaction;
    # orphan-scan resume may see a shard whose previous owner crashed before
    # first advance. Bootstrap defensively rather than rely on path (1).
    initial_state = await load_state(pool, WORKFLOW_KIND, str(ctx.shard_id))
    if initial_state is None:
        async with pool.acquire() as conn:
            async with conn.transaction():
                inserted = await _persist_initial_workflow_state(
                    conn,
                    ctx.lease,
                )
                if not inserted:
                    raise ShardLeaseLost(
                        f"lease lost while bootstrapping shard {ctx.shard_id}",
                    )
        return
    # Resume: carry forward the count from prior passes.
    ctx.records_fetched = int(initial_state.state_data.get("records_fetched", 0))


async def _load_fetch_cursor(
    pool: asyncpg.Pool, ctx: _FetchLoopContext,
) -> tuple[WorkflowState | None, dict[str, Any] | None]:
    # Re-read N1 cursor each iteration. Robust against cross-replica handoffs
    # where another replica may have advanced the cursor.
    current_state = await load_state(pool, WORKFLOW_KIND, str(ctx.shard_id))
    cursor = current_state.state_data.get("cursor") if current_state else None
    return current_state, cursor


async def _fetch_page(
    ctx: _FetchLoopContext,
    *,
    install: asyncpg.Record,
    cursor: dict[str, Any] | None,
) -> Any:
    # Provider clients own quota acquisition, retry-after propagation, and
    # per-actual-call retry policy. ShardFetch must not guess one fixed budget
    # per logical page or sleep while holding workflow work.
    fetcher = resolve_fetcher(ctx.source)
    return await fetcher(install, ctx.shard_identifier, cursor)


async def _write_fetch_page_messages(
    s3_client: S3Client | None,
    config: ShardFetchConfig,
    ctx: _FetchLoopContext,
    *,
    cursor: dict[str, Any] | None,
    records: list[dict[str, Any]],
) -> list[KafkaMessage]:
    if records and s3_client is None:
        raise RuntimeError(
            "shard_fetch backfill producer requires an "
            "S3Client (A27.1) but none was wired; set "
            "S3_ENDPOINT_URL / S3_RAW_BUCKET and pass "
            "s3_client=… to ShardFetch."
        )
    try:
        write_sem = asyncio.Semaphore(max(1, config.s3_write_concurrency))

        async def _write_one(rec: dict[str, Any]) -> KafkaMessage:
            async with write_sem:
                return await _write_record_and_build_message(
                    s3_client,
                    tenant_id=ctx.tenant_id,
                    source=ctx.source,
                    shard_id=ctx.shard_id,
                    installation_row_id=ctx.installation_row_id,
                    cursor=cursor,
                    record=rec,
                    env=config.ingestion_env,
                )

        return await asyncio.gather(*(_write_one(rec) for rec in records))
    except Exception as exc:  # noqa: BLE001
        # Transient raw-tier (S3) write failure — missing bucket, 5xx, network.
        # This is infra, not a poison shard: leave the shard in_progress so the
        # orphan-scan retries it. Cursor not advanced.
        log.warning(
            "shard_fetch.s3_write_failure_exit_loop",
            extra={
                "shard_id": str(ctx.shard_id),
                "source": ctx.source,
                "error": f"{type(exc).__name__}: {exc}"[:200],
            },
        )
        raise RetryLater.after(
            request_context=RequestContext(
                source=ctx.source,
                operation="write_raw_page",
                tenant_id=str(ctx.tenant_id),
            ),
            delay_seconds=config.zero_progress_retry_seconds,
            reason=RetryReason.TRANSIENT,
            cause_code=type(exc).__name__,
            message=f"raw-tier write failed: {type(exc).__name__}: {exc}",
        ) from exc


async def _advance_fetch_cursor(
    pool: asyncpg.Pool,
    kafka_producer: Any,
    config: ShardFetchConfig,
    ctx: _FetchLoopContext,
    *,
    current_state: WorkflowState | None,
    result: Any,
    messages: list[KafkaMessage],
) -> bool:
    try:
        await advance_cursor_atomic_with_kafka_publish(
            _LeaseFencedStateExecutor(pool=pool, lease=ctx.lease),
            kafka_producer,
            workflow_kind=WORKFLOW_KIND,
            workflow_id=str(ctx.shard_id),
            new_state_data={
                "cursor": result.next_cursor,
                "pages_fetched": (
                    (current_state.state_data.get("pages_fetched", 0)
                     if current_state else 0) + 1
                ),
                # Cumulative raw-record count, persisted so a resumed orphan
                # reports the whole shard's count in `shard.fetched`.
                "records_fetched": ctx.records_fetched,
                "end_of_data": result.end_of_data,
            },
            kafka_messages=messages,
            flush_timeout_seconds=config.flush_timeout_seconds,
        )
        return True
    except (CursorAdvanceFlushFailure, CursorAdvancePublishFailure) as exc:
        # Flush timeout OR produce-enqueue failure (broker down, unprovisioned
        # topic, queue full). Both are transient infra.
        log.warning(
            "shard_fetch.publish_failure_exit_loop",
            extra={
                "shard_id": str(ctx.shard_id),
                "source": ctx.source,
                "failure_kind": type(exc).__name__,
            },
        )
        raise RetryLater.after(
            request_context=RequestContext(
                source=ctx.source,
                operation="publish_raw_page",
                tenant_id=str(ctx.tenant_id),
            ),
            delay_seconds=config.zero_progress_retry_seconds,
            reason=RetryReason.TRANSIENT,
            cause_code=type(exc).__name__,
            message=f"raw-page publish failed: {type(exc).__name__}: {exc}",
        ) from exc


async def _run_fetch_pages(
    pool: asyncpg.Pool,
    kafka_producer: Any,
    s3_client: S3Client | None,
    config: ShardFetchConfig,
    ctx: _FetchLoopContext,
    *,
    install: asyncpg.Record,
    lease_lost: asyncio.Event,
) -> bool:
    while True:
        _raise_if_lease_lost(lease_lost, ctx.lease)
        current_state, cursor = await _load_fetch_cursor(pool, ctx)
        result = await _fetch_page(
            ctx,
            install=install,
            cursor=cursor,
        )
        _raise_if_lease_lost(lease_lost, ctx.lease)
        try:
            validate_fetch_progress(
                cursor_before=cursor,
                cursor_after=result.next_cursor,
                records_emitted=len(result.records),
                end_of_data=result.end_of_data,
            )
        except ZeroProgressError as exc:
            try:
                installation_id = str(install["id"])
            except (KeyError, TypeError):
                installation_id = None
            raise RetryLater.after(
                request_context=RequestContext(
                    source=ctx.source,
                    operation="fetch_page",
                    tenant_id=str(ctx.tenant_id),
                    installation_id=installation_id,
                ),
                delay_seconds=config.zero_progress_retry_seconds,
                reason=RetryReason.TRANSIENT,
                cause_code=exc.code,
                message=(
                    "legacy fetcher returned an empty unchanged nonterminal "
                    "page; scheduling instead of hot-looping"
                ),
            ) from exc
        ctx.records_fetched += len(result.records)
        messages = await _write_fetch_page_messages(
            s3_client, config, ctx, cursor=cursor, records=result.records,
        )
        _raise_if_lease_lost(lease_lost, ctx.lease)
        advanced = await _advance_fetch_cursor(
            pool,
            kafka_producer,
            config,
            ctx,
            current_state=current_state,
            result=result,
            messages=messages,
        )
        if not advanced:
            return False
        _raise_if_lease_lost(lease_lost, ctx.lease)
        if result.end_of_data:
            return True


def _log_recoverable_fetch_error(
    ctx: _FetchLoopContext, exc: Exception,
) -> None:
    log.warning(
        "shard_fetch.recoverable_fetch_error_park",
        extra={
            "shard_id": str(ctx.shard_id),
            "source": ctx.source,
            "error": f"{type(exc).__name__}: {exc}"[:200],
        },
    )


async def _schedule_retry_for_context(
    pool: asyncpg.Pool,
    ctx: _FetchLoopContext,
    *,
    not_before: dt.datetime,
    reason: str,
    detail: str,
) -> None:
    scheduled = await _schedule_shard_retry(
        pool,
        ctx.shard_id,
        not_before=not_before,
        reason=reason,
        detail=detail,
        lease=ctx.lease,
    )
    if not scheduled:
        raise ShardLeaseLost(
            f"lease lost while scheduling retry for shard {ctx.shard_id}",
        )


# ---------------------------------------------------------------------
# Service.
# ---------------------------------------------------------------------
class ShardFetch(LongRunningService):
    """LongRunningService that drains shard_fetch_requested signals
    AND scans for orphan in-progress shards.

    Two responsibilities per tick:
      (a) Signal drain — new shards triggered by SourceOnboarding.
      (b) Orphan scan — in-progress shards whose lease has expired
          (from prior crash, SIGTERM mid-flight, or cross-replica
          handoff).
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        kafka_producer: Any,  # IdempotentProducer
        *,
        config: ShardFetchConfig | None = None,
        s3_client: S3Client | None = None,
    ) -> None:
        self._pool = pool
        self._kafka_producer = kafka_producer
        self._config = config or ShardFetchConfig()
        # Raw-tier client for the backfill producer (A27.1). Required
        # whenever a fetcher returns records to publish; the fetch loop
        # raises a clear error if it's missing so a misconfigured
        # subprocess fails loudly rather than silently dropping records.
        self._s3_client = s3_client

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: interleave due orphan recovery with fresh signals.

        Signal handlers run the FULL fetch loop for their shard. Work
        is concurrent only across independent shards; each shard keeps
        its own cursor loop serial so the N1 publish-before-cursor
        barrier remains intact. Concurrent replicas drain signals via
        SKIP LOCKED.
        """
        signals_processed = 0
        orphans_resumed = 0
        remaining = await self._signal_tick_budget()
        width = self._claim_wave_width(remaining)
        while remaining > 0:
            wave_size = min(width, remaining)
            # A due retry/orphan gets one fair wave before each fresh backfill
            # wave. A large new-install backlog therefore cannot postpone
            # already-due recovery work until the entire snapshot drains.
            orphans_resumed += await self._scan_and_resume_orphans(
                limit=wave_size,
            )
            processed_in_wave = await self._process_signal_wave(wave_size)
            signals_processed += processed_in_wave
            remaining -= wave_size
            if processed_in_wave < wave_size:
                break

        # Auto mode historically drains the complete orphan snapshot. Preserve
        # that throughput after the fair interleaving pass; explicitly bounded
        # mode keeps its configured per-tick ceiling.
        if self._config.max_signals_per_tick <= 0:
            orphans_resumed += await self._scan_and_resume_orphans()

        await self._persist_scan_state(
            signals_processed=signals_processed,
            orphans_resumed=orphans_resumed,
        )

    async def _signal_tick_budget(self) -> int:
        configured = self._config.max_signals_per_tick
        if configured > 0:
            return configured
        pending = await signal_count(
            self._pool,
            workflow_kind=WORKFLOW_KIND,
            workflow_id=WORKFLOW_ID_INBOX,
        )
        return max(1, pending)

    def _claim_wave_width(self, remaining: int) -> int:
        configured = self._config.max_concurrent_shards
        if configured > 0:
            return configured
        auto_limit = self._config.auto_concurrency_limit
        if auto_limit > 0:
            return max(1, min(remaining, auto_limit))
        return max(1, remaining)

    async def _process_one_signal(self) -> bool:
        """Claim one shard_fetch_requested signal + run its fetch loop.

        The claim transaction commits the signal-consume + shard-
        bootstrap together. The fetch loop runs OUTSIDE the claim
        transaction (see module docstring's transactional discipline
        section).
        """
        return bool(await self._process_signal_wave(1))

    async def _process_signal_wave(self, batch_size: int) -> int:
        """Claim/bootstrap one fair batch, then run provider work concurrently.

        Fair selection is intentionally one database operation per wave. Running
        ``batch_size`` separate fair-ranking queries made durable setup compete
        with itself on a small pool and prevented the configured provider
        concurrency from ever being reached. The transaction below preserves
        the load-bearing signal-claim + shard-lease + workflow-state atomicity;
        only the provider calls leave that transaction.
        """

        if batch_size <= 0:
            return 0
        prepared: list[tuple[asyncpg.Record, _ShardLease]] = []
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                signals = await claim_signals(
                    conn,
                    workflow_kind=WORKFLOW_KIND,
                    workflow_id=WORKFLOW_ID_INBOX,
                    consumed_by=self._config.instance_name,
                    batch_size=batch_size,
                    fairness="shard",
                )
                if not signals:
                    return 0
                for sig in signals:
                    work = await self._prepare_claimed_signal(conn, sig)
                    if work is not None:
                        prepared.append(work)

        # Fetch loops run OUTSIDE the claim transaction — see the module
        # docstring's transactional discipline section.
        results = await asyncio.gather(
            *(
                self._run_fetch_loop(shard, lease)
                for shard, lease in prepared
            ),
            return_exceptions=True,
        )
        first_error = next(
            (result for result in results if isinstance(result, Exception)),
            None,
        )
        if first_error is not None:
            raise first_error
        return len(signals)

    async def _prepare_claimed_signal(
        self,
        conn: asyncpg.Connection,
        sig: Any,
    ) -> tuple[asyncpg.Record, _ShardLease] | None:
        """Stage one already-claimed signal inside its wave transaction."""

        shard_id = UUID(sig.signal_data["shard_id"])
        shard = await _load_shard(conn, shard_id)
        if shard is None:
            log.warning(
                "shard_fetch.shard_missing",
                extra={
                    "shard_id": str(shard_id),
                    "signal_id": str(sig.id),
                },
            )
            return None

        if shard["state"] in ("done", "failed"):
            # Idempotent re-emit — the requester is replaying but the shard
            # already terminated. Keep it atomic with consuming the signal.
            await self._emit_shard_completed(
                conn,
                shard=shard,
                status=shard["state"],
                failure_reason=None,
            )
            return None

        if shard["state"] != "pending":
            # state == 'in_progress' — the orphan path owns it. Consume the
            # duplicate request, but never run provider work without a new
            # owner/version generation.
            return None

        lease_timeout_seconds = _effective_lease_timeout_seconds(
            self._config.lease_timeout_seconds,
        )
        lease = await _claim_shard_for_fetch(
            conn,
            shard_id,
            lease_owner=self._config.instance_name,
            lease_timeout_seconds=lease_timeout_seconds,
        )
        if lease is None:
            # Another replica won between the fair signal selection and shard
            # claim. Its lease generation owns the fetch.
            return None
        await self._bootstrap_workflow_state(conn, lease)
        return shard, lease

    async def _scan_and_resume_orphans(
        self,
        *,
        limit: int | None = None,
    ) -> int:
        """Find in-progress shards with stale N1 heartbeat; resume
        each fetch loop after extending the lease.

        Returns count of orphans resumed. Concurrent replicas use
        CLAIM-VIA-UPDATE for the lease extension so only one wins.
        """
        claim_limit = limit
        if claim_limit is not None and claim_limit <= 0:
            return 0
        if claim_limit is None:
            claim_limit = self._config.max_signals_per_tick
        if claim_limit <= 0:
            claim_limit = max(
                1,
                await _count_orphan_shards(
                    self._pool,
                    lease_timeout_seconds=self._config.lease_timeout_seconds,
                ),
            )
        orphans = await _load_orphan_shards(
            self._pool,
            lease_timeout_seconds=self._config.lease_timeout_seconds,
            limit=claim_limit,
        )
        sem = asyncio.Semaphore(self._claim_wave_width(len(orphans)))

        async def _resume_one(orphan: asyncpg.Record) -> int:
            shard_id = orphan["id"]
            async with sem:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        lease_timeout_seconds = (
                            _effective_lease_timeout_seconds(
                                self._config.lease_timeout_seconds,
                            )
                        )
                        lease = await _refresh_shard_lease(
                            conn,
                            shard_id,
                            lease_owner=self._config.instance_name,
                            lease_timeout_seconds=lease_timeout_seconds,
                        )
                if lease is None:
                    return 0
                await self._run_fetch_loop(orphan, lease)
                return 1

        results = await asyncio.gather(
            *(_resume_one(orphan) for orphan in orphans),
            return_exceptions=True,
        )
        first_error = next((r for r in results if isinstance(r, Exception)), None)
        if first_error is not None:
            raise first_error
        return sum(int(r) for r in results)

    async def _bootstrap_workflow_state(
        self,
        conn: asyncpg.Connection,
        lease: _ShardLease,
    ) -> None:
        """Initialize the N1 home for this shard.

        Required precondition for `advance_cursor_atomic_with_kafka_publish`
        (which raises `CursorAdvanceMissingState` if the row doesn't
        exist; the substrate refuses to silently create state).
        """
        bootstrapped = await _persist_initial_workflow_state(conn, lease)
        if not bootstrapped:
            raise ShardLeaseLost(
                f"lease lost while bootstrapping shard {lease.shard_id}",
            )

    async def _run_fetch_loop(
        self,
        shard: asyncpg.Record,
        lease: _ShardLease,
    ) -> None:
        """Run one shard while a background task maintains its lease."""
        try:
            lease_timeout_seconds = _effective_lease_timeout_seconds(
                self._config.lease_timeout_seconds,
            )
            async with _maintain_shard_lease(
                self._pool,
                lease,
                lease_timeout_seconds=lease_timeout_seconds,
            ) as lease_lost:
                try:
                    ctx = _FetchLoopContext.from_shard(shard, lease=lease)
                except InstallationIdentityError as exc:
                    await self._terminate_shard(
                        lease=lease,
                        state="failed",
                        failure_reason=str(exc),
                    )
                    return
                await self._run_fetch_loop_owned(ctx, lease_lost)
        except ShardLeaseLost as exc:
            # Losing a lease is a normal handoff. The current worker must stop
            # without retry/terminal/cursor writes; the new generation owns all
            # subsequent mutations.
            log.warning(
                "shard_fetch.lease_lost_stop",
                extra={
                    "shard_id": str(lease.shard_id),
                    "lease_owner": lease.owner,
                    "lease_version": lease.version,
                    "error": str(exc),
                },
            )

    async def _run_fetch_loop_owned(
        self,
        ctx: _FetchLoopContext,
        lease_lost: asyncio.Event,
    ) -> None:
        """Fetch until end-of-data, durable retry, or lease loss.

        Per the module docstring: this runs OUTSIDE the claim
        transaction. Each iteration:
          1. Load current cursor from workflow_states (N1 home).
          2. Load install (from provider_installations or
             gmail_installations).
          3. Resolve the source contract's fetcher and call it with
             `(install, shard_identifier, cursor)` → FetchResult.
          4. Build Kafka messages for result.records.
          5. Call advance_cursor_atomic_with_kafka_publish — N1.
          6. If end_of_data: exit loop.

        Exit conditions:
          - end_of_data → mark shard 'done' + emit completion.
          - Transient infra fault → persist a due time and release ownership.
            Covers Kafka enqueue/flush and raw-tier S3 failures. These must NOT
            terminal-fail a shard; doing so silently drops history.
          - NotImplementedError (fetcher stub) → mark shard 'failed'
            + emit completion with failure_reason.
          - Other exception → mark 'failed' + emit with failure_reason.
        """
        try:
            _raise_if_lease_lost(lease_lost, ctx.lease)
            install = await _load_install(
                self._pool,
                tenant_id=ctx.tenant_id,
                source=ctx.source,
                installation_row_id=ctx.installation_row_id,
            )
            _raise_if_lease_lost(lease_lost, ctx.lease)
            if install is None:
                # Install disabled mid-flight — suspended/revoked via the
                # lifecycle webhook, or an A14 race. This is RECOVERABLE:
                # park the shard (leave it in_progress) so the orphan-scan
                # resumes it once the install is re-enabled (unsuspend),
                # rather than terminal-failing it (which needs a manual
                # requeue). A genuinely-deleted install leaves the shard
                # parked until an operator cleans up the onboarding run —
                # the retry is one cheap query per lease interval.
                _log_fetch_install_unavailable(ctx)
                await _schedule_retry_for_context(
                    self._pool,
                    ctx,
                    not_before=(
                        dt.datetime.now(tz=dt.timezone.utc)
                        + dt.timedelta(
                            seconds=self._config.zero_progress_retry_seconds
                        )
                    ),
                    reason=RetryReason.TRANSIENT.value,
                    detail="active installation unavailable",
                )
                return  # stay in_progress; orphan-scan retries on re-enable

            await _ensure_fetch_loop_state(self._pool, ctx)
            if not await _run_fetch_pages(
                self._pool,
                self._kafka_producer,
                self._s3_client,
                self._config,
                ctx,
                install=install,
                lease_lost=lease_lost,
            ):
                return

        except ShardLeaseLost:
            raise

        except RetryLater as exc:
            _raise_if_lease_lost(lease_lost, ctx.lease)
            await _schedule_retry_for_context(
                self._pool,
                ctx,
                not_before=exc.not_before,
                reason=exc.reason.value,
                detail=str(exc),
            )
            log.info(
                "shard_fetch.retry_scheduled",
                extra={
                    "shard_id": str(ctx.shard_id),
                    "source": ctx.source,
                    "not_before": exc.not_before.isoformat(),
                    "retry_reason": exc.reason.value,
                    "blocked_scope": exc.blocked_scope,
                },
            )
            return

        except NotImplementedError as exc:
            _raise_if_lease_lost(lease_lost, ctx.lease)
            await self._terminate_shard(
                lease=ctx.lease,
                state="failed",
                failure_reason=str(exc),
            )
            return

        except Exception as exc:  # noqa: BLE001 — terminal recovery boundary
            _raise_if_lease_lost(lease_lost, ctx.lease)
            if getattr(exc, "recoverable", False):
                # Transient upstream fault (rate limit, 5xx). Park the shard —
                # leave it in_progress so the orphan-scan retries — rather than
                # terminal-failing on a recoverable error (which would need a
                # manual requeue). Same posture as the S3/Kafka transient
                # handling above.
                _log_recoverable_fetch_error(ctx, exc)
                await _schedule_retry_for_context(
                    self._pool,
                    ctx,
                    not_before=(
                        dt.datetime.now(tz=dt.timezone.utc)
                        + dt.timedelta(
                            seconds=self._config.zero_progress_retry_seconds
                        )
                    ),
                    reason=RetryReason.TRANSIENT.value,
                    detail=f"{type(exc).__name__}: {exc}",
                )
                return  # stay in_progress; orphan-scan retries
            log.exception(
                "shard_fetch.unexpected_exception",
                extra={"shard_id": str(ctx.shard_id)},
            )
            await self._terminate_shard(
                lease=ctx.lease,
                state="failed",
                failure_reason=f"{type(exc).__name__}: {exc}",
            )
            return

        # Clean end-of-data exit. Pass the fetch metrics so the terminal
        # transition can emit the `shard.fetched` progress event.
        _raise_if_lease_lost(lease_lost, ctx.lease)
        await self._terminate_shard(
            lease=ctx.lease,
            state="done",
            failure_reason=None,
            observation_count=ctx.records_fetched,
            fetched_in_seconds=ctx.fetched_in_seconds(),
        )

    async def _terminate_shard(
        self,
        *,
        lease: _ShardLease,
        state: str,  # 'done' or 'failed'
        failure_reason: str | None,
        observation_count: int | None = None,
        fetched_in_seconds: float | None = None,
    ) -> bool:
        """Mark shard terminal + emit shard_fetch_completed.

        One transaction: shard state update + emit, atomic. If the
        emit collides with an earlier one (idempotency_key=shard_id
        already in workflow_signals), emit_signal returns
        was_new=False and the transaction commits successfully —
        the SourceOnboarding consumer sees one completion regardless
        of replicas.

        On the `state='done'` exit (and only then), also publishes the
        user-facing `shard.fetched` progress event (LLD §6) AFTER the
        transaction commits — the shard `done` transition is
        claim-via-UPDATE guarded, so post-commit publish gives the
        at-least-once + Bridge-dedup contract. The failure paths pass no
        metrics and emit no progress event (no `shard.failed` in the
        contract)."""
        events: list[ProgressEvent] = []
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if state == "done":
                    changed = await _mark_shard_done(
                        conn,
                        lease.shard_id,
                        lease=lease,
                    )
                else:
                    changed = await _mark_shard_failed(
                        conn,
                        lease.shard_id,
                        failure_reason or "<unknown>",
                        lease=lease,
                    )
                if not changed:
                    log.warning(
                        "shard_fetch.stale_terminal_write_rejected",
                        extra={
                            "shard_id": str(lease.shard_id),
                            "lease_owner": lease.owner,
                            "lease_version": lease.version,
                            "requested_state": state,
                        },
                    )
                    return False
                # Re-load to get the shard's run/source for the signal.
                shard = await _load_shard(conn, lease.shard_id)
                if shard is None:
                    return False
                await self._emit_shard_completed(
                    conn, shard=shard, status=state,
                    failure_reason=failure_reason,
                )
                if state == "done" and observation_count is not None:
                    events.append(ShardFetched(
                        tenant_id=shard["tenant_id"],
                        source=shard["source"],
                        shard_id=shard["id"],
                        observation_count=observation_count,
                        fetched_in_seconds=fetched_in_seconds or 0.0,
                    ))
        await publish_progress_events(self._kafka_producer, events)
        return True

    async def _emit_shard_completed(
        self, conn: asyncpg.Connection, *,
        shard: asyncpg.Record,
        status: str,
        failure_reason: str | None,
    ) -> None:
        """Emit `shard_fetch_completed` to SourceOnboarding's inbox.

        Idempotency key: `str(shard.id)`. The SourceOnboarding
        consumer's handler is idempotent on shard_id (M6.2a Phase 1
        contract).
        """
        data: dict[str, Any] = {
            "shard_id": str(shard["id"]),
            "onboarding_run_id": str(shard["onboarding_run_id"]),
            "tenant_id": str(shard["tenant_id"]),
            "source": shard["source"],
            "status": status,
        }
        if failure_reason is not None:
            data["failure_reason"] = failure_reason
        await emit_signal(
            conn,
            workflow_kind=SOURCE_ONBOARDING_INBOX_KIND,
            workflow_id=SOURCE_ONBOARDING_INBOX_ID,
            signal_kind=SIGNAL_KIND_COMPLETED,
            idempotency_key=str(shard["id"]),
            signal_data=data,
        )

    async def _persist_scan_state(
        self, *, signals_processed: int, orphans_resumed: int,
    ) -> None:
        """Diagnostic state row for ops queries. Not load-bearing for
        correctness; the per-shard `workflow_states` row (keyed by
        shard_id) is the N1 home and IS load-bearing."""
        existing = await load_state(
            self._pool, WORKFLOW_KIND, self._config.instance_name,
        )
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=self._config.instance_name,
            tenant_id=None,
            state_data={
                "last_tick_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
                "last_signals_processed": signals_processed,
                "last_orphans_resumed": orphans_resumed,
                "lifetime_signals_processed": (
                    (existing.state_data.get("lifetime_signals_processed", 0)
                     if existing else 0)
                    + signals_processed
                ),
                "lifetime_orphans_resumed": (
                    (existing.state_data.get("lifetime_orphans_resumed", 0)
                     if existing else 0)
                    + orphans_resumed
                ),
            },
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
        await persist_state(self._pool, state)


# ---------------------------------------------------------------------
# CLI entrypoint — `python -m services.ingest.ingestion.workflows.shard_fetch`.
# ---------------------------------------------------------------------
# ENV:
#   DATABASE_URL              — Postgres DSN (required).
#   KAFKA_BOOTSTRAP_SERVERS   — Kafka bootstrap (default localhost:9092).
#   SHARD_FETCH_TICK_SEC      — tick interval (default 5.0).
#   SHARD_FETCH_BATCH         — max signals per tick (default auto).
#   SHARD_FETCH_LEASE_SEC     — orphan lease timeout (default 30.0).
#   SHARD_FETCH_FLUSH_SEC     — Kafka flush timeout (default 5.0).
#   SHARD_FETCH_CONCURRENCY   — concurrent independent shard loops
#                               (default auto). Auto scales with the current
#                               backlog up to SHARD_FETCH_AUTO_CONCURRENCY_MAX.
#   SHARD_FETCH_AUTO_CONCURRENCY_MAX — auto-mode safety ceiling (default 32).
#                               0 restores unbounded fan-out.
#   SHARD_FETCH_S3_WRITE_CONCURRENCY — per-page S3 PUT fan-out (default 1).
#   SHARD_FETCH_INSTANCE      — instance name for diagnostics.
#   WORKFLOWS_LOG_LEVEL       — log level (default INFO).
async def _run_service() -> None:
    import asyncio
    import os
    import signal as sig_module

    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.workflows.runtime import make_workflow_pool

    pool = await make_workflow_pool(os.environ["DATABASE_URL"])
    producer = IdempotentProducer(ProducerConfig(
        bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
        ),
        client_id="workflow-shard_fetch",
    ))
    await producer.start()

    # Raw-tier S3 client for the backfill producer (A27.1). S3_ENDPOINT_URL
    # is optional (None → real AWS); S3_RAW_BUCKET defaults to fyralis-raw,
    # matching the webhook shadow path.
    s3_client = S3Client(
        os.environ.get("S3_RAW_BUCKET", DEFAULT_S3_BUCKET),
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        region_name=os.environ.get("S3_REGION_NAME", "auto"),
    )
    await s3_client.connect()

    config = ShardFetchConfig(
        tick_interval_seconds=float(
            os.environ.get("SHARD_FETCH_TICK_SEC", "5.0"),
        ),
        max_signals_per_tick=parse_auto_parallelism(
            os.environ.get("SHARD_FETCH_BATCH"),
        ),
        lease_timeout_seconds=float(
            os.environ.get("SHARD_FETCH_LEASE_SEC", "30.0"),
        ),
        flush_timeout_seconds=float(
            os.environ.get("SHARD_FETCH_FLUSH_SEC", "5.0"),
        ),
        instance_name=os.environ.get(
            "SHARD_FETCH_INSTANCE", DEFAULT_DIAGNOSTIC_INSTANCE,
        ),
        ingestion_env=os.environ.get("INGESTION_ENV", DEFAULT_INGESTION_ENV),
        max_concurrent_shards=parse_auto_parallelism(
            os.environ.get("SHARD_FETCH_CONCURRENCY"),
        ),
        auto_concurrency_limit=parse_auto_parallelism(
            os.environ.get("SHARD_FETCH_AUTO_CONCURRENCY_MAX"),
            default=str(DEFAULT_AUTO_CONCURRENCY_LIMIT),
        ),
        s3_write_concurrency=int(
            os.environ.get("SHARD_FETCH_S3_WRITE_CONCURRENCY", "1"),
        ),
    )

    service = ShardFetch(
        pool,
        producer,
        config=config,
        s3_client=s3_client,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    # Liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT). This
    # worker has no in-process counter dict of its own; /metrics carries the
    # shared registry (kafka producer, db pool, oauth, per-source backfill
    # API counters recorded by the fetchers running in this process).
    from services.ingest.ingestion.observability import (
        Heartbeat,
        run_heartbeat_ticker,
        start_health_server,
    )

    heartbeat = Heartbeat()
    health = start_health_server(get_metrics=dict, heartbeat=heartbeat)
    ticker = asyncio.ensure_future(run_heartbeat_ticker(heartbeat, stop_event))

    log.info("workflow.shard_fetch.started", extra={
        "instance": config.instance_name,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.shard_fetch.shutting_down")
        ticker.cancel()
        await asyncio.gather(ticker, return_exceptions=True)
        if health is not None:
            health.shutdown()
        await producer.stop()
        await s3_client.close()
        await pool.close()
    log.info("workflow.shard_fetch.exited")


def main() -> None:
    import asyncio
    import os
    logging.basicConfig(
        level=os.environ.get("WORKFLOWS_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run_service())


if __name__ == "__main__":
    main()


__all__ = [
    "DEFAULT_AUTO_CONCURRENCY_LIMIT",
    "DEFAULT_FLUSH_TIMEOUT_SECONDS",
    "DEFAULT_LEASE_TIMEOUT_SECONDS",
    "DEFAULT_MAX_SIGNALS_PER_TICK",
    "DEFAULT_TICK_INTERVAL_SECONDS",
    "parse_auto_parallelism",
    "RAW_TOPIC",
    "SIGNAL_KIND_COMPLETED",
    "SIGNAL_KIND_REQUESTED",
    "SOURCE_ONBOARDING_INBOX_ID",
    "SOURCE_ONBOARDING_INBOX_KIND",
    "ShardFetch",
    "ShardFetchConfig",
    "WORKFLOW_ID_DEFAULT",
    "WORKFLOW_ID_INBOX",
    "WORKFLOW_KIND",
    "main",
]
