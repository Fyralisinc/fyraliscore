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
the publish→flush→advance barrier. S3 failures propagate as
exceptions and mark the shard 'failed' per A19.

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

  (a) **Signal-driven claim** (`_process_one_signal`). Used when
      SourceOnboarding emits `shard_fetch_requested` for a NEW
      shard. The mechanism:
        1. `claim_signals(conn, ...)` — SKIP LOCKED on the inbox.
        2. `_claim_shard_for_fetch(conn, shard_id)` — UPDATE
           onboarding_shards SET state='in_progress' WHERE id=$1
           AND state='pending'. Returns True iff this caller's
           UPDATE matched (won the race vs. another replica).
        3. `_bootstrap_workflow_state(conn, shard_id)` — INSERT
           the N1 home row.
      All three commit atomically as the signal-claim transaction.

  (b) **Orphan-scan claim** (`_scan_and_resume_orphans`). Used to
      recover shards whose previous owner crashed mid-fetch. The
      mechanism:
        1. `_load_orphan_shards(pool, lease_timeout, limit)` —
           LEFT JOIN onboarding_shards ⨝ workflow_states; find
           rows where state='in_progress' AND
           (workflow_states.last_advanced_at IS NULL OR
            < now() - lease_timeout).
        2. For each: `_refresh_shard_lease(conn, shard_id)` —
           UPDATE onboarding_shards SET started_at=now() WHERE
           id=$1 AND state='in_progress'. Returns True iff this
           caller's UPDATE matched.
        3. Run the fetch loop (which calls `load_state` to read
           the persisted cursor; if no row, the fetch loop
           defensively bootstraps).

The two mechanisms NEVER produce double-fetches:
  - In (a), state='pending' guard prevents claiming a shard
    already 'in_progress' (which would be served by mechanism (b)
    on a different replica).
  - In (b), the lease-timeout filter prevents claiming a shard
    whose owner is still actively advancing (their N1 advance
    updates last_advanced_at, refreshing the lease).
  - Concurrent replicas on either mechanism: SKIP LOCKED + the
    state='in_progress' / state='pending' guards ensure exactly
    one UPDATE matches per shard.

The lease timeout (`lease_timeout_seconds`, default 30s) is the
tunable knob. Tighter = faster orphan recovery, more risk of
double-claim under slow advances. Looser = safer for slow
fetchers (e.g., rate-limited per-source APIs), longer worst-case
recovery time. Tests use 0.01-0.3s; production at 30s; M6.3-M6.6
per-source fetchers may want longer if their natural fetch
latency approaches the timeout.

============================================================
RESTART RESUMPTION (where the two mechanisms compose)
============================================================
On SIGTERM/SIGKILL mid-fetch, the durable surfaces are:
  - `onboarding_shards.state = 'in_progress'`.
  - `workflow_states.state_data["cursor"]` — most-recent N1 advance.
  - `workflow_states.last_advanced_at` — N1 heartbeat.

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
    `state.persist_state` to bootstrap; the N1 primitive's
    `advance_cursor_atomic_with_kafka_publish` for every cursor
    advance. The shard's state column is the surviving anchor.

  Rule 3 (retry in named functions):
    None at this granularity. The fetch loop has no `try ... await
    asyncio.sleep ...` retry shape. Per-source retry (rate-limit
    backoff, 5xx) is the per-source fetcher's concern; M6.3-M6.6
    will wrap with named retry helpers from
    `services.ingest.ingestion.workflows.retry`.

  Rule 4 (signals via Postgres polling):
    The service is a consumer (`shard_fetch_requested`) and a
    producer (`shard_fetch_completed`). All via the substrate.

  Rule 5 (no cross-workflow shared state):
    No module-level mutable state in this file. `FETCHER_DISPATCH`
    in `services/ingest/ingestion/fetchers/__init__.py` is ALL_CAPS
    (constant-style) and outside the analyzer's `services/ingest/ingestion/
    workflows/*.py` scope.
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import orjson

from services.ingest.ingestion.fetchers import FETCHER_DISPATCH
from services.ingest.ingestion.kafka.topics import topic_for
from services.ingest.ingestion.progress.events import ProgressEvent, ShardFetched
from services.ingest.ingestion.progress.publisher import publish_progress_events
from services.ingest.ingestion.rate_limit import (
    FetchRateLimiter,
    RateLimitWaitExceeded,
)
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope
from services.ingest.ingestion.raw_tier.s3 import (
    S3Client,
    build_raw_s3_key,
    compute_content_hash,
)
from services.ingest.ingestion.workflows.runtime import LongRunningService
from services.ingest.ingestion.workflows.signals import (
    claim_signals,
    emit_signal,
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
DEFAULT_MAX_SIGNALS_PER_TICK = 10  # smaller batch — each runs a fetch loop
DEFAULT_LEASE_TIMEOUT_SECONDS = 30.0
DEFAULT_FLUSH_TIMEOUT_SECONDS = 5.0
# Bound on how long one rate-limited page fetch may wait for a token
# before exiting the loop (shard stays in_progress; orphan-scan resumes).
DEFAULT_RATE_LIMIT_MAX_WAIT_SECONDS = 30.0

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
       shard_identifier, state, started_at
  FROM onboarding_shards
 WHERE id = $1
"""

_MARK_SHARD_IN_PROGRESS_SQL = """
UPDATE onboarding_shards
   SET state = 'in_progress',
       started_at = COALESCE(started_at, now())
 WHERE id = $1 AND state = 'pending'
RETURNING id
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
   SET started_at = now()
 WHERE id = $1 AND state = 'in_progress'
RETURNING id
"""

_MARK_SHARD_DONE_SQL = """
UPDATE onboarding_shards
   SET state = 'done', completed_at = now()
 WHERE id = $1 AND state IN ('pending', 'in_progress')
"""

_MARK_SHARD_FAILED_SQL = """
UPDATE onboarding_shards
   SET state = 'failed', completed_at = now(), last_error = $2
 WHERE id = $1 AND state IN ('pending', 'in_progress')
"""

# Find orphan in-progress shards: those whose workflow_states row is
# missing OR whose last_advanced_at is older than the lease timeout.
# The LEFT JOIN treats "workflow_states row absent" as "stale-since-
# beginning-of-time" so first-page bootstraps are caught too.
_LOAD_ORPHAN_SHARDS_SQL = """
SELECT s.id, s.onboarding_run_id, s.tenant_id, s.source, s.shard_kind,
       s.shard_identifier, s.state
  FROM onboarding_shards s
  LEFT JOIN workflow_states ws
    ON ws.workflow_kind = 'shard_fetch'
   AND ws.workflow_id   = s.id::text
 WHERE s.state = 'in_progress'
   AND (ws.last_advanced_at IS NULL OR ws.last_advanced_at < $1)
 ORDER BY s.started_at NULLS FIRST
 LIMIT $2
"""

_LOAD_PROVIDER_INSTALL_SQL = """
SELECT id, tenant_id, provider, installation_id, secret_ref, enabled
  FROM provider_installations
 WHERE tenant_id = $1 AND provider = $2 AND enabled = TRUE
 LIMIT 1
"""

_LOAD_GMAIL_INSTALL_SQL = """
SELECT id, tenant_id, workspace_domain, service_account_email,
       scope, disabled_at
  FROM gmail_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# IN-15: Google Calendar is a DWD source like Gmail (not in
# provider_installations). The fetcher works one calendar at a time via
# shard_identifier, so the install row only needs scope + id + tenant_id;
# the impersonated owner_email comes from the shard.
_LOAD_GCAL_INSTALL_SQL = """
SELECT id, tenant_id, workspace_domain, service_account_email,
       scope, disabled_at
  FROM google_calendar_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# IN-16: Google Drive is a DWD source like Gmail/Calendar (not in
# provider_installations). The fetcher works one drive at a time via
# shard_identifier, so the install row only needs scope + id + tenant_id;
# the impersonated owner_email + drive_id come from the shard.
_LOAD_GDRIVE_INSTALL_SQL = """
SELECT id, tenant_id, workspace_domain, service_account_email,
       scope, disabled_at
  FROM google_drive_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# IN-17: Jira is a per-site install (not in provider_installations). The
# fetcher works one project at a time via shard_identifier; the install row
# carries base_url + account_email + secret_ref the JiraClient needs.
_LOAD_JIRA_INSTALL_SQL = """
SELECT id, tenant_id, base_url, account_email, secret_ref, cloud_id,
       disabled_at
  FROM jira_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# Finance: Mercury is a per-tenant API-token install (not in
# provider_installations). The fetcher works one account at a time via
# shard_identifier; the install row carries base_url + secret_ref the
# MercuryClient needs.
_LOAD_MERCURY_INSTALL_SQL = """
SELECT id, tenant_id, base_url, secret_ref, disabled_at
  FROM mercury_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# Finance: QuickBooks is a per-tenant OAuth install (not in
# provider_installations). The fetcher works one entity type at a time via
# shard_identifier; the install row carries realm_id + base_url + secret_ref
# (access token) + refresh_secret_ref the QuickBooksClient needs.
_LOAD_QUICKBOOKS_INSTALL_SQL = """
SELECT id, tenant_id, realm_id, base_url, secret_ref, refresh_secret_ref,
       disabled_at
  FROM quickbooks_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# Grafana is a per-tenant service-account install (not in
# provider_installations). There is ONE shard per install (annotation/alert
# state is org-wide), so the fetcher does not need a child row — the install
# row carries base_url + secret_ref the GrafanaClient needs and base_url for
# the `_instance_of` external_id namespace. Without this dedicated branch a
# grafana shard would fall through to the provider_installations lookup, find
# nothing, and park forever — its install never lands in that table.
_LOAD_GRAFANA_INSTALL_SQL = """
SELECT id, tenant_id, base_url, org_id, secret_ref, disabled_at
  FROM grafana_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
"""

# IN-TELEGRAM: Telegram is a per-tenant MTProto-session install (not in
# provider_installations). The fetcher works one dialog at a time via
# shard_identifier; the install row carries the api credentials + the persisted
# session refs the TelegramClient needs. Backfill uses the second authorization
# (backfill_session_secret_ref); without this dedicated branch a telegram shard
# would fall through to provider_installations, find nothing, and park forever.
_LOAD_TELEGRAM_INSTALL_SQL = """
SELECT id, tenant_id, account_label, api_id, api_hash_secret_ref,
       session_secret_ref, backfill_session_secret_ref, disabled_at
  FROM telegram_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 LIMIT 1
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
    # Bound on a single page fetch's rate-limit wait (LLD §13 FetchPage
    # gate). Past this, the loop exits transiently for orphan-scan retry.
    rate_limit_max_wait_seconds: float = DEFAULT_RATE_LIMIT_MAX_WAIT_SECONDS


# ---------------------------------------------------------------------
# Named side-effect functions (Rule 1).
# ---------------------------------------------------------------------
async def _load_shard(
    executor: asyncpg.Pool | asyncpg.Connection, shard_id: UUID,
) -> asyncpg.Record | None:
    return await executor.fetchrow(_LOAD_SHARD_SQL, shard_id)


async def _claim_shard_for_fetch(
    conn: asyncpg.Connection, shard_id: UUID,
) -> bool:
    """CLAIM-VIA-UPDATE: mark shard 'in_progress' if it's currently
    'pending'. Returns True iff this caller won the claim.
    """
    row = await conn.fetchval(_MARK_SHARD_IN_PROGRESS_SQL, shard_id)
    return row is not None


async def _refresh_shard_lease(
    conn: asyncpg.Connection, shard_id: UUID,
) -> bool:
    """Extend the lease on an orphan in-progress shard. Returns True
    iff this caller now holds the lease (the UPDATE matched a row
    still in 'in_progress' state)."""
    row = await conn.fetchval(_REFRESH_SHARD_LEASE_SQL, shard_id)
    return row is not None


async def _mark_shard_done(
    executor: asyncpg.Pool | asyncpg.Connection, shard_id: UUID,
) -> None:
    await executor.execute(_MARK_SHARD_DONE_SQL, shard_id)


async def _mark_shard_failed(
    executor: asyncpg.Pool | asyncpg.Connection,
    shard_id: UUID, last_error: str,
) -> None:
    await executor.execute(
        _MARK_SHARD_FAILED_SQL, shard_id, last_error,
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


async def _load_install(
    pool: asyncpg.Pool, *, tenant_id: UUID, source: str,
) -> asyncpg.Record | None:
    """Load the active install row for this (tenant, source)."""
    if source == "gmail":
        return await pool.fetchrow(_LOAD_GMAIL_INSTALL_SQL, tenant_id)
    if source == "google_calendar":
        return await pool.fetchrow(_LOAD_GCAL_INSTALL_SQL, tenant_id)
    if source == "google_drive":
        return await pool.fetchrow(_LOAD_GDRIVE_INSTALL_SQL, tenant_id)
    if source == "jira":
        return await pool.fetchrow(_LOAD_JIRA_INSTALL_SQL, tenant_id)
    if source == "mercury":
        return await pool.fetchrow(_LOAD_MERCURY_INSTALL_SQL, tenant_id)
    if source == "quickbooks":
        return await pool.fetchrow(_LOAD_QUICKBOOKS_INSTALL_SQL, tenant_id)
    if source == "grafana":
        return await pool.fetchrow(_LOAD_GRAFANA_INSTALL_SQL, tenant_id)
    if source == "telegram":
        return await pool.fetchrow(_LOAD_TELEGRAM_INSTALL_SQL, tenant_id)
    return await pool.fetchrow(_LOAD_PROVIDER_INSTALL_SQL, tenant_id, source)


async def _write_record_and_build_message(
    s3_client: S3Client,
    *,
    tenant_id: UUID,
    source: str,
    shard_id: UUID,
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
    on S3 failure; the fetch loop's A19 boundary marks the shard failed.

    Partition key = tenant_id bytes (LLD §5.2 partition affinity).
    """
    now = now or dt.datetime.now(tz=dt.timezone.utc)

    record_body = dict(record)
    webhook_metadata = record_body.pop("webhook_metadata", {})
    blob = {
        "record": record_body,
        "shard_context": {"shard_id": str(shard_id), "cursor": cursor},
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
    )
    return KafkaMessage(
        # Per-source raw topic so backfill traffic for one source cannot
        # head-of-line block another's lane (source-isolation.md).
        topic=topic_for("raw", source),
        value=orjson.dumps(envelope.model_dump(mode="json")),
        key=str(tenant_id).encode("utf-8"),
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
        rate_limiter: FetchRateLimiter | None = None,
    ) -> None:
        self._pool = pool
        self._kafka_producer = kafka_producer
        self._config = config or ShardFetchConfig()
        # Raw-tier client for the backfill producer (A27.1). Required
        # whenever a fetcher returns records to publish; the fetch loop
        # raises a clear error if it's missing so a misconfigured
        # subprocess fails loudly rather than silently dropping records.
        self._s3_client = s3_client
        # FetchPage rate-limit gate (LLD §13). Optional: when None (no
        # REDIS_URL / tests) the fetch loop runs unthrottled, preserving
        # prior behaviour. When set, one token is consumed per page fetch
        # from the (source, method) bucket before the upstream call.
        self._rate_limiter = rate_limiter

    @property
    def tick_interval_seconds(self) -> float:
        return self._config.tick_interval_seconds

    async def tick(self) -> None:
        """One tick: drain signals + scan for orphans.

        Each signal handler runs the FULL fetch loop for its shard
        synchronously (the loop is not gated by tick interval). One
        slow fetcher can therefore consume an entire tick — that is
        the intended back-pressure. Concurrent replicas drain
        signals via SKIP LOCKED.
        """
        signals_processed = 0
        for _ in range(self._config.max_signals_per_tick):
            processed = await self._process_one_signal()
            if not processed:
                break
            signals_processed += 1

        orphans_resumed = await self._scan_and_resume_orphans()

        await self._persist_scan_state(
            signals_processed=signals_processed,
            orphans_resumed=orphans_resumed,
        )

    async def _process_one_signal(self) -> bool:
        """Claim one shard_fetch_requested signal + run its fetch loop.

        The claim transaction commits the signal-consume + shard-
        bootstrap together. The fetch loop runs OUTSIDE the claim
        transaction (see module docstring's transactional discipline
        section).
        """
        shard: asyncpg.Record | None
        is_new_claim: bool
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                signals = await claim_signals(
                    conn,
                    workflow_kind=WORKFLOW_KIND,
                    workflow_id=WORKFLOW_ID_INBOX,
                    consumed_by=self._config.instance_name,
                    batch_size=1,
                )
                if not signals:
                    return False
                sig = signals[0]
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
                    return True

                if shard["state"] in ("done", "failed"):
                    # Idempotent re-emit — the requester is replaying
                    # but the shard already terminated. Re-emit the
                    # completion in the same transaction to keep the
                    # downstream consumer in sync.
                    await self._emit_shard_completed(
                        conn, shard=shard,
                        status=shard["state"],
                        failure_reason=None,
                    )
                    return True

                if shard["state"] == "pending":
                    is_new_claim = await _claim_shard_for_fetch(
                        conn, shard_id,
                    )
                    if not is_new_claim:
                        # Race: another replica claimed between our
                        # SELECT and UPDATE. That replica will run the
                        # loop; we return.
                        return True
                    await self._bootstrap_workflow_state(conn, shard_id)
                else:
                    # state == 'in_progress' — claim is being handled
                    # by the orphan scan path; we don't double-claim
                    # here. Just consume the signal.
                    pass

        # Fetch loop runs OUTSIDE the claim transaction — see module
        # docstring's transactional discipline section.
        await self._run_fetch_loop(shard)
        return True

    async def _scan_and_resume_orphans(self) -> int:
        """Find in-progress shards with stale N1 heartbeat; resume
        each fetch loop after extending the lease.

        Returns count of orphans resumed. Concurrent replicas use
        CLAIM-VIA-UPDATE for the lease extension so only one wins.
        """
        orphans = await _load_orphan_shards(
            self._pool,
            lease_timeout_seconds=self._config.lease_timeout_seconds,
            limit=self._config.max_signals_per_tick,
        )
        resumed = 0
        for orphan in orphans:
            shard_id = orphan["id"]
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    won = await _refresh_shard_lease(conn, shard_id)
            if not won:
                continue
            resumed += 1
            await self._run_fetch_loop(orphan)
        return resumed

    async def _bootstrap_workflow_state(
        self, conn: asyncpg.Connection, shard_id: UUID,
    ) -> None:
        """Initialize the N1 home for this shard.

        Required precondition for `advance_cursor_atomic_with_kafka_publish`
        (which raises `CursorAdvanceMissingState` if the row doesn't
        exist; the substrate refuses to silently create state).
        """
        state = WorkflowState(
            workflow_kind=WORKFLOW_KIND,
            workflow_id=str(shard_id),
            tenant_id=None,
            state_data={"cursor": None, "pages_fetched": 0},
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )
        await persist_state(conn, state)

    async def _run_fetch_loop(self, shard: asyncpg.Record) -> None:
        """Run the fetch loop for one shard until end-of-data or
        N1 flush failure.

        Per the module docstring: this runs OUTSIDE the claim
        transaction. Each iteration:
          1. Load current cursor from workflow_states (N1 home).
          2. Load install (from provider_installations or
             gmail_installations).
          3. Call FETCHER_DISPATCH[source](install, shard_identifier,
             cursor) → FetchResult.
          4. Build Kafka messages for result.records.
          5. Call advance_cursor_atomic_with_kafka_publish — N1.
          6. If end_of_data: exit loop.

        Exit conditions:
          - end_of_data → mark shard 'done' + emit completion.
          - Transient infra fault → exit silently; shard stays in_progress;
            next tick's orphan scan resumes. Covers: CursorAdvanceFlushFailure
            (flush timeout), CursorAdvancePublishFailure (produce-enqueue:
            broker down / unprovisioned topic / queue full), and raw-tier
            (S3) write failures. These must NOT terminal-fail a shard —
            doing so silently drops that shard's history with no auto-recovery.
          - NotImplementedError (fetcher stub) → mark shard 'failed'
            + emit completion with failure_reason.
          - Other exception → mark 'failed' + emit with failure_reason.
        """
        shard_id: UUID = shard["id"]
        tenant_id: UUID = shard["tenant_id"]
        source: str = shard["source"]
        # shard_identifier is JSONB; asyncpg returns it as a string or dict.
        ident_raw = shard["shard_identifier"]
        shard_identifier = (
            orjson.loads(ident_raw) if isinstance(ident_raw, (str, bytes))
            else dict(ident_raw)
        )

        # Wall-clock start for the `shard.fetched` progress event's
        # `fetched_in_seconds` (this fetch pass; a resumed orphan restarts
        # the clock). `records_fetched` is seeded from the N1 state so it
        # survives a cross-replica handoff and reports the cumulative
        # raw-record count for the shard, not just the resuming pass.
        loop_started_at = dt.datetime.now(tz=dt.timezone.utc)
        records_fetched = 0

        try:
            install = await _load_install(
                self._pool, tenant_id=tenant_id, source=source,
            )
            if install is None:
                # Install disabled mid-flight — suspended/revoked via the
                # lifecycle webhook, or an A14 race. This is RECOVERABLE:
                # park the shard (leave it in_progress) so the orphan-scan
                # resumes it once the install is re-enabled (unsuspend),
                # rather than terminal-failing it (which needs a manual
                # requeue). A genuinely-deleted install leaves the shard
                # parked until an operator cleans up the onboarding run —
                # the retry is one cheap query per lease interval.
                log.warning(
                    "shard_fetch.install_unavailable_park",
                    extra={
                        "shard_id": str(shard_id), "source": source,
                        "tenant_id": str(tenant_id),
                    },
                )
                return  # stay in_progress; orphan-scan retries on re-enable

            # Ensure the N1 home exists before the first advance.
            # Two paths reach this point: (1) signal-driven start
            # bootstraps in the claim transaction; (2) orphan-scan
            # resume of a shard whose previous owner crashed before
            # first advance — no workflow_states row exists yet.
            # Bootstrap defensively rather than rely on path (1).
            initial_state = await load_state(
                self._pool, WORKFLOW_KIND, str(shard_id),
            )
            if initial_state is None:
                async with self._pool.acquire() as conn:
                    async with conn.transaction():
                        await self._bootstrap_workflow_state(conn, shard_id)
            else:
                # Resume: carry forward the count from prior passes.
                records_fetched = int(
                    initial_state.state_data.get("records_fetched", 0)
                )

            while True:
                # Re-read N1 cursor each iteration. Robust against
                # cross-replica handoffs where another replica may
                # have advanced the cursor in the interim.
                current_state = await load_state(
                    self._pool, WORKFLOW_KIND, str(shard_id),
                )
                cursor = (
                    current_state.state_data.get("cursor")
                    if current_state else None
                )

                # FetchPage rate-limit gate (LLD §13): acquire one token
                # for the (source, method) bucket BEFORE the upstream page
                # call. Pass-through for sources with no published budget.
                # A bounded wait that's exceeded raises RateLimitWaitExceeded,
                # caught below as a transient loop exit.
                if self._rate_limiter is not None:
                    await self._rate_limiter.acquire(
                        source=source, tenant_id=tenant_id,
                    )

                fetcher = FETCHER_DISPATCH[source]
                result = await fetcher(install, shard_identifier, cursor)
                records_fetched += len(result.records)

                # A27.1 — write each record's blob to S3 + build the
                # RawEnvelope pointer BEFORE the N1 publish. S3 failures
                # are tagged distinctly from Kafka/cursor failures for
                # operator debugging (the broad except below records the
                # message), then propagate to mark the shard 'failed'.
                if result.records and self._s3_client is None:
                    raise RuntimeError(
                        "shard_fetch backfill producer requires an "
                        "S3Client (A27.1) but none was wired; set "
                        "S3_ENDPOINT_URL / S3_RAW_BUCKET and pass "
                        "s3_client=… to ShardFetch."
                    )
                try:
                    msgs = [
                        await _write_record_and_build_message(
                            self._s3_client,
                            tenant_id=tenant_id, source=source,
                            shard_id=shard_id, cursor=cursor, record=rec,
                            env=self._config.ingestion_env,
                        )
                        for rec in result.records
                    ]
                except Exception as exc:  # noqa: BLE001
                    # Transient raw-tier (S3) write failure — missing bucket,
                    # 5xx, network. This is infra, not a poison shard: leave
                    # the shard in_progress so the orphan-scan retries it,
                    # rather than terminal-failing (which is unrecoverable and
                    # silently drops this shard's history). Cursor not advanced.
                    log.warning(
                        "shard_fetch.s3_write_failure_exit_loop",
                        extra={
                            "shard_id": str(shard_id), "source": source,
                            "error": f"{type(exc).__name__}: {exc}"[:200],
                        },
                    )
                    return  # shard stays in_progress; orphan-scan retries

                try:
                    await advance_cursor_atomic_with_kafka_publish(
                        self._pool, self._kafka_producer,
                        workflow_kind=WORKFLOW_KIND,
                        workflow_id=str(shard_id),
                        new_state_data={
                            "cursor": result.next_cursor,
                            "pages_fetched": (
                                (current_state.state_data.get(
                                    "pages_fetched", 0,
                                ) if current_state else 0) + 1
                            ),
                            # Cumulative raw-record count, persisted so a
                            # resumed orphan reports the whole shard's count
                            # in its `shard.fetched` event.
                            "records_fetched": records_fetched,
                            "end_of_data": result.end_of_data,
                        },
                        kafka_messages=msgs,
                        flush_timeout_seconds=(
                            self._config.flush_timeout_seconds
                        ),
                    )
                except (CursorAdvanceFlushFailure, CursorAdvancePublishFailure) as exc:
                    # Flush timeout OR produce-enqueue failure (broker down,
                    # unprovisioned topic, queue full). Both are transient infra
                    # — leave the shard in_progress for the orphan-scan to retry
                    # rather than terminal-failing on a hiccup. (Previously a
                    # produce-enqueue KafkaException escaped to the terminal
                    # boundary below and silently lost the shard's history.)
                    log.warning(
                        "shard_fetch.publish_failure_exit_loop",
                        extra={
                            "shard_id": str(shard_id),
                            "source": source,
                            "failure_kind": type(exc).__name__,
                        },
                    )
                    return  # shard stays in_progress; orphan-scan retries

                if result.end_of_data:
                    break

        except RateLimitWaitExceeded as exc:
            # Transient: the (source, method) bucket stayed empty past the
            # bounded wait. Exit without terminating — the shard stays
            # in_progress and the orphan scan resumes it once the bucket
            # refills (or the operator lifts the pause). Same recovery
            # shape as a CursorAdvanceFlushFailure.
            log.warning(
                "shard_fetch.rate_limited_exit_loop",
                extra={
                    "shard_id": str(shard_id),
                    "source": source,
                    "bucket_key": exc.bucket_key,
                    "waited_seconds": exc.waited_seconds,
                },
            )
            return

        except NotImplementedError as exc:
            await self._terminate_shard(
                shard_id=shard_id, state="failed",
                failure_reason=str(exc),
            )
            return

        except Exception as exc:  # noqa: BLE001 — terminal recovery boundary
            if getattr(exc, "recoverable", False):
                # Transient upstream fault (rate limit, 5xx). Park the shard —
                # leave it in_progress so the orphan-scan retries — rather than
                # terminal-failing on a recoverable error (which would need a
                # manual requeue). Same posture as the S3/Kafka transient
                # handling above.
                log.warning(
                    "shard_fetch.recoverable_fetch_error_park",
                    extra={
                        "shard_id": str(shard_id), "source": source,
                        "error": f"{type(exc).__name__}: {exc}"[:200],
                    },
                )
                return  # stay in_progress; orphan-scan retries
            log.exception(
                "shard_fetch.unexpected_exception",
                extra={"shard_id": str(shard_id)},
            )
            await self._terminate_shard(
                shard_id=shard_id, state="failed",
                failure_reason=f"{type(exc).__name__}: {exc}",
            )
            return

        # Clean end-of-data exit. Pass the fetch metrics so the terminal
        # transition can emit the `shard.fetched` progress event.
        fetched_in_seconds = (
            dt.datetime.now(tz=dt.timezone.utc) - loop_started_at
        ).total_seconds()
        await self._terminate_shard(
            shard_id=shard_id, state="done", failure_reason=None,
            observation_count=records_fetched,
            fetched_in_seconds=fetched_in_seconds,
        )

    async def _terminate_shard(
        self, *,
        shard_id: UUID,
        state: str,  # 'done' or 'failed'
        failure_reason: str | None,
        observation_count: int | None = None,
        fetched_in_seconds: float | None = None,
    ) -> None:
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
                    await _mark_shard_done(conn, shard_id)
                else:
                    await _mark_shard_failed(
                        conn, shard_id, failure_reason or "<unknown>",
                    )
                # Re-load to get the shard's run/source for the signal.
                shard = await _load_shard(conn, shard_id)
                if shard is None:
                    return
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
#   SHARD_FETCH_BATCH         — max signals per tick (default 10).
#   SHARD_FETCH_LEASE_SEC     — orphan lease timeout (default 30.0).
#   SHARD_FETCH_FLUSH_SEC     — Kafka flush timeout (default 5.0).
#   SHARD_FETCH_INSTANCE      — instance name for diagnostics.
#   REDIS_URL                 — Redis for the FetchPage rate-limit gate
#                               (LLD §13). Unset → gate disabled.
#   SHARD_FETCH_RATE_LIMIT    — "0" disables the gate even with REDIS_URL.
#   SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC — per-page rate-limit wait bound
#                               (default 30.0).
#   WORKFLOWS_LOG_LEVEL       — log level (default INFO).
async def _run_service() -> None:
    import asyncio
    import os
    import signal as sig_module

    from redis.asyncio import Redis as AsyncRedis

    from services.ingest.ingestion.kafka.producer import (
        IdempotentProducer,
        ProducerConfig,
    )
    from services.ingest.ingestion.rate_limit import RateLimiter
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
        max_signals_per_tick=int(
            os.environ.get("SHARD_FETCH_BATCH", "10"),
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
        rate_limit_max_wait_seconds=float(
            os.environ.get(
                "SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC",
                str(DEFAULT_RATE_LIMIT_MAX_WAIT_SECONDS),
            ),
        ),
    )

    # FetchPage rate-limit gate (LLD §13). Enabled when REDIS_URL is set
    # (it is, in compose's app-env) unless SHARD_FETCH_RATE_LIMIT=0
    # explicitly disables it. When off, the fetch loop runs unthrottled.
    redis = None
    rate_limiter = None
    redis_url = os.environ.get("REDIS_URL")
    if redis_url and os.environ.get("SHARD_FETCH_RATE_LIMIT", "1") != "0":
        redis = AsyncRedis.from_url(redis_url, decode_responses=False)
        rate_limiter = FetchRateLimiter(
            RateLimiter(redis),
            max_wait_seconds=config.rate_limit_max_wait_seconds,
        )
        log.info("workflow.shard_fetch.rate_limit_enabled")

    service = ShardFetch(
        pool, producer, config=config, s3_client=s3_client,
        rate_limiter=rate_limiter,
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_event_loop()
    for s in (sig_module.SIGTERM, sig_module.SIGINT):
        loop.add_signal_handler(s, stop_event.set)

    log.info("workflow.shard_fetch.started", extra={
        "instance": config.instance_name,
    })
    try:
        await service.run(stop_event=stop_event)
    finally:
        log.info("workflow.shard_fetch.shutting_down")
        await producer.stop()
        await s3_client.close()
        if redis is not None:
            await redis.aclose()
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
    "DEFAULT_FLUSH_TIMEOUT_SECONDS",
    "DEFAULT_LEASE_TIMEOUT_SECONDS",
    "DEFAULT_MAX_SIGNALS_PER_TICK",
    "DEFAULT_TICK_INTERVAL_SECONDS",
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
