"""Durable event-to-replica ownership for isolated ingestion trials.

This module is deliberately independent of ``source_certification``.  A future
exact-pipeline adapter can place an explicit attribution identity on a raw
event, carry it unchanged through normalization, and call
``record_event_replica_attribution`` after the Observation boundary succeeds
but before its Kafka offset is committed.

The helper never infers tenant, installation, event, operation, or replica
identity from process state or provider payloads.  The first replica whose
claim commits owns the event.  At-least-once redelivery increments
``delivery_count`` while preserving that owner; a replay with changed
tenant/installation/operation identity fails closed.
"""
from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Iterator
from uuid import UUID


DEFAULT_ATTRIBUTION_RETENTION = dt.timedelta(days=7)
MAX_ATTRIBUTION_RETENTION = dt.timedelta(days=30)
EVENT_ATTRIBUTION_METADATA_KEY = "_fyralis_event_replica_attribution"
EVENT_ATTRIBUTION_SCHEMA_VERSION = (
    "fyralis.ingestion.event-replica-attribution.v1"
)

_TEXT_LIMITS = {
    "trial_namespace": 200,
    "source": 64,
    "event_id": 512,
    "operation_id": 256,
    "replica_id": 256,
}


class EventReplicaAttributionError(RuntimeError):
    """Base error for the exact attribution ledger."""


class EventReplicaAttributionTransactionRequired(EventReplicaAttributionError):
    """The helper was called without a caller-owned transaction."""


class EventReplicaIdentityConflict(EventReplicaAttributionError):
    """An event key already exists with different immutable identity."""


class UnexpectedReplicaAttribution(EventReplicaAttributionError):
    """A stored event belongs to a replica outside the declared topology."""


class EventAttributionStampError(EventReplicaAttributionError):
    """A scoped or serialized attribution stamp is invalid."""


class EventAttributionMetadataCollision(EventAttributionStampError):
    """Caller metadata or a nested scope already owns the reserved key."""


class MissingWriterReplicaId(EventAttributionStampError):
    """A stamped event reached a writer with no explicit replica identity."""


@dataclass(frozen=True, slots=True)
class EventAttributionStamp:
    """Producer-owned identity carried through raw and normalized envelopes.

    Tenant and source deliberately do not live in the stamp: the writer takes
    those only from the validated ``NormalizedEnvelope``.  Replica identity is
    also excluded and comes only from the writer process configuration.
    """

    trial_namespace: str
    installation_id: UUID
    event_id: str
    operation_id: str

    def __post_init__(self) -> None:
        _validate_text(
            self.trial_namespace,
            "trial_namespace",
            _TEXT_LIMITS["trial_namespace"],
        )
        _validate_uuid(self.installation_id, "installation_id")
        _validate_text(self.event_id, "event_id", _TEXT_LIMITS["event_id"])
        _validate_text(
            self.operation_id,
            "operation_id",
            _TEXT_LIMITS["operation_id"],
        )

    def to_ingress_metadata(self) -> dict[str, object]:
        return {
            "schema_version": EVENT_ATTRIBUTION_SCHEMA_VERSION,
            "trial_namespace": self.trial_namespace,
            "installation_id": str(self.installation_id),
            "event_id": self.event_id,
            "operation_id": self.operation_id,
        }


_CURRENT_EVENT_ATTRIBUTION: ContextVar[EventAttributionStamp | None] = ContextVar(
    "fyralis_current_event_attribution",
    default=None,
)
_STAMP_FIELDS = frozenset(
    {
        "schema_version",
        "trial_namespace",
        "installation_id",
        "event_id",
        "operation_id",
    }
)


@contextmanager
def event_attribution_scope(
    stamp: EventAttributionStamp,
) -> Iterator[EventAttributionStamp]:
    """Scope one opt-in attribution stamp across async dispatch.

    Context variables are copied into child tasks by ``asyncio``.  Nested
    attribution scopes are rejected because silently replacing an outer event
    identity would make ownership depend on call ordering.
    """

    if not isinstance(stamp, EventAttributionStamp):
        raise TypeError("stamp must be EventAttributionStamp")
    if _CURRENT_EVENT_ATTRIBUTION.get() is not None:
        raise EventAttributionMetadataCollision(
            "an event attribution scope is already active"
        )
    token = _CURRENT_EVENT_ATTRIBUTION.set(stamp)
    try:
        yield stamp
    finally:
        _CURRENT_EVENT_ATTRIBUTION.reset(token)


def current_event_attribution_stamp() -> EventAttributionStamp | None:
    """Return the current opt-in stamp without manufacturing a default."""

    return _CURRENT_EVENT_ATTRIBUTION.get()


def merge_current_event_attribution(
    ingress_metadata: Mapping[str, Any] | None,
    *,
    expected_installation_id: UUID | None = None,
) -> dict[str, Any]:
    """Copy caller metadata and merge the current reserved stamp.

    The reserved key may only be created from ``event_attribution_scope``.
    Pre-populating it is rejected even when no scope is active, preventing
    provider/caller metadata from spoofing a diagnostic event identity.
    """

    merged = dict(ingress_metadata or {})
    if EVENT_ATTRIBUTION_METADATA_KEY in merged:
        raise EventAttributionMetadataCollision(
            f"{EVENT_ATTRIBUTION_METADATA_KEY!r} is reserved"
        )
    stamp = current_event_attribution_stamp()
    if stamp is None:
        return merged
    if (
        expected_installation_id is not None
        and stamp.installation_id != expected_installation_id
    ):
        raise EventReplicaIdentityConflict(
            "scoped attribution installation does not match the exact "
            "historical installation row"
        )
    merged[EVENT_ATTRIBUTION_METADATA_KEY] = stamp.to_ingress_metadata()
    return merged


def parse_event_replica_attribution(
    ingress_metadata: Mapping[str, Any],
    *,
    tenant_id: UUID,
    source: str,
    replica_id: str | None,
) -> EventReplicaAttribution | None:
    """Parse a strict v1 stamp using trusted envelope/process identity.

    An unstamped production event is a no-op.  Once the reserved key is
    present, malformed/extra fields and a missing writer replica fail closed.
    """

    payload = ingress_metadata.get(EVENT_ATTRIBUTION_METADATA_KEY)
    if payload is None:
        return None
    if not isinstance(payload, Mapping):
        raise EventAttributionStampError(
            "event attribution metadata must be an object"
        )
    fields = frozenset(payload)
    if fields != _STAMP_FIELDS:
        missing = sorted(_STAMP_FIELDS - fields)
        extra = sorted(fields - _STAMP_FIELDS)
        raise EventAttributionStampError(
            f"event attribution metadata fields differ "
            f"(missing={missing}, extra={extra})"
        )
    if payload["schema_version"] != EVENT_ATTRIBUTION_SCHEMA_VERSION:
        raise EventAttributionStampError(
            "event attribution schema_version is unsupported"
        )
    if replica_id is None:
        raise MissingWriterReplicaId(
            "stamped event requires explicit WRITER_REPLICA_ID"
        )

    installation_raw = payload["installation_id"]
    if not isinstance(installation_raw, str):
        raise EventAttributionStampError(
            "event attribution installation_id must be a canonical UUID"
        )
    try:
        installation_id = UUID(installation_raw)
    except ValueError as exc:
        raise EventAttributionStampError(
            "event attribution installation_id must be a canonical UUID"
        ) from exc
    if str(installation_id) != installation_raw:
        raise EventAttributionStampError(
            "event attribution installation_id must be a canonical UUID"
        )

    try:
        return EventReplicaAttribution(
            trial_namespace=payload["trial_namespace"],
            source=source,
            tenant_id=tenant_id,
            installation_id=installation_id,
            event_id=payload["event_id"],
            operation_id=payload["operation_id"],
            replica_id=replica_id,
        )
    except (TypeError, ValueError) as exc:
        raise EventAttributionStampError(
            f"event attribution identity is invalid: {exc}"
        ) from exc


@dataclass(frozen=True, slots=True)
class EventReplicaAttribution:
    """Exact identity supplied at the durable writer boundary."""

    trial_namespace: str
    source: str
    tenant_id: UUID
    installation_id: UUID
    event_id: str
    operation_id: str
    replica_id: str

    def __post_init__(self) -> None:
        _validate_text(
            self.trial_namespace,
            "trial_namespace",
            _TEXT_LIMITS["trial_namespace"],
        )
        _validate_text(self.source, "source", _TEXT_LIMITS["source"])
        _validate_text(self.event_id, "event_id", _TEXT_LIMITS["event_id"])
        _validate_text(
            self.operation_id,
            "operation_id",
            _TEXT_LIMITS["operation_id"],
        )
        _validate_text(
            self.replica_id,
            "replica_id",
            _TEXT_LIMITS["replica_id"],
        )
        _validate_uuid(self.tenant_id, "tenant_id")
        _validate_uuid(self.installation_id, "installation_id")


@dataclass(frozen=True, slots=True)
class EventReplicaAttributionRecord:
    """One persisted event owner plus replay and retention metadata."""

    attribution: EventReplicaAttribution
    delivery_count: int
    first_recorded_at: dt.datetime
    last_seen_at: dt.datetime
    expires_at: dt.datetime

    def __post_init__(self) -> None:
        if (
            isinstance(self.delivery_count, bool)
            or not isinstance(self.delivery_count, int)
            or self.delivery_count < 1
        ):
            raise ValueError("delivery_count must be a positive integer")
        first = _aware_utc(self.first_recorded_at, "first_recorded_at")
        last = _aware_utc(self.last_seen_at, "last_seen_at")
        expires = _aware_utc(self.expires_at, "expires_at")
        if last < first:
            raise ValueError("last_seen_at cannot precede first_recorded_at")
        if expires <= first:
            raise ValueError("expires_at must follow first_recorded_at")
        if expires - first > MAX_ATTRIBUTION_RETENTION:
            raise ValueError("attribution retention exceeds the 30-day maximum")


_RECORD_SQL = """
INSERT INTO ingestion_event_replica_attributions (
    trial_namespace,
    source,
    event_id,
    tenant_id,
    installation_id,
    operation_id,
    replica_id,
    delivery_count,
    first_recorded_at,
    last_seen_at,
    expires_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 1, $8, $8, $9)
ON CONFLICT (trial_namespace, source, event_id) DO UPDATE
   SET delivery_count =
           ingestion_event_replica_attributions.delivery_count + 1,
       last_seen_at = GREATEST(
           ingestion_event_replica_attributions.last_seen_at,
           EXCLUDED.last_seen_at
       )
 WHERE ingestion_event_replica_attributions.tenant_id =
           EXCLUDED.tenant_id
   AND ingestion_event_replica_attributions.installation_id =
           EXCLUDED.installation_id
   AND ingestion_event_replica_attributions.operation_id =
           EXCLUDED.operation_id
   AND ingestion_event_replica_attributions.expires_at >
           EXCLUDED.first_recorded_at
RETURNING
    trial_namespace,
    source,
    event_id,
    tenant_id,
    installation_id,
    operation_id,
    replica_id,
    delivery_count,
    first_recorded_at,
    last_seen_at,
    expires_at
"""

_READ_ACTIVE_SQL = """
SELECT
    trial_namespace,
    source,
    event_id,
    tenant_id,
    installation_id,
    operation_id,
    replica_id,
    delivery_count,
    first_recorded_at,
    last_seen_at,
    expires_at
  FROM ingestion_event_replica_attributions
 WHERE trial_namespace = $1
   AND source = $2
   AND tenant_id = $3
   AND expires_at > $4
 ORDER BY event_id
"""

_DELETE_TRIAL_SQL = """
WITH deleted AS (
    DELETE FROM ingestion_event_replica_attributions
     WHERE trial_namespace = $1
       AND source = $2
       AND tenant_id = $3
    RETURNING 1
)
SELECT count(*) FROM deleted
"""

_PURGE_EXPIRED_SQL = """
WITH deleted AS (
    DELETE FROM ingestion_event_replica_attributions
     WHERE tenant_id = $1
       AND expires_at <= $2
    RETURNING 1
)
SELECT count(*) FROM deleted
"""


async def record_event_replica_attribution(
    conn: Any,
    attribution: EventReplicaAttribution,
    *,
    recorded_at: dt.datetime | None = None,
    retention: dt.timedelta = DEFAULT_ATTRIBUTION_RETENTION,
) -> EventReplicaAttributionRecord:
    """Atomically claim one event for the first durable replica.

    The caller must own an explicit transaction.  The helper binds strict RLS
    with ``SET LOCAL app.current_tenant`` and never commits on the caller's
    behalf.  A writer integration must let any exception escape so its Kafka
    offset remains uncommitted and the event can be retried.

    A replay from another replica returns the original owner and increments
    ``delivery_count``.  It does not move the event between replicas.
    """

    _require_transaction(conn)
    if not isinstance(attribution, EventReplicaAttribution):
        raise TypeError("attribution must be EventReplicaAttribution")
    now = (
        dt.datetime.now(tz=dt.timezone.utc)
        if recorded_at is None
        else _aware_utc(recorded_at, "recorded_at")
    )
    validated_retention = _validate_retention(retention)
    try:
        expires_at = now + validated_retention
    except OverflowError as exc:
        raise ValueError("attribution expiry is outside datetime range") from exc

    await _bind_tenant(conn, attribution.tenant_id)
    row = await conn.fetchrow(
        _RECORD_SQL,
        attribution.trial_namespace,
        attribution.source,
        attribution.event_id,
        attribution.tenant_id,
        attribution.installation_id,
        attribution.operation_id,
        attribution.replica_id,
        now,
        expires_at,
    )
    if row is None:
        raise EventReplicaIdentityConflict(
            "event attribution conflicts with an existing tenant, "
            "installation, or operation identity, or the trial namespace "
            "has expired and was not cleaned"
        )
    return _record_from_row(row)


async def read_active_event_replica_attributions(
    conn: Any,
    *,
    trial_namespace: str,
    source: str,
    tenant_id: UUID,
    active_at: dt.datetime | None = None,
) -> tuple[EventReplicaAttributionRecord, ...]:
    """Read active rows for one exact tenant and trial namespace.

    A two-tenant adapter calls this once per declared tenant and combines the
    results.  Requiring the tenant on every read keeps RLS and query scope
    aligned; this helper has no cross-tenant or unbound fallback.
    """

    _require_transaction(conn)
    _validate_scope(trial_namespace, source, tenant_id)
    when = (
        dt.datetime.now(tz=dt.timezone.utc)
        if active_at is None
        else _aware_utc(active_at, "active_at")
    )
    await _bind_tenant(conn, tenant_id)
    rows = await conn.fetch(
        _READ_ACTIVE_SQL,
        trial_namespace,
        source,
        tenant_id,
        when,
    )
    return tuple(_record_from_row(row) for row in rows)


async def delete_trial_event_replica_attributions(
    conn: Any,
    *,
    trial_namespace: str,
    source: str,
    tenant_id: UUID,
) -> int:
    """Delete one tenant's rows in an exact trial/source namespace."""

    _require_transaction(conn)
    _validate_scope(trial_namespace, source, tenant_id)
    await _bind_tenant(conn, tenant_id)
    deleted = await conn.fetchval(
        _DELETE_TRIAL_SQL,
        trial_namespace,
        source,
        tenant_id,
    )
    return int(deleted)


async def purge_expired_event_replica_attributions(
    conn: Any,
    *,
    tenant_id: UUID,
    expired_at: dt.datetime | None = None,
) -> int:
    """Delete expired attribution metadata for one exact tenant."""

    _require_transaction(conn)
    _validate_uuid(tenant_id, "tenant_id")
    when = (
        dt.datetime.now(tz=dt.timezone.utc)
        if expired_at is None
        else _aware_utc(expired_at, "expired_at")
    )
    await _bind_tenant(conn, tenant_id)
    deleted = await conn.fetchval(_PURGE_EXPIRED_SQL, tenant_id, when)
    return int(deleted)


def replica_processed_item_counts(
    records: Sequence[EventReplicaAttributionRecord],
    *,
    replica_ids: Sequence[str],
) -> tuple[tuple[str, int], ...]:
    """Count one immutable event owner against each declared replica.

    The return order exactly matches ``replica_ids``.  Duplicate event keys or
    an owner outside the declared topology fail closed instead of being hidden
    in an aggregate.
    """

    expected = tuple(replica_ids)
    if len(expected) != len(set(expected)):
        raise ValueError("replica_ids must be unique")
    for replica_id in expected:
        _validate_text(replica_id, "replica_id", _TEXT_LIMITS["replica_id"])

    counts = dict.fromkeys(expected, 0)
    seen: set[tuple[str, str, str]] = set()
    for record in records:
        key = (
            record.attribution.trial_namespace,
            record.attribution.source,
            record.attribution.event_id,
        )
        if key in seen:
            raise EventReplicaIdentityConflict(
                "duplicate event attribution returned across tenant reads"
            )
        seen.add(key)
        owner = record.attribution.replica_id
        if owner not in counts:
            raise UnexpectedReplicaAttribution(
                f"event {record.attribution.event_id!r} is owned by "
                f"undeclared replica {owner!r}"
            )
        counts[owner] += 1
    return tuple((replica_id, counts[replica_id]) for replica_id in expected)


def _validate_scope(
    trial_namespace: str,
    source: str,
    tenant_id: UUID,
) -> None:
    _validate_text(
        trial_namespace,
        "trial_namespace",
        _TEXT_LIMITS["trial_namespace"],
    )
    _validate_text(source, "source", _TEXT_LIMITS["source"])
    _validate_uuid(tenant_id, "tenant_id")


def _validate_text(value: object, name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    if value != value.strip():
        raise ValueError(f"{name} must not contain surrounding whitespace")
    if len(value) > maximum:
        raise ValueError(f"{name} exceeds {maximum} characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f"{name} must not contain control characters")
    return value


def _validate_uuid(value: object, name: str) -> UUID:
    if not isinstance(value, UUID):
        raise ValueError(f"{name} must be a UUID")
    return value


def _aware_utc(value: dt.datetime, name: str) -> dt.datetime:
    if (
        not isinstance(value, dt.datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(dt.timezone.utc)


def _validate_retention(value: dt.timedelta) -> dt.timedelta:
    if not isinstance(value, dt.timedelta):
        raise ValueError("retention must be a timedelta")
    if value <= dt.timedelta(0):
        raise ValueError("retention must be positive")
    if value > MAX_ATTRIBUTION_RETENTION:
        raise ValueError("retention exceeds the 30-day maximum")
    return value


def _require_transaction(conn: Any) -> None:
    raw_conn = getattr(conn, "conn", conn)
    is_in_transaction = getattr(raw_conn, "is_in_transaction", None)
    if not callable(is_in_transaction) or not is_in_transaction():
        raise EventReplicaAttributionTransactionRequired(
            "event attribution helpers require a caller-owned transaction"
        )


async def _bind_tenant(conn: Any, tenant_id: UUID) -> None:
    await conn.execute(
        "SELECT set_config('app.current_tenant', $1::text, true)",
        str(tenant_id),
    )


def _record_from_row(
    row: Mapping[str, Any],
) -> EventReplicaAttributionRecord:
    attribution = EventReplicaAttribution(
        trial_namespace=str(row["trial_namespace"]),
        source=str(row["source"]),
        tenant_id=row["tenant_id"],
        installation_id=row["installation_id"],
        event_id=str(row["event_id"]),
        operation_id=str(row["operation_id"]),
        replica_id=str(row["replica_id"]),
    )
    return EventReplicaAttributionRecord(
        attribution=attribution,
        delivery_count=int(row["delivery_count"]),
        first_recorded_at=row["first_recorded_at"],
        last_seen_at=row["last_seen_at"],
        expires_at=row["expires_at"],
    )


__all__ = [
    "DEFAULT_ATTRIBUTION_RETENTION",
    "EVENT_ATTRIBUTION_METADATA_KEY",
    "EVENT_ATTRIBUTION_SCHEMA_VERSION",
    "MAX_ATTRIBUTION_RETENTION",
    "EventAttributionMetadataCollision",
    "EventAttributionStamp",
    "EventAttributionStampError",
    "EventReplicaAttribution",
    "EventReplicaAttributionError",
    "EventReplicaAttributionRecord",
    "EventReplicaAttributionTransactionRequired",
    "EventReplicaIdentityConflict",
    "MissingWriterReplicaId",
    "UnexpectedReplicaAttribution",
    "current_event_attribution_stamp",
    "delete_trial_event_replica_attributions",
    "event_attribution_scope",
    "merge_current_event_attribution",
    "parse_event_replica_attribution",
    "purge_expired_event_replica_attributions",
    "read_active_event_replica_attributions",
    "record_event_replica_attribution",
    "replica_processed_item_counts",
]
