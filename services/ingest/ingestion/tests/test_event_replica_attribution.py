"""Unit gates for exact event-to-replica attribution."""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any
from uuid import UUID

import orjson
import pytest

from services.ingest.ingestion.event_replica_attribution import (
    EVENT_ATTRIBUTION_METADATA_KEY,
    EVENT_ATTRIBUTION_SCHEMA_VERSION,
    EventAttributionMetadataCollision,
    EventAttributionStamp,
    EventAttributionStampError,
    EventReplicaAttribution,
    EventReplicaAttributionRecord,
    EventReplicaAttributionTransactionRequired,
    EventReplicaIdentityConflict,
    MissingWriterReplicaId,
    UnexpectedReplicaAttribution,
    delete_trial_event_replica_attributions,
    event_attribution_scope,
    merge_current_event_attribution,
    parse_event_replica_attribution,
    purge_expired_event_replica_attributions,
    read_active_event_replica_attributions,
    record_event_replica_attribution,
    replica_processed_item_counts,
)
from services.ingest.ingestion.raw_tier.envelope import RawEnvelope
from services.ingest.ingestion.shadow_write import shadow_write_raw
from services.ingest.ingestion.workflows.shard_fetch import (
    _write_record_and_build_message,
)


pytestmark = pytest.mark.asyncio

_TENANT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_INSTALLATION_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
_NOW = dt.datetime(2026, 7, 27, 12, 0, tzinfo=dt.timezone.utc)
_MIGRATION = (
    Path(__file__).resolve().parents[4]
    / "db/migrations/0199_ingestion_event_replica_attributions.sql"
)


class _FakeConnection:
    def __init__(
        self,
        *,
        in_transaction: bool = True,
        fetchrow_result: dict[str, Any] | None = None,
        fetch_results: list[dict[str, Any]] | None = None,
        fetchval_result: int = 0,
    ) -> None:
        self._in_transaction = in_transaction
        self.fetchrow_result = fetchrow_result
        self.fetch_results = fetch_results or []
        self.fetchval_result = fetchval_result
        self.execute_calls: list[tuple[Any, ...]] = []
        self.fetchrow_calls: list[tuple[Any, ...]] = []
        self.fetch_calls: list[tuple[Any, ...]] = []
        self.fetchval_calls: list[tuple[Any, ...]] = []

    def is_in_transaction(self) -> bool:
        return self._in_transaction

    async def execute(self, query: str, *args: Any) -> str:
        self.execute_calls.append((query, *args))
        return "SELECT 1"

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.fetchrow_calls.append((query, *args))
        return self.fetchrow_result

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, *args))
        return self.fetch_results

    async def fetchval(self, query: str, *args: Any) -> int:
        self.fetchval_calls.append((query, *args))
        return self.fetchval_result


class _RawS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self.objects.setdefault(key, body)


class _RawProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes | None]] = []

    async def produce(
        self,
        topic: str,
        value: bytes,
        *,
        key: bytes | None = None,
    ) -> None:
        self.messages.append((topic, value, key))


def _attribution(*, replica_id: str = "writer-a") -> EventReplicaAttribution:
    return EventReplicaAttribution(
        trial_namespace="pipeline:github:trial-7",
        source="github",
        tenant_id=_TENANT_ID,
        installation_id=_INSTALLATION_ID,
        event_id="github:7:41:0:1:1",
        operation_id="issues.list",
        replica_id=replica_id,
    )


def _row(*, replica_id: str = "writer-a", delivery_count: int = 1) -> dict[str, Any]:
    attribution = _attribution(replica_id=replica_id)
    return {
        "trial_namespace": attribution.trial_namespace,
        "source": attribution.source,
        "tenant_id": attribution.tenant_id,
        "installation_id": attribution.installation_id,
        "event_id": attribution.event_id,
        "operation_id": attribution.operation_id,
        "replica_id": attribution.replica_id,
        "delivery_count": delivery_count,
        "first_recorded_at": _NOW,
        "last_seen_at": _NOW + dt.timedelta(seconds=2),
        "expires_at": _NOW + dt.timedelta(days=7),
    }


def _record(
    *,
    replica_id: str = "writer-a",
    event_id: str | None = None,
) -> EventReplicaAttributionRecord:
    row = _row(replica_id=replica_id)
    if event_id is not None:
        row["event_id"] = event_id
    attribution = EventReplicaAttribution(
        trial_namespace=row["trial_namespace"],
        source=row["source"],
        tenant_id=row["tenant_id"],
        installation_id=row["installation_id"],
        event_id=row["event_id"],
        operation_id=row["operation_id"],
        replica_id=row["replica_id"],
    )
    return EventReplicaAttributionRecord(
        attribution=attribution,
        delivery_count=row["delivery_count"],
        first_recorded_at=row["first_recorded_at"],
        last_seen_at=row["last_seen_at"],
        expires_at=row["expires_at"],
    )


def _stamp() -> EventAttributionStamp:
    return EventAttributionStamp(
        trial_namespace="pipeline:github:trial-7",
        installation_id=_INSTALLATION_ID,
        event_id="github:7:41:0:1:1",
        operation_id="issues.list",
    )


async def test_scoped_stamp_propagates_through_live_and_historical_raw_paths() -> None:
    live_s3 = _RawS3()
    live_producer = _RawProducer()
    with event_attribution_scope(_stamp()):
        await shadow_write_raw(
            tenant_id=_TENANT_ID,
            source="github",
            ingress_kind="webhook",
            raw_body=b'{"event":"live"}',
            s3_client=live_s3,  # type: ignore[arg-type]
            kafka_producer=live_producer,
            ingress_metadata={"delivery_id": "delivery-1"},
            now=_NOW,
        )
        historical = await _write_record_and_build_message(
            _RawS3(),  # type: ignore[arg-type]
            tenant_id=_TENANT_ID,
            source="github",
            shard_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
            installation_row_id=_INSTALLATION_ID,
            cursor=None,
            record={"id": "historical-1"},
            env="test",
            now=_NOW,
        )

    live = RawEnvelope.model_validate(orjson.loads(live_producer.messages[0][1]))
    backfill = RawEnvelope.model_validate(orjson.loads(historical.value))
    expected = _stamp().to_ingress_metadata()
    assert live.ingress_metadata["delivery_id"] == "delivery-1"
    assert live.ingress_metadata[EVENT_ATTRIBUTION_METADATA_KEY] == expected
    assert backfill.ingress_metadata["installation_row_id"] == str(
        _INSTALLATION_ID
    )
    assert backfill.ingress_metadata[EVENT_ATTRIBUTION_METADATA_KEY] == expected
    assert merge_current_event_attribution({"ordinary": True}) == {
        "ordinary": True
    }


async def test_stamp_collision_and_parsing_fail_closed() -> None:
    with pytest.raises(EventAttributionMetadataCollision):
        merge_current_event_attribution(
            {EVENT_ATTRIBUTION_METADATA_KEY: _stamp().to_ingress_metadata()}
        )

    metadata = {
        EVENT_ATTRIBUTION_METADATA_KEY: _stamp().to_ingress_metadata(),
    }
    parsed = parse_event_replica_attribution(
        metadata,
        tenant_id=_TENANT_ID,
        source="github",
        replica_id="writer-a",
    )
    assert parsed == _attribution()

    with pytest.raises(MissingWriterReplicaId):
        parse_event_replica_attribution(
            metadata,
            tenant_id=_TENANT_ID,
            source="github",
            replica_id=None,
        )

    malformed = dict(_stamp().to_ingress_metadata())
    malformed["schema_version"] = EVENT_ATTRIBUTION_SCHEMA_VERSION + ".future"
    with pytest.raises(EventAttributionStampError):
        parse_event_replica_attribution(
            {EVENT_ATTRIBUTION_METADATA_KEY: malformed},
            tenant_id=_TENANT_ID,
            source="github",
            replica_id="writer-a",
        )

    spoofed = dict(_stamp().to_ingress_metadata())
    spoofed["replica_id"] = "metadata-spoof"
    with pytest.raises(EventAttributionStampError):
        parse_event_replica_attribution(
            {EVENT_ATTRIBUTION_METADATA_KEY: spoofed},
            tenant_id=_TENANT_ID,
            source="github",
            replica_id="writer-a",
        )


async def test_record_preserves_first_durable_owner_on_cross_replica_replay() -> None:
    conn = _FakeConnection(
        fetchrow_result=_row(replica_id="writer-a", delivery_count=2),
    )

    result = await record_event_replica_attribution(
        conn,
        _attribution(replica_id="writer-b"),
        recorded_at=_NOW + dt.timedelta(seconds=2),
    )

    assert result.attribution.replica_id == "writer-a"
    assert result.delivery_count == 2
    assert conn.execute_calls == [
        (
            "SELECT set_config('app.current_tenant', $1::text, true)",
            str(_TENANT_ID),
        )
    ]
    query, *args = conn.fetchrow_calls[0]
    assert "ON CONFLICT (trial_namespace, source, event_id)" in query
    assert "replica_id =" not in query.split(" WHERE ", 1)[1]
    assert args[:7] == [
        "pipeline:github:trial-7",
        "github",
        "github:7:41:0:1:1",
        _TENANT_ID,
        _INSTALLATION_ID,
        "issues.list",
        "writer-b",
    ]
    assert args[7] == _NOW + dt.timedelta(seconds=2)
    assert args[8] == _NOW + dt.timedelta(days=7, seconds=2)


async def test_record_fails_closed_on_identity_drift_or_expired_namespace() -> None:
    conn = _FakeConnection(fetchrow_result=None)

    with pytest.raises(EventReplicaIdentityConflict):
        await record_event_replica_attribution(
            conn,
            _attribution(),
            recorded_at=_NOW,
        )


async def test_helpers_require_a_caller_owned_transaction() -> None:
    conn = _FakeConnection(
        in_transaction=False,
        fetchrow_result=_row(),
    )

    with pytest.raises(EventReplicaAttributionTransactionRequired):
        await record_event_replica_attribution(conn, _attribution())

    assert conn.execute_calls == []
    assert conn.fetchrow_calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("trial_namespace", " leading"),
        ("source", ""),
        ("event_id", "event\nid"),
        ("operation_id", "x" * 257),
        ("replica_id", "writer-a "),
        ("tenant_id", "not-a-uuid"),
        ("installation_id", "not-a-uuid"),
    ],
)
async def test_attribution_identity_is_explicit_and_bounded(
    field: str,
    value: object,
) -> None:
    values: dict[str, object] = {
        "trial_namespace": "pipeline:github:trial-7",
        "source": "github",
        "tenant_id": _TENANT_ID,
        "installation_id": _INSTALLATION_ID,
        "event_id": "event-1",
        "operation_id": "issues.list",
        "replica_id": "writer-a",
    }
    values[field] = value

    with pytest.raises(ValueError):
        EventReplicaAttribution(**values)  # type: ignore[arg-type]


async def test_retention_is_positive_and_capped_at_thirty_days() -> None:
    conn = _FakeConnection(fetchrow_result=_row())

    for retention in (dt.timedelta(0), dt.timedelta(days=31)):
        with pytest.raises(ValueError):
            await record_event_replica_attribution(
                conn,
                _attribution(),
                recorded_at=_NOW,
                retention=retention,
            )

    assert conn.execute_calls == []
    assert conn.fetchrow_calls == []


async def test_read_and_cleanup_are_exact_tenant_namespace_operations() -> None:
    read_conn = _FakeConnection(fetch_results=[_row()])
    records = await read_active_event_replica_attributions(
        read_conn,
        trial_namespace="pipeline:github:trial-7",
        source="github",
        tenant_id=_TENANT_ID,
        active_at=_NOW,
    )
    assert records == (_record(),)
    assert read_conn.fetch_calls[0][1:] == (
        "pipeline:github:trial-7",
        "github",
        _TENANT_ID,
        _NOW,
    )

    delete_conn = _FakeConnection(fetchval_result=3)
    deleted = await delete_trial_event_replica_attributions(
        delete_conn,
        trial_namespace="pipeline:github:trial-7",
        source="github",
        tenant_id=_TENANT_ID,
    )
    assert deleted == 3
    delete_query, *delete_args = delete_conn.fetchval_calls[0]
    assert "trial_namespace = $1" in delete_query
    assert "source = $2" in delete_query
    assert "tenant_id = $3" in delete_query
    assert delete_args == [
        "pipeline:github:trial-7",
        "github",
        _TENANT_ID,
    ]

    purge_conn = _FakeConnection(fetchval_result=4)
    purged = await purge_expired_event_replica_attributions(
        purge_conn,
        tenant_id=_TENANT_ID,
        expired_at=_NOW,
    )
    assert purged == 4
    purge_query, *purge_args = purge_conn.fetchval_calls[0]
    assert "tenant_id = $1" in purge_query
    assert "expires_at <= $2" in purge_query
    assert purge_args == [_TENANT_ID, _NOW]


async def test_replica_counts_cover_declared_topology_in_declared_order() -> None:
    records = (
        _record(replica_id="writer-b", event_id="event-2"),
        _record(replica_id="writer-a", event_id="event-1"),
        _record(replica_id="writer-b", event_id="event-3"),
    )

    assert replica_processed_item_counts(
        records,
        replica_ids=("writer-a", "writer-b"),
    ) == (("writer-a", 1), ("writer-b", 2))


async def test_replica_counts_reject_duplicate_events_and_unknown_owners() -> None:
    duplicate = _record(event_id="event-1")
    with pytest.raises(EventReplicaIdentityConflict):
        replica_processed_item_counts(
            (duplicate, duplicate),
            replica_ids=("writer-a", "writer-b"),
        )

    with pytest.raises(UnexpectedReplicaAttribution):
        replica_processed_item_counts(
            (_record(replica_id="writer-c"),),
            replica_ids=("writer-a", "writer-b"),
        )


async def test_migration_encodes_identity_rls_and_retention_contracts() -> None:
    sql = _MIGRATION.read_text(encoding="utf-8")

    assert "PRIMARY KEY (trial_namespace, source, event_id)" in sql
    assert "installation_id  UUID        NOT NULL" in sql
    assert "operation_id     TEXT        NOT NULL" in sql
    assert "replica_id       TEXT        NOT NULL" in sql
    assert "INTERVAL '7 days'" in sql
    assert "INTERVAL '30 days'" in sql
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "FORCE ROW LEVEL SECURITY" in sql
    assert "current_setting('app.current_tenant', true)" in sql
    assert "ingestion_event_replica_attributions_expiry_idx" in sql
