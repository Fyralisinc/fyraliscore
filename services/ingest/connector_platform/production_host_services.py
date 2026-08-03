"""Production Fyralis backends for least-authority connector host ports."""

from __future__ import annotations

import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import httpx
import orjson

from lib.shared.db_leases import PostgresLease
from lib.shared.ids import uuid7
from lib.shared.secrets import SecretStore
from services.ingest.connector_runtime.host_services import (
    HostServicesFactory,
    MetricIncrement,
    MetricObserve,
)
from services.ingest.ingestion.kafka.flush_batcher import coalesced_flush
from services.ingest.ingestion.raw_tier.s3 import compute_content_hash
from services.ingest.ingestion.shadow_write import shadow_write_raw
from services.ingest.source_contract.errors import (
    PermissionDeniedError,
    StateIncompatibleError,
)
from services.ingest.source_contract.host_services import (
    CallbackAllocation,
    InstallationData,
    InstallationDataPatch,
    SecretCandidate,
    SecretValue,
)
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    PublicationReceipt,
    SourceRecord,
    VersionedState,
)


@dataclass(frozen=True)
class ProductionHostBackends:
    pool: Any
    secret_store: SecretStore
    http_client: httpx.AsyncClient
    s3_raw_client: Any | None = None
    kafka_producer: Any | None = None
    callback_base_url: str | None = None
    metric_incrementer: MetricIncrement | None = None
    metric_observer: MetricObserve | None = None


async def _installation(pool: Any, installation_id: UUID) -> Any:
    row = await pool.fetchrow(
        """
        SELECT id, tenant_id, connector_id, external_installation_id
          FROM source_connector_installations
         WHERE id = $1
        """,
        installation_id,
    )
    if row is None:
        raise PermissionDeniedError("connector installation is unavailable")
    return row


def build_production_host_services_factory(
    backends: ProductionHostBackends,
) -> HostServicesFactory:
    pool = backends.pool
    leases: dict[UUID, PostgresLease] = {}

    async def secret_reader(installation_id: UUID, slot: SlotId) -> SecretValue:
        install = await _installation(pool, installation_id)
        ref = await pool.fetchval(
            """
            SELECT secret_ref
              FROM source_connector_credentials
             WHERE installation_id = $1
               AND tenant_id = $2
               AND slot = $3
               AND state = 'current'
            """,
            installation_id,
            install["tenant_id"],
            str(slot),
        )
        if ref is None:
            raise PermissionDeniedError(
                "installation has no current credential for this slot",
                details={"slot": str(slot)},
            )
        value = await backends.secret_store.get(
            str(ref), tenant_id=install["tenant_id"]
        )
        return SecretValue(value)

    async def secret_writer(installation_id: UUID, candidate: SecretCandidate) -> str:
        install = await _installation(pool, installation_id)
        ref = await backends.secret_store.put(
            candidate.value.reveal_bytes(),
            label=f"connector_candidate:{installation_id}:{candidate.slot}",
            tenant_id=install["tenant_id"],
        )
        generation = int(
            await pool.fetchval(
                """
                SELECT COALESCE(MAX(generation), 0) + 1
                  FROM source_connector_credentials
                 WHERE installation_id = $1 AND slot = $2
                """,
                installation_id,
                str(candidate.slot),
            )
        )
        await pool.execute(
            """
            INSERT INTO source_connector_credentials (
                installation_id, tenant_id, slot, secret_ref, state,
                generation, owner, provenance
            ) VALUES ($1, $2, $3, $4, 'pending', $5, 'connector', $6::jsonb)
            """,
            installation_id,
            install["tenant_id"],
            str(candidate.slot),
            ref,
            generation,
            json.dumps(
                {
                    "expires_at": (
                        candidate.expires_at.isoformat()
                        if candidate.expires_at is not None
                        else None
                    )
                }
            ),
        )
        return str(ref)

    async def state_reader(installation_id: UUID, kind: str) -> VersionedState | None:
        row = await pool.fetchrow(
            """
            SELECT generation, values
              FROM source_connector_installation_data
             WHERE installation_id = $1 AND namespace = $2
            """,
            installation_id,
            f"state:{kind}",
        )
        if row is None:
            return None
        values = dict(row["values"] or {})
        return VersionedState(
            kind=kind,
            schema_version=int(values["schema_version"]),
            producing_connector_version=str(values["connector_version"]),
            revision=int(row["generation"]),
            payload=dict(values.get("payload") or {}),
        )

    async def installation_reader(
        installation_id: UUID, namespace: str
    ) -> InstallationData | None:
        row = await pool.fetchrow(
            """
            SELECT generation, values
              FROM source_connector_installation_data
             WHERE installation_id = $1 AND namespace = $2
            """,
            installation_id,
            namespace,
        )
        if row is None:
            return None
        return InstallationData(
            namespace=namespace,
            generation=int(row["generation"]),
            values=dict(row["values"] or {}),
        )

    async def installation_writer(
        installation_id: UUID, patch: InstallationDataPatch
    ) -> int:
        install = await _installation(pool, installation_id)
        row = await pool.fetchrow(
            """
            INSERT INTO source_connector_installation_data (
                installation_id, tenant_id, namespace, generation, values
            ) VALUES ($1, $2, $3, 1, $5::jsonb)
            ON CONFLICT (installation_id, namespace) DO UPDATE
               SET generation = source_connector_installation_data.generation + 1,
                   values = EXCLUDED.values,
                   updated_at = now()
             WHERE source_connector_installation_data.generation = $4
            RETURNING generation
            """,
            installation_id,
            install["tenant_id"],
            patch.namespace,
            patch.expected_generation,
            json.dumps(patch.values),
        )
        if row is None:
            raise StateIncompatibleError(
                "installation data compare-and-set generation did not match"
            )
        return int(row["generation"])

    async def raw_publisher(
        installation_id: UUID, record: SourceRecord
    ) -> PublicationReceipt:
        if backends.s3_raw_client is None or backends.kafka_producer is None:
            raise PermissionDeniedError("durable raw publication is unavailable")
        install = await _installation(pool, installation_id)
        raw_body = (
            record.payload
            if isinstance(record.payload, bytes)
            else orjson.dumps(record.payload)
        )
        acknowledged_at = datetime.now(timezone.utc)
        source = str(install["connector_id"]).removeprefix("fyralis/")
        raw_key = await shadow_write_raw(
            tenant_id=install["tenant_id"],
            source=source,  # type: ignore[arg-type]
            ingress_kind="gateway",
            connector_installation_id=installation_id,
            raw_body=raw_body,
            s3_client=backends.s3_raw_client,
            kafka_producer=backends.kafka_producer,
            ingress_metadata={"native_type": record.native_type},
            idem_hints=record.identity_hints,
            now=acknowledged_at,
        )
        remaining = await coalesced_flush(backends.kafka_producer, timeout_seconds=2.0)
        if remaining:
            raise RuntimeError("durable raw publication was not acknowledged")
        return PublicationReceipt(
            receipt_id=uuid7(),
            raw_object_key=raw_key,
            content_hash=compute_content_hash(raw_body),
            acknowledged_at=acknowledged_at,
        )

    async def callback_provider(
        installation_id: UUID, purpose: str
    ) -> CallbackAllocation:
        if not backends.callback_base_url:
            raise PermissionDeniedError("callback allocation is unavailable")
        install = await _installation(pool, installation_id)
        endpoint_id = uuid7()
        nonce = secrets.token_urlsafe(32)
        ref = await backends.secret_store.put(
            nonce,
            label=f"connector_callback:{endpoint_id}",
            tenant_id=install["tenant_id"],
        )
        await pool.execute(
            """
            INSERT INTO source_connector_callbacks (
                endpoint_id, installation_id, tenant_id, purpose,
                nonce_secret_ref
            ) VALUES ($1, $2, $3, $4, $5)
            """,
            endpoint_id,
            installation_id,
            install["tenant_id"],
            purpose,
            ref,
        )
        return CallbackAllocation(
            callback_url=(
                f"{backends.callback_base_url.rstrip('/')}/connectors/"
                f"{install['connector_id']}/webhook/{endpoint_id}"
            ),
            endpoint_id=str(endpoint_id),
            verification_nonce=SecretValue.from_text(nonce),
        )

    async def lease_heartbeat(
        installation_id: UUID, details: dict[str, Any] | None
    ) -> None:
        lease = leases.get(installation_id)
        if lease is None:
            lease = PostgresLease(
                pool,
                lease_name=f"source_connector:{installation_id}",
                metadata=details,
            )
            leases[installation_id] = lease
        if lease.is_held():
            if not await lease.refresh():
                raise PermissionDeniedError("connector execution lease was lost")
        elif not await lease.acquire():
            raise PermissionDeniedError("connector execution lease is held")

    return HostServicesFactory(
        http_client=backends.http_client,
        secret_reader=secret_reader,
        secret_writer=secret_writer,
        state_reader=state_reader,
        installation_reader=installation_reader,
        installation_writer=installation_writer,
        raw_publisher=raw_publisher,
        callback_provider=callback_provider,
        metric_incrementer=backends.metric_incrementer,
        metric_observer=backends.metric_observer,
        lease_heartbeat=lease_heartbeat,
    )


__all__ = [
    "ProductionHostBackends",
    "build_production_host_services_factory",
]
