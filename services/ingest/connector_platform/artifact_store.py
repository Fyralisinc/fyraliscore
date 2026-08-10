"""PostgreSQL artifact provenance and quarantine repository."""

from __future__ import annotations

from datetime import timezone
from typing import Any

from services.ingest.connector_runtime.artifacts import (
    ArtifactAttestation,
    DeploymentStatus,
)


class PostgresArtifactRepository:
    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def load_all(self) -> dict[tuple[str, str], ArtifactAttestation]:
        rows = await self._pool.fetch(
            """
            SELECT connector_id, connector_version, artifact_sha256,
                   manifest_sha256, conformance_fingerprint, signer_key_id,
                   builder_id, source_revision, built_at, signature,
                   deployment_status, quarantine_reason
              FROM source_connector_artifacts
             WHERE deployment_status <> 'retired'
            """
        )
        return {
            (row["connector_id"], row["connector_version"]): ArtifactAttestation(
                connector_id=row["connector_id"],
                connector_version=row["connector_version"],
                artifact_sha256=row["artifact_sha256"],
                manifest_sha256=row["manifest_sha256"],
                conformance_fingerprint=row["conformance_fingerprint"],
                signer_key_id=row["signer_key_id"],
                builder_id=row["builder_id"],
                source_revision=row["source_revision"],
                built_at=row["built_at"].astimezone(timezone.utc),
                signature=row["signature"],
                deployment_status=DeploymentStatus(row["deployment_status"]),
                quarantine_reason=row["quarantine_reason"],
            )
            for row in rows
        }

    async def quarantine(
        self,
        connector_id: str,
        connector_version: str,
        reason: str,
    ) -> None:
        await self._pool.execute(
            """
            UPDATE source_connector_artifacts
               SET deployment_status = 'quarantined',
                   quarantine_reason = $3,
                   updated_at = now()
             WHERE connector_id = $1 AND connector_version = $2
            """,
            connector_id,
            connector_version,
            reason,
        )


__all__ = ["PostgresArtifactRepository"]
