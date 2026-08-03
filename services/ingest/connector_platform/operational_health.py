"""Sanitized connector control-plane health for readiness dashboards."""

from __future__ import annotations

from typing import Any

from services.ingest.connector_runtime.composition import ConnectorRuntimeComposition


class PostgresConnectorHealthReader:
    def __init__(self, pool: Any, composition: ConnectorRuntimeComposition) -> None:
        self._pool = pool
        self._composition = composition

    async def snapshot(self) -> dict[str, Any]:
        lifecycle_rows = await self._pool.fetch(
            """
            SELECT observed_phase, count(*) AS count
              FROM source_connector_installations
             GROUP BY observed_phase
            """
        )
        artifact_rows = await self._pool.fetch(
            """
            SELECT deployment_status, count(*) AS count
              FROM source_connector_artifacts
             GROUP BY deployment_status
            """
        )
        active_revision = await self._pool.fetchval(
            """
            SELECT revision
              FROM source_connector_routing_revisions
             WHERE status = 'active'
            """
        )
        lifecycle = {
            str(row["observed_phase"]): int(row["count"])
            for row in lifecycle_rows
        }
        artifacts = {
            str(row["deployment_status"]): int(row["count"])
            for row in artifact_rows
        }
        registry = self._composition.registry.health()
        runtime_quarantine = self._composition.routing.quarantined()
        unhealthy = (
            lifecycle.get("Failed", 0)
            + lifecycle.get("Degraded", 0)
            + artifacts.get("quarantined", 0)
            + len(runtime_quarantine)
        )
        return {
            "status": "degraded" if unhealthy else "ok",
            "registry": {
                "status": registry.status.value,
                "fingerprint": registry.fingerprint,
                "connector_count": registry.connector_count,
                "connectors": [
                    {
                        "id": item.connector_id,
                        "source": item.source,
                        "version": item.connector_version,
                    }
                    for item in registry.connectors
                ],
            },
            "routing_revision": (
                int(active_revision)
                if active_revision is not None
                else self._composition.routing.snapshot().revision
            ),
            "lifecycle_phases": lifecycle,
            "artifact_statuses": artifacts,
            "runtime_quarantine": {
                "count": len(runtime_quarantine),
                "connectors": dict(runtime_quarantine),
            },
        }


__all__ = ["PostgresConnectorHealthReader"]
