"""Ingestion adapter for the benchmark foundation.

This module intentionally starts with an in-memory store. Public adapter
work can swap this for the real Fyralis ingestion path while preserving
the runner and evaluator contracts.
"""

from __future__ import annotations

from collections import defaultdict

from benchmarks.adapters.base import BenchmarkObservation


class InMemoryBenchmarkStore:
    """Tenant-partitioned observation store used by smoke benchmark runs."""

    def __init__(self) -> None:
        self._observations_by_tenant: dict[str, list[BenchmarkObservation]] = defaultdict(list)

    def ingest(self, observations: list[BenchmarkObservation]) -> None:
        for observation in observations:
            self._observations_by_tenant[observation.tenant_id].append(observation)
        for tenant_id in self._observations_by_tenant:
            self._observations_by_tenant[tenant_id].sort(
                key=lambda item: item.occurred_at,
                reverse=True,
            )

    def observations_for_tenant(self, tenant_id: str) -> list[BenchmarkObservation]:
        return list(self._observations_by_tenant.get(tenant_id, []))

    def count_observations(self) -> int:
        return sum(len(rows) for rows in self._observations_by_tenant.values())

