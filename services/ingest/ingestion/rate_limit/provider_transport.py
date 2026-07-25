"""Adapter from the universal provider transport to the Redis Lua limiter."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence
from dataclasses import dataclass

from lib.shared.provider_transport import (
    QuotaDenialReason,
    QuotaDecision,
    QuotaRequirement,
)
from services.ingest.ingestion.rate_limit.client import RateLimiter
from services.ingest.ingestion.rate_limit.client import BucketRequirement


@dataclass(frozen=True, slots=True)
class DistributedCircuitConfig:
    """Universal safety bounds applied independently to each quota bucket."""

    consecutive_failure_threshold: int = 3
    open_duration_seconds: float = 30.0
    half_open_probe_lease_seconds: float = 10.0
    state_retention_seconds: float = 24 * 60 * 60

    def __post_init__(self) -> None:
        if (
            isinstance(self.consecutive_failure_threshold, bool)
            or not isinstance(self.consecutive_failure_threshold, int)
            or self.consecutive_failure_threshold < 1
        ):
            raise ValueError(
                "consecutive_failure_threshold must be an integer >= 1"
            )
        for field_name in (
            "open_duration_seconds",
            "half_open_probe_lease_seconds",
            "state_retention_seconds",
        ):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ValueError(f"{field_name} must be finite and > 0")
        if self.state_retention_seconds < max(
            self.open_duration_seconds,
            self.half_open_probe_lease_seconds,
        ):
            raise ValueError(
                "state_retention_seconds must cover open and probe windows"
            )


class RedisQuotaCoordinator:
    """Atomic quota and circuit coordination over exact Redis bucket keys."""

    def __init__(
        self,
        limiter: RateLimiter,
        *,
        circuit: DistributedCircuitConfig | None = None,
    ) -> None:
        self._limiter = limiter
        self._circuit = circuit or DistributedCircuitConfig()

    async def acquire_many(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> QuotaDecision:
        ordered = sorted(
            requirements,
            key=lambda item: item.bucket_key,
        )
        result = await self._limiter.acquire_many_guarded(
            tuple(
                BucketRequirement(
                    bucket_key=requirement.bucket_key,
                    capacity=requirement.capacity,
                    refill_per_sec=requirement.refill_per_second,
                    cost=requirement.cost,
                )
                for requirement in ordered
            ),
            half_open_probe_lease_ms=self._milliseconds(
                self._circuit.half_open_probe_lease_seconds
            ),
            circuit_state_retention_ms=self._milliseconds(
                self._circuit.state_retention_seconds
            ),
        )
        if not result.granted:
            assert result.blocked_index is not None
            blocked = ordered[result.blocked_index]
            retry_after = (
                None
                if result.retry_after_ms < 0
                else result.retry_after_ms / 1000.0
            )
            return QuotaDecision.deny(
                retry_after_seconds=retry_after,
                blocked_scope=blocked.scope,
                blocked_bucket_key=blocked.bucket_key,
                denial_reason=(
                    QuotaDenialReason.CIRCUIT_OPEN
                    if result.circuit_open
                    else QuotaDenialReason.QUOTA
                ),
            )
        return QuotaDecision.allow()

    async def report_cooldown(
        self,
        requirements: Sequence[QuotaRequirement],
        *,
        retry_after_seconds: float,
    ) -> None:
        retry_after_ms = max(1, math.ceil(retry_after_seconds * 1000.0))
        await asyncio.gather(
            *(
                self._limiter.report_retry_after(
                    requirement.bucket_key,
                    retry_after_ms=retry_after_ms,
                )
                for requirement in requirements
            ),
        )

    async def report_success(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        ordered = sorted(requirements, key=lambda item: item.bucket_key)
        await self._limiter.record_circuit_success(
            tuple(item.bucket_key for item in ordered),
            circuit_state_retention_ms=self._milliseconds(
                self._circuit.state_retention_seconds
            ),
        )

    async def report_failure(
        self,
        requirements: Sequence[QuotaRequirement],
    ) -> None:
        ordered = sorted(requirements, key=lambda item: item.bucket_key)
        await self._limiter.record_circuit_failure(
            tuple(item.bucket_key for item in ordered),
            consecutive_failure_threshold=(
                self._circuit.consecutive_failure_threshold
            ),
            open_duration_ms=self._milliseconds(
                self._circuit.open_duration_seconds
            ),
            circuit_state_retention_ms=self._milliseconds(
                self._circuit.state_retention_seconds
            ),
        )

    @staticmethod
    def _milliseconds(seconds: float) -> int:
        return max(1, math.ceil(float(seconds) * 1000.0))


__all__ = ["DistributedCircuitConfig", "RedisQuotaCoordinator"]
