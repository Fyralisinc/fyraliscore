"""Distributed Lua token buckets used by provider transport.

Provider clients declare quota requirements per actual outbound operation.
``RedisQuotaCoordinator`` atomically charges those scopes through the generic
``RateLimiter``. Workflow code does not guess source budgets or sleep around a
logical page fetch.
"""
from services.ingest.ingestion.rate_limit.client import (  # noqa: F401
    AcquireResult,
    BucketRequirement,
    GuardedAcquireResult,
    MultiAcquireResult,
    RateLimiter,
)
from services.ingest.ingestion.rate_limit.provider_transport import (
    DistributedCircuitConfig,
    RedisQuotaCoordinator,
)

__all__ = [
    "AcquireResult",
    "BucketRequirement",
    "DistributedCircuitConfig",
    "GuardedAcquireResult",
    "MultiAcquireResult",
    "RateLimiter",
    "RedisQuotaCoordinator",
]
