"""services/ingestion/rate_limit — per-(tenant, source, method) Lua token bucket.

Per ingestion LLD §13. Public surface:
  - `RateLimiter`     — async client; owns script-load and EVALSHA.
  - `AcquireResult`   — return shape from `RateLimiter.acquire`.
  - `BUCKET_DEFAULTS` — per (source, method) default capacity/refill.
"""
from services.ingestion.rate_limit.buckets import (  # noqa: F401
    BUCKET_DEFAULTS,
    BucketSpec,
)
from services.ingestion.rate_limit.client import (  # noqa: F401
    AcquireResult,
    RateLimiter,
)

__all__ = [
    "AcquireResult",
    "BucketSpec",
    "BUCKET_DEFAULTS",
    "RateLimiter",
]
