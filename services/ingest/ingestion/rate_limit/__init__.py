"""services/ingest/ingestion/rate_limit — per-(tenant, source, method) Lua token bucket.

Per ingestion LLD §13. Public surface:
  - `RateLimiter`        — async client; owns script-load and EVALSHA.
  - `AcquireResult`      — return shape from `RateLimiter.acquire`.
  - `BUCKET_DEFAULTS`    — per (source, method) default capacity/refill.
  - `FetchRateLimiter`   — the FetchPage gate: consumes one token per
                           page fetch from the (source, method) bucket.
  - `RateLimitWaitExceeded` — raised when a bucket stays empty past the
                           gate's bounded wait; ShardFetch treats it as
                           a transient fetch-loop exit.
"""
from services.ingest.ingestion.rate_limit.buckets import (  # noqa: F401
    BUCKET_DEFAULTS,
    BucketSpec,
)
from services.ingest.ingestion.rate_limit.client import (  # noqa: F401
    AcquireResult,
    RateLimiter,
)
from services.ingest.ingestion.rate_limit.gate import (  # noqa: F401
    PRIMARY_FETCH_METHOD,
    FetchRateLimiter,
    RateLimitWaitExceeded,
    fetch_bucket_key,
)

__all__ = [
    "AcquireResult",
    "BucketSpec",
    "BUCKET_DEFAULTS",
    "FetchRateLimiter",
    "PRIMARY_FETCH_METHOD",
    "RateLimitWaitExceeded",
    "RateLimiter",
    "fetch_bucket_key",
]
