"""Tenant feature-flag plumbing for ingestion.

Per ingestion LLD §11 (cutover feature flags). M2 surface:
  - `TenantFlags` — per-process reader with a 30s TTL cache.
  - `SHADOW_WRITE_ENABLED` / `KAFKA_PATH_ENABLED` — flag-name constants.

M5 will add the circuit breaker (`circuit_breaker.py`) and the
write-side helper that flips flags when the breaker fires.
"""
from services.ingestion.feature_flags.client import (  # noqa: F401
    KAFKA_PATH_ENABLED,
    SHADOW_WRITE_ENABLED,
    FlagCache,
    TenantFlags,
)

__all__ = [
    "KAFKA_PATH_ENABLED",
    "SHADOW_WRITE_ENABLED",
    "FlagCache",
    "TenantFlags",
]
