"""Tenant-scoped feature flags shared by platform services."""

from services.ingest.ingestion.feature_flags.client import FlagCache, TenantFlags

__all__ = ["FlagCache", "TenantFlags"]
