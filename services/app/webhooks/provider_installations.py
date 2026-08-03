"""Compatibility import for the ingestion-owned installation persistence seam."""

from services.ingest.integrations.provider_installations import (
    upsert_provider_installation_for_tenant,
)


__all__ = ["upsert_provider_installation_for_tenant"]
