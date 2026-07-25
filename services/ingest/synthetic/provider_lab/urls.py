"""Catalog-aware Provider Lab URLs.

This services-layer adapter keeps the shared URL contract in ``lib`` free of
dependencies on ingestion's canonical source catalog.
"""
from __future__ import annotations

from lib.integrations.provider_lab import (
    PROVIDER_LAB_URL_ENV,
    provider_lab_root_url,
)
from services.ingest.source_contract import resolve_source_id


_SOURCE_PATH_ALIASES: dict[str, str] = {
    "facebook_pages": "facebook",
    "google_calendar": "gcal",
    "google_drive": "gdrive",
}


def provider_lab_base_url(source: str) -> str:
    """Return the source-scoped lab base for a canonical source or alias."""
    root = provider_lab_root_url()
    if root is None:
        raise RuntimeError(f"{PROVIDER_LAB_URL_ENV} is unset")
    canonical = resolve_source_id(source)
    return f"{root}/{_SOURCE_PATH_ALIASES.get(canonical, canonical)}"


__all__ = ["provider_lab_base_url"]
