"""services/ingest/ingestion — uniform ingestion path + per-channel handlers.

Public surface:
- `core.ingest(...)` — UniformIngestPath entry (ARCHITECTURE §14).
- `handlers.get_handler(channel)` — immutable source-contract lookup.
"""
from services.ingest.ingestion.handlers import (  # noqa: F401
    CHANNEL_TRUST_MAP,
    HandlerNotFound,
    ObservationDraft,
    get_handler,
    handler_channels,
)

__all__ = [
    "CHANNEL_TRUST_MAP",
    "HandlerNotFound",
    "ObservationDraft",
    "get_handler",
    "handler_channels",
]
