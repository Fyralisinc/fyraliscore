"""services/ingestion — uniform ingestion path + per-channel handlers.

Public surface:
- `core.ingest(...)` — UniformIngestPath entry (ARCHITECTURE §14).
- `handlers.get_handler(channel)` — registry lookup for a channel.
- `handlers.register(channel)` — decorator for handler modules.
"""
from services.ingestion.handlers import (  # noqa: F401
    CHANNEL_TRUST_MAP,
    HandlerNotFound,
    ObservationDraft,
    get_handler,
    handler_channels,
    register,
)

__all__ = [
    "CHANNEL_TRUST_MAP",
    "HandlerNotFound",
    "ObservationDraft",
    "get_handler",
    "handler_channels",
    "register",
]
