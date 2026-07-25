"""Resolve raw-envelope ingress routes from the immutable source contract."""
from __future__ import annotations

from services.ingest.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    SourceLiteral,
)
from services.ingest.source_contract.catalog import source_definition


def resolve_channel(
    source: SourceLiteral,
    ingress_kind: IngressKindLiteral,
) -> str | None:
    """Return the source-declared channel for an ingress kind.

    Unknown sources and unsupported ingress kinds deliberately return
    ``None``. The normalizer records that outcome as an unsupported
    combination instead of applying a catch-all route.
    """

    try:
        definition = source_definition(source)
    except (KeyError, TypeError):
        return None
    return definition.channel_for_ingress(ingress_kind)


__all__ = ["resolve_channel"]
