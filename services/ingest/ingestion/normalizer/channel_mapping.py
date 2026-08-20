"""Direct channel mapping for consolidation carry-forward adapters.

Stable connector sources own semantic routing inside their manifests. This
module remains only for compatibility with the Instagram and Facebook Pages
history/webhook implementations that predate their SourceConnector-v1 ports.
"""

from __future__ import annotations

from services.ingest.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    SourceLiteral,
)
from services.ingest.source_contract.source_catalog import (
    supplemental_source_channel,
)

_SUPPORTED_INGRESS: dict[str, frozenset[str]] = {
    "facebook_pages": frozenset({"backfill", "webhook"}),
    "instagram": frozenset({"backfill", "poll", "webhook"}),
}


def resolve_channel(
    source: SourceLiteral,
    ingress_kind: IngressKindLiteral,
) -> str | None:
    if ingress_kind not in _SUPPORTED_INGRESS.get(source, frozenset()):
        return None
    return supplemental_source_channel(source)


__all__ = ["resolve_channel"]
