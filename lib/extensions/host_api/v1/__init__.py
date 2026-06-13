"""lib.extensions.host_api.v1 — stable host API v1 for Fyralis extensions.

Extensions bind to the names here and to the ``company_os.*`` entry-point groups,
**not** to core internals. This is the SemVer-guaranteed surface a manifest's
``engines.fyralis_host_api`` range targets.

> The original ADR-0004 sketch used ``host_api/v3`` illustratively (mirroring VS
> Code's versioning). The published surface starts at **v1** — there is no v2/v3
> to imply. The discipline (read projections, a SemVer pin, a stable/proposed
> split) is unchanged.

v1 scope is the **draft-enricher** contribution point (the generalization of the
former hardcoded github inline hook). Read-projection / substrate-reader surfaces
land in v1 minor releases as the host API hardens (roadmap E1); today an
in-process extension still reaches core data through the symbols documented in its
manifest's capabilities — acceptable for first-party / verified in-process
extensions, the boundary that matters for untrusted third parties is the
network/edge tier (roadmap E3+).
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

HOST_API_VERSION = "1.0"

# A draft-enricher mutates ``draft.content`` (and optionally ``draft.content_text``)
# in place *before persistence*, so the same observation row carries the derived
# signal. It is duck-typed against the host's ObservationDraft — it only reads /
# writes ``.content``, ``.content_text``, ``.raw_payload``, ``.occurred_at`` — so
# the contract does not leak the concrete internal type. Hard rules (enforced by
# the host runner, but the enricher should honour them too):
#   * read-only with respect to the substrate (no observation/state writes here);
#   * bounded (own timeout);
#   * never raise — on any error the RAW draft must persist unchanged.
# Signature: ``async def fn(draft, *, pool, tenant_id) -> None``.
EnricherFn = Callable[..., Awaitable[None]]


@dataclass(frozen=True)
class DraftEnricher:
    """One enricher contributed for a source channel.

    The unit an extension exposes through the ``company_os.draft_enrichers``
    entry-point group: the group's entry point resolves to a ``DraftEnricher``
    (or a list of them, or a zero-arg callable returning either).
    """

    channel: str
    fn: EnricherFn
    name: str = "enricher"


# Read projections, the capability-checked read contract, and the capability
# vocabulary — re-exported so extensions import them from one stable place.
from lib.extensions.host_api.v1.capabilities import (  # noqa: E402
    ALL_CHANNELS,
    MUTATE_REASONING_VALUES,
    Capabilities,
    CapabilityError,
    RESOURCE_KINDS,
    SUBSTRATE_KINDS,
)
from lib.extensions.host_api.v1.substrate import SubstrateReader  # noqa: E402
from lib.extensions.host_api.v1.views import (  # noqa: E402
    DraftView,
    ModelView,
    ObservationView,
)

__all__ = [
    "HOST_API_VERSION",
    "EnricherFn",
    "DraftEnricher",
    "ObservationView",
    "DraftView",
    "ModelView",
    "SubstrateReader",
    "Capabilities",
    "CapabilityError",
    "SUBSTRATE_KINDS",
    "RESOURCE_KINDS",
    "MUTATE_REASONING_VALUES",
    "ALL_CHANNELS",
]
