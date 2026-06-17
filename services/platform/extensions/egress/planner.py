"""services/platform/extensions/egress/planner.py — pure egress fan-out decision.

Given one observation (as an :class:`ObservationView`) and the active grants for its
tenant, decide which extensions receive it and with what redaction. No I/O — the
Kafka projector calls this after loading grants; tests call it directly.

Capability rules (mirror the read-API gate, E2):
  * the grant must include ``substrate_read:observation``;
  * the observation's channel must be in the grant's ``read_channels`` (or ALL);
then the view is run through the channel's egress redaction (E3.1b).
"""
from __future__ import annotations

from dataclasses import dataclass

from lib.extensions.host_api.v1 import Capabilities, ObservationView
from services.platform.extensions.redaction import redact


@dataclass(frozen=True)
class EgressItem:
    extension_id: str
    tenant_id: str
    view: ObservationView  # already redacted


@dataclass(frozen=True)
class GrantSpec:
    """The slice of an active grant the planner needs (extension_id + capabilities)."""

    extension_id: str
    capabilities: Capabilities


def _allows(caps: Capabilities, channel: str) -> bool:
    return "observation" in caps.substrate_read and caps.allows_channel(channel)


def plan_egress(view: ObservationView, grants: list[GrantSpec]) -> list[EgressItem]:
    """Fan one observation out to every extension whose grant permits it, redacted."""
    items: list[EgressItem] = []
    for g in grants:
        if _allows(g.capabilities, view.source_channel):
            items.append(EgressItem(
                extension_id=g.extension_id,
                tenant_id=str(view.tenant_id),
                view=redact(view),
            ))
    return items


__all__ = ["EgressItem", "GrantSpec", "plan_egress"]
