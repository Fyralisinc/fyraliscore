"""lib.extensions.host_api.v1.capabilities — the capability vocabulary.

The canonical, parseable capability model an extension declares (in its manifest)
and an operator grants (in ``extension_grants``). The granted set is always the
*intersection* of declared-and-approved; an extension can never receive more than
it declared.

The vocabulary mirrors the ``can_read`` discriminators
(``services.platform.access_control.checks``):
  - ``SUBSTRATE_KINDS`` == ``can_read``'s entity kinds,
  - ``RESOURCE_KINDS`` == ``_RESOURCE_KIND_ROLES`` keys.
A test asserts this equivalence so the two cannot drift.

Pure stdlib/dataclasses → safe under the ``lib`` floor.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# The six substrate entity kinds (== can_read EntityKind).
SUBSTRATE_KINDS: frozenset[str] = frozenset(
    {"observation", "commitment", "goal", "decision", "resource", "model"}
)

# Resource kinds (== _RESOURCE_KIND_ROLES keys).
RESOURCE_KINDS: frozenset[str] = frozenset(
    {"financial", "ip", "relational", "capacity", "infrastructure", "regulatory"}
)

MUTATE_REASONING_VALUES: tuple[str, ...] = ("none", "augment_only", "contribute_diff")

# Sentinel: read all channels (subject to substrate_read + tenant scope).
ALL_CHANNELS = "all"


class CapabilityError(Exception):
    """Raised when a capability is requested/used outside what was granted, or a
    manifest declares an invalid capability."""


@dataclass(frozen=True)
class Capabilities:
    """A parsed, validated capability grant."""

    read_channels: tuple[str, ...] | str = ()  # tuple of channels, or ALL_CHANNELS
    substrate_read: frozenset[str] = frozenset()
    substrate_write: frozenset[str] = frozenset()
    write_observations: bool = False
    mutate_reasoning: str = "none"
    resource_kinds: frozenset[str] = frozenset()

    # ---- queries -----------------------------------------------------
    def allows_channel(self, channel: str) -> bool:
        if self.read_channels == ALL_CHANNELS:
            return True
        return channel in self.read_channels

    def allows_read_kind(self, kind: str) -> bool:
        return kind in self.substrate_read

    def allows_resource_kind(self, kind: str) -> bool:
        return kind in self.resource_kinds

    @property
    def may_write_reasoning(self) -> bool:
        return self.mutate_reasoning == "contribute_diff"

    # ---- (de)serialization ------------------------------------------
    def to_dict(self) -> dict[str, Any]:
        return {
            "read_channels": (
                self.read_channels
                if self.read_channels == ALL_CHANNELS
                else list(self.read_channels)
            ),
            "substrate_read": sorted(self.substrate_read),
            "substrate_write": sorted(self.substrate_write),
            "write_observations": self.write_observations,
            "mutate_reasoning": self.mutate_reasoning,
            "resource_kinds": sorted(self.resource_kinds),
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> "Capabilities":
        """Parse + validate. Raises CapabilityError on unknown kinds/values."""
        raw = raw or {}
        rc = raw.get("read_channels", ())
        if rc == ALL_CHANNELS:
            read_channels: tuple[str, ...] | str = ALL_CHANNELS
        elif isinstance(rc, (list, tuple)):
            read_channels = tuple(str(c) for c in rc)
        elif rc in ((), None, ""):
            read_channels = ()
        else:
            raise CapabilityError(f"read_channels must be a list or {ALL_CHANNELS!r}")

        sr = frozenset(str(k) for k in (raw.get("substrate_read") or []))
        sw = frozenset(str(k) for k in (raw.get("substrate_write") or []))
        bad = (sr | sw) - SUBSTRATE_KINDS
        if bad:
            raise CapabilityError(f"unknown substrate kind(s): {sorted(bad)}")

        rk = frozenset(str(k) for k in (raw.get("resource_kinds") or []))
        bad_rk = rk - RESOURCE_KINDS
        if bad_rk:
            raise CapabilityError(f"unknown resource_kind(s): {sorted(bad_rk)}")

        mr = str(raw.get("mutate_reasoning", "none"))
        if mr not in MUTATE_REASONING_VALUES:
            raise CapabilityError(
                f"mutate_reasoning must be one of {MUTATE_REASONING_VALUES}, got {mr!r}"
            )

        return cls(
            read_channels=read_channels,
            substrate_read=sr,
            substrate_write=sw,
            write_observations=bool(raw.get("write_observations", False)),
            mutate_reasoning=mr,
            resource_kinds=rk,
        )

    def intersect(self, approved: "Capabilities") -> "Capabilities":
        """The effective grant = intersection(declared=self, approved). An
        extension can never receive more than it declared."""
        if self.read_channels == ALL_CHANNELS:
            rc: tuple[str, ...] | str = approved.read_channels
        elif approved.read_channels == ALL_CHANNELS:
            rc = self.read_channels
        else:
            rc = tuple(c for c in self.read_channels if c in set(approved.read_channels))
        # mutate_reasoning: take the weaker of the two by ladder position.
        order = {v: i for i, v in enumerate(MUTATE_REASONING_VALUES)}
        mr = min(self.mutate_reasoning, approved.mutate_reasoning, key=lambda v: order[v])
        return Capabilities(
            read_channels=rc,
            substrate_read=self.substrate_read & approved.substrate_read,
            substrate_write=self.substrate_write & approved.substrate_write,
            write_observations=self.write_observations and approved.write_observations,
            mutate_reasoning=mr,
            resource_kinds=self.resource_kinds & approved.resource_kinds,
        )


__all__ = [
    "Capabilities",
    "CapabilityError",
    "SUBSTRATE_KINDS",
    "RESOURCE_KINDS",
    "MUTATE_REASONING_VALUES",
    "ALL_CHANNELS",
]
