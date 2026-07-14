"""Telemetry tiers (C3) — the customer-configurable, boundary-enforced policy.

Per SPRINT_PLAN C3 the tiers are **cumulative**:

    | Tier | Contents                          | PII / payload |
    |------|-----------------------------------|---------------|
    | T1   | aggregated metrics only (default) | ZERO (I1)     |
    | T2   | T1 + redacted logs                | logs redacted |
    | T3   | T1 + T2 + sampled traces          | traces redacted |

This module is the **single source of truth** for "what may leave the customer
VPC at tier X" and is consumed by the boundary OTel Collector's tier-enforcement
processor (P2). The boundary is configured to the advertised tier and **drops**
any signal class above it; this module gives that processor a precise, testable
``permits(signal_class)`` predicate plus the redaction obligations.

Nothing here talks to a network; it is a pure policy table so both the boundary
(enforcer) and the agent (advertiser) share identical semantics.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, ConfigDict

from .errors import TierError

__all__ = [
    "SignalClass",
    "TelemetryTier",
    "TierPolicy",
    "tier_policy",
    "TIER_POLICIES",
]


class SignalClass(str, Enum):
    """The classes of telemetry signal that could cross the VPC boundary."""

    METRICS = "metrics"
    LOGS = "logs"
    TRACES = "traces"


class TelemetryTier(str, Enum):
    """The three cumulative telemetry tiers from C3.

    String-valued so a tier round-trips as ``"T1"`` in JSON (the C4 deployment
    record stores ``telemetry_tier`` as exactly this literal).
    """

    T1 = "T1"
    T2 = "T2"
    T3 = "T3"

    @classmethod
    def parse(cls, value: "str | TelemetryTier") -> "TelemetryTier":
        """Parse a tier from a string/enum, raising ``TierError`` on garbage."""
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            key = value.strip().upper()
            try:
                return cls(key)
            except ValueError:
                pass
        raise TierError(f"unknown telemetry tier: {value!r} (expected T1|T2|T3)")

    @property
    def rank(self) -> int:
        """Numeric rank for cumulative comparisons (T1=1, T2=2, T3=3)."""
        return {TelemetryTier.T1: 1, TelemetryTier.T2: 2, TelemetryTier.T3: 3}[self]

    def includes(self, other: "TelemetryTier") -> bool:
        """True if this tier is at least ``other`` (tiers are cumulative)."""
        return self.rank >= other.rank


class TierPolicy(BaseModel):
    """What a single tier permits to leave the customer VPC.

    ``permitted_signals`` is the cumulative set; ``redacted_signals`` is the
    subset that may egress **only after boundary redaction** (logs at T2, traces
    at T3). Metrics are never redacted because at T1 they carry zero PII (I1).
    """

    model_config = ConfigDict(frozen=True)

    tier: TelemetryTier
    permitted_signals: frozenset[SignalClass]
    redacted_signals: frozenset[SignalClass]
    description: str

    def permits(self, signal: "SignalClass | str") -> bool:
        """True if ``signal`` may cross the boundary at this tier."""
        sig = signal if isinstance(signal, SignalClass) else SignalClass(signal)
        return sig in self.permitted_signals

    def requires_redaction(self, signal: "SignalClass | str") -> bool:
        """True if ``signal`` must be redacted at the boundary before egress."""
        sig = signal if isinstance(signal, SignalClass) else SignalClass(signal)
        return sig in self.redacted_signals

    def carries_pii_risk(self) -> bool:
        """True if this tier can emit anything beyond zero-PII metrics.

        T1 is the only tier for which this is False — the I1 guarantee.
        """
        return self.permitted_signals != frozenset({SignalClass.METRICS})


# --- the policy table (cumulative) -----------------------------------------

TIER_POLICIES: dict[TelemetryTier, TierPolicy] = {
    TelemetryTier.T1: TierPolicy(
        tier=TelemetryTier.T1,
        permitted_signals=frozenset({SignalClass.METRICS}),
        redacted_signals=frozenset(),
        description="Aggregated metrics only — zero PII / payload (default, I1).",
    ),
    TelemetryTier.T2: TierPolicy(
        tier=TelemetryTier.T2,
        permitted_signals=frozenset({SignalClass.METRICS, SignalClass.LOGS}),
        redacted_signals=frozenset({SignalClass.LOGS}),
        description="T1 + redacted logs (logs redacted at the boundary before egress).",
    ),
    TelemetryTier.T3: TierPolicy(
        tier=TelemetryTier.T3,
        permitted_signals=frozenset(
            {SignalClass.METRICS, SignalClass.LOGS, SignalClass.TRACES}
        ),
        redacted_signals=frozenset({SignalClass.LOGS, SignalClass.TRACES}),
        description="T1 + T2 + sampled traces (traces sampled + redacted).",
    ),
}


def tier_policy(tier: "TelemetryTier | str") -> TierPolicy:
    """Return the :class:`TierPolicy` for a tier (parsing strings safely)."""
    return TIER_POLICIES[TelemetryTier.parse(tier)]
