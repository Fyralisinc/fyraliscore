"""Fleet operational-certification contract used by release automation."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum


class ResilienceScenario(StrEnum):
    PROVIDER_THROTTLE = "provider_throttle"
    PROVIDER_OUTAGE = "provider_outage"
    LEASE_LOSS = "lease_loss"
    CANCELLATION = "cancellation"
    SECRET_ROTATION = "secret_rotation"
    CREDENTIAL_REVOCATION = "credential_revocation"
    POISON_PAYLOAD = "poison_payload"
    MULTI_REGION_FAILOVER = "multi_region_failover"
    DISASTER_RECOVERY_REPLAY = "disaster_recovery_replay"


REQUIRED_RESILIENCE_SCENARIOS = frozenset(ResilienceScenario)


@dataclass(frozen=True)
class ResilienceEvidence:
    connector_id: str
    connector_version: str
    scenario: ResilienceScenario
    passed: bool
    observed_at: datetime
    evidence_ref: str

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None:
            raise ValueError("resilience evidence timestamp must be timezone-aware")
        if not self.evidence_ref:
            raise ValueError("resilience evidence requires a durable reference")


def assert_fleet_resilience_certified(
    evidence: Sequence[ResilienceEvidence],
    *,
    connector_versions: dict[str, str],
    now: datetime | None = None,
    maximum_age: timedelta = timedelta(days=90),
) -> None:
    current = now or datetime.now(UTC)
    by_key: dict[tuple[str, ResilienceScenario], ResilienceEvidence] = {}
    for item in evidence:
        key = (item.connector_id, item.scenario)
        if key in by_key:
            raise ValueError(f"duplicate resilience evidence: {key}")
        by_key[key] = item
    for connector_id, version in connector_versions.items():
        for scenario in REQUIRED_RESILIENCE_SCENARIOS:
            item = by_key.get((connector_id, scenario))
            if item is None:
                raise ValueError(
                    f"resilience evidence missing: {connector_id}/{scenario.value}"
                )
            if not item.passed:
                raise ValueError(
                    f"resilience scenario failed: {connector_id}/{scenario.value}"
                )
            if item.connector_version != version:
                raise ValueError(
                    f"resilience evidence version drifted for {connector_id}"
                )
            if current - item.observed_at > maximum_age:
                raise ValueError(
                    f"resilience evidence expired: {connector_id}/{scenario.value}"
                )


__all__ = [
    "REQUIRED_RESILIENCE_SCENARIOS",
    "ResilienceEvidence",
    "ResilienceScenario",
    "assert_fleet_resilience_certified",
]
