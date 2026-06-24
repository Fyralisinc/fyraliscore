"""Deployment record (C4) — the fleet-registry row + health derivation.

The fleet registry stores exactly one row per deployment, in the shape pinned by
SPRINT_PLAN C4::

    {
      "tenant_id": "acme",
      "deployment_id": "acme-use1-7f3a",
      "version": "1.4.2",
      "region": "us-east-1",
      "last_heartbeat_ts": "2026-06-24T00:00:00Z",
      "health": "green",
      "license_expiry": "2027-06-24T00:00:00Z",
      "telemetry_tier": "T1"
    }

Field names, the ``health`` enum (``green|yellow|red``), the ``telemetry_tier``
enum (``T1|T2|T3``), and RFC-3339 UTC timestamps are part of the contract and
must not be renamed.

Health is **derived**, not authored: this module is the single place that turns
heartbeat freshness (and optional fleet-SLI burn flags) into ``green/yellow/red``
so the agent (which mints the record) and the console (which displays it) agree.

The agent typically builds a record with ``DeploymentRecord.heartbeat(...)``
(which derives health from the heartbeat age) and the console re-derives on read
to catch a deployment that went silent *after* its last heartbeat.
"""

from __future__ import annotations

import datetime as _dt
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from .primitives import parse_rfc3339, to_rfc3339, utcnow
from .tiers import TelemetryTier

__all__ = [
    "Health",
    "DeploymentRecord",
    "derive_health",
    "DEFAULT_YELLOW_AFTER_S",
    "DEFAULT_RED_AFTER_S",
]


# Defaults mirror lib.config; kept here too so health derivation is usable with
# no config object (the agent and tests call it directly).
DEFAULT_YELLOW_AFTER_S = 90
DEFAULT_RED_AFTER_S = 300


class Health(str, Enum):
    """Derived deployment health (C4)."""

    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"

    @property
    def rank(self) -> int:
        """green < yellow < red, so ``max`` over ranks = the worst health."""
        return {Health.GREEN: 0, Health.YELLOW: 1, Health.RED: 2}[self]

    @classmethod
    def worst(cls, *healths: "Health") -> "Health":
        """Return the most-degraded health among the arguments."""
        return max(healths, key=lambda h: h.rank) if healths else cls.GREEN


def derive_health(
    last_heartbeat_ts: "str | _dt.datetime",
    *,
    now: _dt.datetime | None = None,
    yellow_after_s: int = DEFAULT_YELLOW_AFTER_S,
    red_after_s: int = DEFAULT_RED_AFTER_S,
    sli_breached: bool = False,
    license_expiry: "str | _dt.datetime | None" = None,
) -> Health:
    """Derive ``green | yellow | red`` from heartbeat age + flags (C4).

    Rules (fail toward *worse* health):
      * heartbeat age ≤ ``yellow_after_s``       → ``green``
      * ``yellow_after_s`` < age ≤ ``red_after_s`` → ``yellow`` (stale)
      * age > ``red_after_s``                    → ``red`` (missing/expired)
      * a heartbeat in the future is clamped to age 0 (clock skew → not penalized)
      * ``sli_breached=True`` (fleet-SLI burn flag) degrades green→yellow
      * an **expired license** forces ``red`` regardless of heartbeat — an
        expired deployment is not healthy even if it is still beating.

    The worst applicable condition wins.
    """
    now = now or utcnow()
    hb = (
        last_heartbeat_ts
        if isinstance(last_heartbeat_ts, _dt.datetime)
        else parse_rfc3339(last_heartbeat_ts)
    )
    if hb.tzinfo is None:
        hb = hb.replace(tzinfo=_dt.timezone.utc)

    age_s = (now - hb).total_seconds()
    if age_s < 0:
        age_s = 0.0  # future heartbeat → clock skew, treat as fresh

    if age_s > red_after_s:
        from_heartbeat = Health.RED
    elif age_s > yellow_after_s:
        from_heartbeat = Health.YELLOW
    else:
        from_heartbeat = Health.GREEN

    candidates = [from_heartbeat]
    if sli_breached:
        candidates.append(Health.YELLOW)

    if license_expiry is not None:
        exp = (
            license_expiry
            if isinstance(license_expiry, _dt.datetime)
            else parse_rfc3339(license_expiry)
        )
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=_dt.timezone.utc)
        if exp <= now:
            candidates.append(Health.RED)

    return Health.worst(*candidates)


class DeploymentRecord(BaseModel):
    """One fleet-registry row (C4).

    Serializes to exactly the C4 JSON shape: timestamps are RFC-3339 UTC strings
    (``...Z``), ``health`` is ``green|yellow|red``, ``telemetry_tier`` is
    ``T1|T2|T3``. Use :meth:`heartbeat` to build a record whose health is derived
    from the heartbeat age, and :meth:`with_derived_health` to re-derive on read.
    """

    model_config = ConfigDict(extra="forbid", use_enum_values=False)

    tenant_id: str = Field(min_length=1)
    deployment_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    region: str = Field(min_length=1)
    last_heartbeat_ts: _dt.datetime
    health: Health = Health.GREEN
    license_expiry: _dt.datetime
    telemetry_tier: TelemetryTier = TelemetryTier.T1

    # --- validators / normalizers -----------------------------------------

    @field_validator("last_heartbeat_ts", "license_expiry", mode="before")
    @classmethod
    def _coerce_ts(cls, v):
        if isinstance(v, _dt.datetime):
            return v if v.tzinfo else v.replace(tzinfo=_dt.timezone.utc)
        if isinstance(v, str):
            return parse_rfc3339(v)
        raise ValueError(f"expected datetime or RFC-3339 string, got {type(v).__name__}")

    @field_validator("health", mode="before")
    @classmethod
    def _coerce_health(cls, v):
        if isinstance(v, Health):
            return v
        return Health(str(v).strip().lower())

    @field_validator("telemetry_tier", mode="before")
    @classmethod
    def _coerce_tier(cls, v):
        return TelemetryTier.parse(v)

    # --- serializers: emit the C4 wire shape ------------------------------

    @field_serializer("last_heartbeat_ts", "license_expiry")
    def _ser_ts(self, value: _dt.datetime) -> str:
        return to_rfc3339(value)

    @field_serializer("health")
    def _ser_health(self, value: Health) -> str:
        return value.value

    @field_serializer("telemetry_tier")
    def _ser_tier(self, value: TelemetryTier) -> str:
        return value.value

    # --- helpers ----------------------------------------------------------

    def to_registry_dict(self) -> dict:
        """Return the exact C4 JSON-able dict (RFC-3339 strings + enum values)."""
        return self.model_dump(mode="json")

    def derived_health(
        self,
        *,
        now: _dt.datetime | None = None,
        yellow_after_s: int = DEFAULT_YELLOW_AFTER_S,
        red_after_s: int = DEFAULT_RED_AFTER_S,
        sli_breached: bool = False,
    ) -> Health:
        """Compute what this record's health *should* be right now."""
        return derive_health(
            self.last_heartbeat_ts,
            now=now,
            yellow_after_s=yellow_after_s,
            red_after_s=red_after_s,
            sli_breached=sli_breached,
            license_expiry=self.license_expiry,
        )

    def with_derived_health(
        self,
        *,
        now: _dt.datetime | None = None,
        yellow_after_s: int = DEFAULT_YELLOW_AFTER_S,
        red_after_s: int = DEFAULT_RED_AFTER_S,
        sli_breached: bool = False,
    ) -> "DeploymentRecord":
        """Return a copy with ``health`` recomputed (console read-path use)."""
        return self.model_copy(
            update={
                "health": self.derived_health(
                    now=now,
                    yellow_after_s=yellow_after_s,
                    red_after_s=red_after_s,
                    sli_breached=sli_breached,
                )
            }
        )

    @classmethod
    def heartbeat(
        cls,
        *,
        tenant_id: str,
        deployment_id: str,
        version: str,
        region: str,
        license_expiry: "str | _dt.datetime",
        telemetry_tier: "TelemetryTier | str" = TelemetryTier.T1,
        last_heartbeat_ts: "str | _dt.datetime | None" = None,
        now: _dt.datetime | None = None,
        yellow_after_s: int = DEFAULT_YELLOW_AFTER_S,
        red_after_s: int = DEFAULT_RED_AFTER_S,
        sli_breached: bool = False,
    ) -> "DeploymentRecord":
        """Build a record at heartbeat time with health derived from freshness.

        This is what the agent calls each heartbeat: it stamps
        ``last_heartbeat_ts`` (defaulting to now) and derives ``health`` so the
        emitted record is internally consistent.
        """
        now = now or utcnow()
        hb = last_heartbeat_ts if last_heartbeat_ts is not None else now
        hb_dt = hb if isinstance(hb, _dt.datetime) else parse_rfc3339(hb)
        health = derive_health(
            hb_dt,
            now=now,
            yellow_after_s=yellow_after_s,
            red_after_s=red_after_s,
            sli_breached=sli_breached,
            license_expiry=license_expiry,
        )
        return cls(
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            version=version,
            region=region,
            last_heartbeat_ts=hb_dt,
            health=health,
            license_expiry=license_expiry,
            telemetry_tier=telemetry_tier,
        )
