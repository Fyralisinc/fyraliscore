"""Serializable results produced by the connector conformance harness."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict


class ConformanceStatus(StrEnum):
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"


class ConformanceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ConformanceCheck(ConformanceModel):
    name: str
    status: ConformanceStatus
    message: str
    diagnostic_code: str | None = None


class ConformanceReport(ConformanceModel):
    suite_version: str
    connector_id: str
    connector_version: str
    fingerprint: str
    checks: tuple[ConformanceCheck, ...]

    @property
    def passed(self) -> bool:
        return all(
            check.status is not ConformanceStatus.FAILED
            for check in self.checks
        )

    @property
    def warnings(self) -> tuple[ConformanceCheck, ...]:
        return tuple(
            check
            for check in self.checks
            if check.status is ConformanceStatus.WARNING
        )

    @property
    def failures(self) -> tuple[ConformanceCheck, ...]:
        return tuple(
            check
            for check in self.checks
            if check.status is ConformanceStatus.FAILED
        )


__all__ = [
    "ConformanceCheck",
    "ConformanceModel",
    "ConformanceReport",
    "ConformanceStatus",
]
