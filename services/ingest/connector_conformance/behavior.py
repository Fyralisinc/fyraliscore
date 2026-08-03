"""Independent behavioral conformance for connector release evidence.

The suite consumes deterministic fixtures instead of production host services.
Connector packages provide the operations; the suite owns the assertions and
the stable evidence fingerprint used by artifact admission.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from services.ingest.connector_conformance.models import (
    ConformanceCheck,
    ConformanceReport,
    ConformanceStatus,
)


BEHAVIORAL_SUITE_VERSION = "source-connector-behavior/v1"
REQUIRED_BEHAVIORS = (
    "pagination",
    "cursor_monotonicity",
    "identity_stability",
    "reconciliation",
    "retry_classification",
    "webhook_verification",
    "normalization",
    "cleanup",
    "lifecycle",
    "state_migration",
)


@dataclass(frozen=True)
class PageEvidence:
    cursor: Mapping[str, Any] | None
    record_ids: tuple[str, ...]
    end_of_data: bool = False


BehaviorCheck = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class BehavioralFixture:
    checks: Mapping[str, BehaviorCheck]


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


async def assert_pagination(pages: Sequence[PageEvidence]) -> None:
    if not pages:
        raise AssertionError("pagination fixture produced no pages")
    seen_records: set[str] = set()
    for page in pages:
        overlap = seen_records.intersection(page.record_ids)
        if overlap:
            raise AssertionError(f"records repeated across pages: {sorted(overlap)}")
        seen_records.update(page.record_ids)
    if not pages[-1].end_of_data:
        raise AssertionError("final pagination fixture must signal end_of_data")


async def assert_cursor_monotonicity(pages: Sequence[PageEvidence]) -> None:
    cursors = [canonical(page.cursor) for page in pages if page.cursor is not None]
    if len(cursors) != len(set(cursors)):
        raise AssertionError("a non-terminal cursor repeated")
    if any(page.end_of_data and page.cursor is not None for page in pages):
        raise AssertionError("terminal pages must not expose another cursor")


def stable_operation_check(
    operation: Callable[[], Awaitable[Any]],
    *,
    label: str,
) -> BehaviorCheck:
    async def check() -> None:
        first = canonical(await operation())
        second = canonical(await operation())
        if first != second:
            raise AssertionError(f"{label} is not deterministic")

    return check


def retry_classification_check(
    classifier: Callable[[BaseException], bool],
    *,
    transient: BaseException,
    permanent: BaseException,
) -> BehaviorCheck:
    async def check() -> None:
        if not classifier(transient):
            raise AssertionError("transient failure was classified permanent")
        if classifier(permanent):
            raise AssertionError("permanent failure was classified retryable")

    return check


def webhook_verification_check(
    verify_valid: Callable[[], Awaitable[bool]],
    verify_invalid: Callable[[], Awaitable[bool]],
) -> BehaviorCheck:
    async def check() -> None:
        if not await verify_valid():
            raise AssertionError("valid webhook was rejected")
        if await verify_invalid():
            raise AssertionError("invalid webhook was accepted")

    return check


def cleanup_idempotency_check(cleanup: Callable[[], Awaitable[bool]]) -> BehaviorCheck:
    async def check() -> None:
        if not await cleanup() or not await cleanup():
            raise AssertionError("cleanup did not complete idempotently")

    return check


def lifecycle_sequence_check(
    observe: Callable[[], Awaitable[Sequence[str]]],
) -> BehaviorCheck:
    async def check() -> None:
        phases = tuple(await observe())
        if not phases or phases[-1] not in {"Ready", "Degraded", "Removed"}:
            raise AssertionError("lifecycle did not reach a stable phase")
        if "Removed" in phases and phases[-1] != "Removed":
            raise AssertionError("Removed lifecycle phase is terminal")

    return check


def state_migration_check(
    migrate: Callable[[], Awaitable[tuple[int, int, Any, Any]]],
) -> BehaviorCheck:
    async def check() -> None:
        from_version, to_version, first, second = await migrate()
        if to_version <= from_version:
            raise AssertionError("state migration did not advance the schema")
        if canonical(first) != canonical(second):
            raise AssertionError("state migration is not deterministic")

    return check


class BehavioralConformanceSuite:
    async def run(
        self,
        *,
        connector_id: str,
        connector_version: str,
        fixture: BehavioralFixture,
    ) -> ConformanceReport:
        checks: list[ConformanceCheck] = []
        unknown = set(fixture.checks) - set(REQUIRED_BEHAVIORS)
        if unknown:
            raise ValueError(f"unknown behavioral checks: {tuple(sorted(unknown))}")
        for name in REQUIRED_BEHAVIORS:
            operation = fixture.checks.get(name)
            if operation is None:
                checks.append(
                    ConformanceCheck(
                        name=f"behavior.{name}",
                        status=ConformanceStatus.FAILED,
                        message="required behavioral evidence is missing",
                    )
                )
                continue
            try:
                await operation()
            except Exception as exc:  # noqa: BLE001 - evidence boundary
                checks.append(
                    ConformanceCheck(
                        name=f"behavior.{name}",
                        status=ConformanceStatus.FAILED,
                        message=f"{type(exc).__name__}: {exc}",
                    )
                )
            else:
                checks.append(
                    ConformanceCheck(
                        name=f"behavior.{name}",
                        status=ConformanceStatus.PASSED,
                        message="behavioral fixture passed",
                    )
                )
        fingerprint = hashlib.sha256(
            canonical(
                {
                    "suite_version": BEHAVIORAL_SUITE_VERSION,
                    "connector_id": connector_id,
                    "connector_version": connector_version,
                    "checks": [item.model_dump(mode="json") for item in checks],
                }
            ).encode()
        ).hexdigest()
        return ConformanceReport(
            suite_version=BEHAVIORAL_SUITE_VERSION,
            connector_id=connector_id,
            connector_version=connector_version,
            fingerprint=fingerprint,
            checks=tuple(checks),
        )


__all__ = [
    "BEHAVIORAL_SUITE_VERSION",
    "REQUIRED_BEHAVIORS",
    "BehavioralConformanceSuite",
    "BehavioralFixture",
    "PageEvidence",
    "assert_cursor_monotonicity",
    "assert_pagination",
    "canonical",
    "cleanup_idempotency_check",
    "lifecycle_sequence_check",
    "retry_classification_check",
    "stable_operation_check",
    "state_migration_check",
    "webhook_verification_check",
]
