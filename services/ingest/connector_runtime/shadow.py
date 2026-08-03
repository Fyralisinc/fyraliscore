"""Non-authoritative connector/legacy parity comparison."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from pydantic import BaseModel


class ShadowDimension(StrEnum):
    IDENTITY = "identity"
    NORMALIZATION = "normalization"
    PUBLICATION = "publication"
    CURSOR = "cursor"
    STATE = "state"


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def canonical_fingerprint(value: Any) -> str:
    payload = json.dumps(
        _jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class ShadowDifference:
    dimension: ShadowDimension
    matches: bool
    legacy_fingerprint: str
    connector_fingerprint: str


@dataclass(frozen=True)
class ShadowReport:
    connector_id: str
    installation_id: str
    capability: str
    differences: tuple[ShadowDifference, ...]
    connector_error_code: str | None = None

    @property
    def matches(self) -> bool:
        return self.connector_error_code is None and all(
            difference.matches for difference in self.differences
        )


class ShadowReportSink(Protocol):
    def record(self, report: ShadowReport) -> None: ...


class InMemoryShadowReportSink:
    def __init__(self) -> None:
        self.reports: list[ShadowReport] = []

    def record(self, report: ShadowReport) -> None:
        self.reports.append(report)


ShadowProjection = Callable[[Any], Mapping[ShadowDimension, Any]]


def compare_shadow_results(
    legacy: Any,
    connector: Any,
    *,
    projection: ShadowProjection,
) -> tuple[ShadowDifference, ...]:
    legacy_values = projection(legacy)
    connector_values = projection(connector)
    dimensions = tuple(sorted(set(legacy_values) | set(connector_values)))
    result: list[ShadowDifference] = []
    for dimension in dimensions:
        legacy_fingerprint = canonical_fingerprint(legacy_values.get(dimension))
        connector_fingerprint = canonical_fingerprint(
            connector_values.get(dimension)
        )
        result.append(
            ShadowDifference(
                dimension=dimension,
                matches=legacy_fingerprint == connector_fingerprint,
                legacy_fingerprint=legacy_fingerprint,
                connector_fingerprint=connector_fingerprint,
            )
        )
    return tuple(result)


__all__ = [
    "InMemoryShadowReportSink",
    "ShadowDifference",
    "ShadowDimension",
    "ShadowProjection",
    "ShadowReport",
    "ShadowReportSink",
    "canonical_fingerprint",
    "compare_shadow_results",
]
