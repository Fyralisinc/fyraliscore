"""Versioned, fail-closed provider evidence packs.

The checked-in JSON files are inventories of evidence that still needs to be
locked, not assertions that a provider has been certified.  A pack remains
unready while any evidence item lacks ``verified_at`` or while the used API
surface lacks a schema checksum.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Mapping as TypingMapping

from services.ingest.source_certification.models import EvidenceReference
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


EVIDENCE_PACK_SCHEMA_VERSION = 1
EVIDENCE_PACK_DIRECTORY = Path(__file__).with_name("evidence")
SURFACE_ARTIFACT_DIRECTORY = Path(__file__).with_name("surfaces")
_TOP_LEVEL_FIELDS = frozenset(
    {
        "schema_version",
        "source_id",
        "pack_version",
        "provider_api_version",
        "evidence",
    }
)
_EVIDENCE_FIELDS = frozenset(
    {
        "behavior_id",
        "kind",
        "uri",
        "api_version",
        "schema_sha256",
        "quota_uri",
        "verified_at",
        "notes",
    }
)
_REQUIRED_BEHAVIORS = frozenset(
    {
        "used_api_surface",
        "quota_policy",
        "fyralis_runtime_contract",
    }
)
_HTTP_METHODS = frozenset(
    {"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"}
)


class EvidencePackError(ValueError):
    """A checked-in evidence pack is missing, malformed, or inconsistent."""


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise EvidencePackError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise EvidencePackError(f"{field} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise EvidencePackError(f"{field} keys must be strings")
    return value


def _exact_fields(
    value: Mapping[str, object],
    expected: frozenset[str],
    field: str,
) -> None:
    actual = frozenset(value)
    if actual == expected:
        return
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    details: list[str] = []
    if missing:
        details.append("missing " + ", ".join(missing))
    if extra:
        details.append("unknown " + ", ".join(extra))
    raise EvidencePackError(f"{field} fields are invalid: {'; '.join(details)}")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EvidencePackError(f"{field} must be a non-empty string")
    return value.strip()


def _optional_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field)


def _timestamp(value: object, field: str) -> datetime | None:
    if value is None:
        return None
    rendered = _text(value, field)
    try:
        parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidencePackError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvidencePackError(f"{field} must include a timezone")
    return parsed


@dataclass(frozen=True, slots=True)
class EvidencePack:
    """One source's immutable provider-behavior evidence inventory."""

    source_id: str
    pack_version: str
    provider_api_version: str
    evidence: tuple[EvidenceReference, ...]
    content_sha256: str
    path: Path

    @property
    def ready(self) -> bool:
        surface = next(
            (
                reference
                for reference in self.evidence
                if reference.behavior_id == "used_api_surface"
            ),
            None,
        )
        return (
            surface is not None
            and surface.schema_sha256 is not None
            and all(reference.verified for reference in self.evidence)
        )


def _parse_reference(
    value: object,
    *,
    source_id: str,
    index: int,
) -> EvidenceReference:
    field = f"{source_id}.evidence[{index}]"
    item = _mapping(value, field)
    _exact_fields(item, _EVIDENCE_FIELDS, field)
    kind = _text(item["kind"], f"{field}.kind")
    if kind not in {"documented", "observed_live", "fyralis_specific"}:
        raise EvidencePackError(f"{field}.kind is invalid")
    return EvidenceReference(
        behavior_id=_text(item["behavior_id"], f"{field}.behavior_id"),
        kind=kind,  # type: ignore[arg-type]
        uri=_text(item["uri"], f"{field}.uri"),
        api_version=_optional_text(
            item["api_version"],
            f"{field}.api_version",
        ),
        schema_sha256=_optional_text(
            item["schema_sha256"],
            f"{field}.schema_sha256",
        ),
        quota_uri=_optional_text(
            item["quota_uri"],
            f"{field}.quota_uri",
        ),
        verified_at=_timestamp(
            item["verified_at"],
            f"{field}.verified_at",
        ),
        notes=(
            ""
            if item["notes"] is None
            else _text(item["notes"], f"{field}.notes")
        ),
    )


def load_evidence_pack(
    source_id: str,
    *,
    directory: Path = EVIDENCE_PACK_DIRECTORY,
    surface_directory: Path = SURFACE_ARTIFACT_DIRECTORY,
) -> EvidencePack:
    """Load and strictly validate one canonical source evidence pack."""

    if source_id not in CANONICAL_SOURCE_IDS:
        raise EvidencePackError(f"unknown canonical source {source_id!r}")
    path = directory / f"{source_id}.json"
    try:
        rendered = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise EvidencePackError(
            f"cannot read evidence pack for {source_id!r}: {exc}"
        ) from exc
    try:
        raw = json.loads(rendered, object_pairs_hook=_reject_duplicate_keys)
    except EvidencePackError:
        raise
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise EvidencePackError(
            f"evidence pack for {source_id!r} is not valid UTF-8 JSON: {exc}"
        ) from exc
    item = _mapping(raw, source_id)
    _exact_fields(item, _TOP_LEVEL_FIELDS, source_id)
    if item["schema_version"] != EVIDENCE_PACK_SCHEMA_VERSION:
        raise EvidencePackError(
            f"{source_id}.schema_version must equal "
            f"{EVIDENCE_PACK_SCHEMA_VERSION}"
        )
    declared_source = _text(item["source_id"], f"{source_id}.source_id")
    if declared_source != source_id:
        raise EvidencePackError(
            f"evidence pack {path.name!r} declares source "
            f"{declared_source!r}"
        )
    pack_version = _text(item["pack_version"], f"{source_id}.pack_version")
    provider_api_version = _text(
        item["provider_api_version"],
        f"{source_id}.provider_api_version",
    )
    raw_evidence = item["evidence"]
    if not isinstance(raw_evidence, list) or not raw_evidence:
        raise EvidencePackError(f"{source_id}.evidence must be a non-empty array")
    evidence = tuple(
        _parse_reference(value, source_id=source_id, index=index)
        for index, value in enumerate(raw_evidence)
    )
    behavior_ids = tuple(reference.behavior_id for reference in evidence)
    if len(behavior_ids) != len(set(behavior_ids)):
        raise EvidencePackError(
            f"{source_id}.evidence contains duplicate behavior IDs"
        )
    if frozenset(behavior_ids) != _REQUIRED_BEHAVIORS:
        raise EvidencePackError(
            f"{source_id}.evidence must contain exactly "
            f"{sorted(_REQUIRED_BEHAVIORS)!r}"
        )
    surface = next(
        reference
        for reference in evidence
        if reference.behavior_id == "used_api_surface"
    )
    if surface.api_version != provider_api_version:
        raise EvidencePackError(
            f"{source_id} used API surface version differs from pack version"
        )
    if surface.schema_sha256 is not None:
        surface_path = surface_directory / f"{source_id}.json"
        try:
            actual_surface_sha256 = hashlib.sha256(
                surface_path.read_bytes(),
            ).hexdigest()
        except OSError as exc:
            raise EvidencePackError(
                f"cannot read pinned used API surface for {source_id!r}: {exc}"
            ) from exc
        if actual_surface_sha256 != surface.schema_sha256:
            raise EvidencePackError(
                f"{source_id} used API surface checksum differs from "
                f"{surface_path.name!r}: expected {surface.schema_sha256}, "
                f"got {actual_surface_sha256}"
            )
    canonical = json.dumps(
        raw,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return EvidencePack(
        source_id=source_id,
        pack_version=pack_version,
        provider_api_version=provider_api_version,
        evidence=evidence,
        content_sha256=hashlib.sha256(canonical).hexdigest(),
        path=path,
    )


def load_surface_operation_methods(
    source_id: str,
    *,
    directory: Path = SURFACE_ARTIFACT_DIRECTORY,
) -> TypingMapping[str, str]:
    """Load the hash-pinned exact HTTP method for each provider operation.

    Older or incomplete surface bundles yield no inferred method for missing
    bindings. The canary catalog treats those operations as unclassified, so
    stale generation fails closed without importing the production-guarded
    synthetic Provider Lab package.
    """

    if source_id not in CANONICAL_SOURCE_IDS:
        raise EvidencePackError(f"unknown canonical source {source_id!r}")
    path = directory / f"{source_id}.json"
    try:
        raw = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except EvidencePackError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EvidencePackError(
            f"cannot load used API surface for {source_id!r}: {exc}"
        ) from exc
    surface = _mapping(raw, f"{source_id}.surface")
    if surface.get("source_id") != source_id:
        raise EvidencePackError(
            f"used API surface identity differs for {source_id!r}"
        )
    provider_lab = _mapping(
        surface.get("provider_lab"),
        f"{source_id}.surface.provider_lab",
    )
    routes = provider_lab.get("routes")
    if not isinstance(routes, list):
        raise EvidencePackError(
            f"{source_id}.surface.provider_lab.routes must be an array"
        )
    methods: dict[str, str] = {}
    for route_index, route_raw in enumerate(routes):
        route = _mapping(
            route_raw,
            f"{source_id}.surface.provider_lab.routes[{route_index}]",
        )
        bindings = route.get("operation_bindings", [])
        if not isinstance(bindings, list):
            raise EvidencePackError(
                f"{source_id}.surface route operation_bindings must be an array"
            )
        for binding_index, binding_raw in enumerate(bindings):
            binding = _mapping(
                binding_raw,
                (
                    f"{source_id}.surface.provider_lab.routes[{route_index}]"
                    f".operation_bindings[{binding_index}]"
                ),
            )
            operation_id = _text(
                binding.get("operation_id"),
                f"{source_id}.surface operation_id",
            )
            method = _text(
                binding.get("method"),
                f"{source_id}.surface operation method",
            ).upper()
            if method not in _HTTP_METHODS:
                raise EvidencePackError(
                    f"{source_id} surface has unsupported HTTP method {method!r}"
                )
            if operation_id in methods:
                raise EvidencePackError(
                    f"{source_id} surface duplicates operation {operation_id!r}"
                )
            methods[operation_id] = method
    return MappingProxyType(methods)


def load_evidence_catalog(
    *,
    directory: Path = EVIDENCE_PACK_DIRECTORY,
    surface_directory: Path = SURFACE_ARTIFACT_DIRECTORY,
) -> Mapping[str, EvidencePack]:
    """Load exactly one evidence pack for every canonical source."""

    try:
        actual_files = {path.name for path in directory.glob("*.json")}
    except OSError as exc:
        raise EvidencePackError(f"cannot list evidence pack directory: {exc}") from exc
    expected_files = {f"{source_id}.json" for source_id in CANONICAL_SOURCE_IDS}
    if actual_files != expected_files:
        missing = sorted(expected_files - actual_files)
        extra = sorted(actual_files - expected_files)
        details: list[str] = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if extra:
            details.append("unknown " + ", ".join(extra))
        raise EvidencePackError(
            "evidence pack membership differs from the canonical catalog: "
            + "; ".join(details)
        )
    return MappingProxyType(
        {
            source_id: load_evidence_pack(
                source_id,
                directory=directory,
                surface_directory=surface_directory,
            )
            for source_id in CANONICAL_SOURCE_IDS
        }
    )


__all__ = [
    "EVIDENCE_PACK_DIRECTORY",
    "EVIDENCE_PACK_SCHEMA_VERSION",
    "SURFACE_ARTIFACT_DIRECTORY",
    "EvidencePack",
    "EvidencePackError",
    "load_evidence_catalog",
    "load_evidence_pack",
    "load_surface_operation_methods",
]
