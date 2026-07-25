"""Fail-closed validation for a production source-certification manifest.

This module deliberately depends only on the standard library and the
dependency-light source/certification catalogs. Production promotion can
therefore verify a downloaded artifact without installing Fyralis runtime
dependencies.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from pathlib import Path

from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_certification.evaluator import canonical_json
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


_FULL_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_TOP_LEVEL_FIELDS = frozenset(
    {
        "manifest_version",
        "state",
        "evaluated_at",
        "commit_sha",
        "required_sources",
        "passed_sources",
        "missing_sources",
        "failures",
        "sources",
        "legacy_ratchet_clean",
        "signature",
    }
)
_SIGNATURE_FIELDS = frozenset({"sha256", "hmac_sha256"})
_SCENARIO_RESULT_FIELDS = frozenset(
    {"scenario_id", "state", "artifact_uri", "failures"}
)
_CANARY_OPERATION_RESULT_FIELDS = frozenset(
    {"operation_id", "state", "artifact_uri", "failures"}
)


class PromotionManifestError(ValueError):
    """A certification manifest is unsafe for production promotion."""


def validate_commit_sha(value: object, *, field: str = "commit_sha") -> str:
    """Return a validated lowercase, full-length Git SHA."""

    if not isinstance(value, str) or _FULL_GIT_SHA.fullmatch(value) is None:
        raise PromotionManifestError(
            f"{field} must be a lowercase 40-character hexadecimal commit SHA"
        )
    return value


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise PromotionManifestError(f"{field} must be an object")
    if not all(isinstance(key, str) for key in value):
        raise PromotionManifestError(f"{field} keys must be strings")
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
    unknown = sorted(actual - expected)
    details: list[str] = []
    if missing:
        details.append("missing " + ", ".join(missing))
    if unknown:
        details.append("unknown " + ", ".join(unknown))
    raise PromotionManifestError(f"{field} fields are invalid: {'; '.join(details)}")


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PromotionManifestError(f"{field} must be an integer")
    return value


def _verify_signature(
    manifest: Mapping[str, object],
    *,
    signing_key: bytes,
) -> str:
    if not signing_key:
        raise PromotionManifestError("manifest signing key must not be empty")
    signature = _mapping(manifest.get("signature"), "signature")
    _exact_fields(signature, _SIGNATURE_FIELDS, "signature")
    content_sha = signature.get("sha256")
    keyed_sha = signature.get("hmac_sha256")
    if (
        not isinstance(content_sha, str)
        or _HEX_SHA256.fullmatch(content_sha) is None
        or not isinstance(keyed_sha, str)
        or _HEX_SHA256.fullmatch(keyed_sha) is None
    ):
        raise PromotionManifestError(
            "signature values must be lowercase SHA-256 hex digests"
        )

    unsigned = dict(manifest)
    del unsigned["signature"]
    body = canonical_json(unsigned)
    expected_content_sha = hashlib.sha256(body).hexdigest()
    expected_keyed_sha = hmac.new(signing_key, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(content_sha, expected_content_sha):
        raise PromotionManifestError("manifest content digest verification failed")
    if not hmac.compare_digest(keyed_sha, expected_keyed_sha):
        raise PromotionManifestError("manifest HMAC verification failed")
    return content_sha


def _validate_result_coverage(
    value: object,
    *,
    expected_ids: tuple[str, ...],
    identity_field: str,
    expected_fields: frozenset[str],
    field: str,
) -> None:
    if not isinstance(value, list):
        raise PromotionManifestError(f"{field} must be an array")
    actual_ids: list[str] = []
    for index, raw_result in enumerate(value):
        result_field = f"{field}[{index}]"
        result = _mapping(raw_result, result_field)
        _exact_fields(result, expected_fields, result_field)
        result_id = result.get(identity_field)
        if not isinstance(result_id, str) or not result_id:
            raise PromotionManifestError(
                f"{result_field}.{identity_field} must be a non-empty string"
            )
        actual_ids.append(result_id)
        if result.get("state") != "passed":
            raise PromotionManifestError(f"{result_field}.state must equal passed")
        artifact_uri = result.get("artifact_uri")
        if not isinstance(artifact_uri, str) or not artifact_uri.strip():
            raise PromotionManifestError(
                f"{result_field}.artifact_uri must be non-empty"
            )
        if result.get("failures") != []:
            raise PromotionManifestError(f"{result_field}.failures must be empty")
    if (
        len(actual_ids) != len(expected_ids)
        or len(actual_ids) != len(set(actual_ids))
        or set(actual_ids) != set(expected_ids)
    ):
        raise PromotionManifestError(
            f"{field} must cover exactly the declared IDs without "
            "missing, extra, or duplicate results"
        )


def _validate_source_artifact(value: object, *, index: int) -> str:
    field = f"sources[{index}]"
    artifact = _mapping(value, field)
    source_id = artifact.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        raise PromotionManifestError(f"{field}.source_id must be a non-empty string")
    spec = SOURCE_CERTIFICATION_CATALOG.get(source_id)
    if spec is None:
        raise PromotionManifestError(
            f"{field}.source_id is not a canonical certification source"
        )
    if artifact.get("state") != "passed":
        raise PromotionManifestError(f"{field}.state must equal passed")
    if artifact.get("failures") != []:
        raise PromotionManifestError(f"{field}.failures must be empty")

    supplied = _mapping(artifact.get("input"), f"{field}.input")
    if _integer(
        supplied.get("legacy_reference_count"),
        f"{field}.input.legacy_reference_count",
    ) != 0:
        raise PromotionManifestError(
            f"{field}.input.legacy_reference_count must equal 0"
        )
    if supplied.get("skipped_tests") != []:
        raise PromotionManifestError(f"{field}.input.skipped_tests must be empty")
    if supplied.get("todos") != []:
        raise PromotionManifestError(f"{field}.input.todos must be empty")
    if supplied.get("local_correctness") != "passed":
        raise PromotionManifestError(
            f"{field}.input.local_correctness must equal passed"
        )
    _validate_result_coverage(
        supplied.get("scenario_results"),
        expected_ids=spec.required_scenarios,
        identity_field="scenario_id",
        expected_fields=_SCENARIO_RESULT_FIELDS,
        field=f"{field}.input.scenario_results",
    )
    canary = _mapping(supplied.get("canary"), f"{field}.input.canary")
    if canary.get("state") != "passed":
        raise PromotionManifestError(f"{field}.input.canary.state must equal passed")
    if canary.get("account_type") != spec.canary.account_type:
        raise PromotionManifestError(
            f"{field}.input.canary.account_type differs from certification spec"
        )
    _validate_result_coverage(
        canary.get("operation_results"),
        expected_ids=spec.canary.required_operations,
        identity_field="operation_id",
        expected_fields=_CANARY_OPERATION_RESULT_FIELDS,
        field=f"{field}.input.canary.operation_results",
    )
    return source_id


def validate_promotion_manifest(
    value: object,
    *,
    target_sha: str,
    signing_key: bytes,
) -> dict[str, object]:
    """Authenticate and validate one exact 27-source promotion manifest."""

    expected_sha = validate_commit_sha(target_sha, field="target_sha")
    manifest = _mapping(value, "manifest")
    _exact_fields(manifest, _TOP_LEVEL_FIELDS, "manifest")

    # Authenticate the complete persisted payload before trusting its release
    # decision or any nested evidence.
    content_sha = _verify_signature(manifest, signing_key=signing_key)

    if _integer(manifest.get("manifest_version"), "manifest_version") != 2:
        raise PromotionManifestError("manifest_version must equal 2")
    commit_sha = validate_commit_sha(manifest.get("commit_sha"))
    if not hmac.compare_digest(commit_sha, expected_sha):
        raise PromotionManifestError(
            f"manifest commit SHA {commit_sha} does not match target {expected_sha}"
        )
    if manifest.get("state") != "passed":
        raise PromotionManifestError("manifest state must equal passed")

    required_count = len(CANONICAL_SOURCE_IDS)
    if _integer(manifest.get("required_sources"), "required_sources") != required_count:
        raise PromotionManifestError(
            f"required_sources must equal {required_count}"
        )
    if _integer(manifest.get("passed_sources"), "passed_sources") != required_count:
        raise PromotionManifestError(f"passed_sources must equal {required_count}")
    if manifest.get("missing_sources") != []:
        raise PromotionManifestError("missing_sources must be empty")
    if manifest.get("failures") != {}:
        raise PromotionManifestError("failures must be empty")
    if manifest.get("legacy_ratchet_clean") is not True:
        raise PromotionManifestError("legacy_ratchet_clean must be true")

    sources = manifest.get("sources")
    if not isinstance(sources, list):
        raise PromotionManifestError("sources must be an array")
    source_ids = tuple(
        _validate_source_artifact(item, index=index)
        for index, item in enumerate(sources)
    )
    if source_ids != CANONICAL_SOURCE_IDS:
        raise PromotionManifestError(
            "sources must contain each canonical source exactly once in catalog order"
        )

    return {
        "state": "passed",
        "commit_sha": commit_sha,
        "verified_sources": required_count,
        "legacy_ratchet_clean": True,
        "manifest_sha256": content_sha,
    }


def _reject_duplicate_keys(
    pairs: list[tuple[str, object]],
) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotionManifestError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def load_promotion_manifest(path: Path) -> object:
    """Load JSON while rejecting duplicate object keys and invalid documents."""

    try:
        rendered = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PromotionManifestError(
            f"cannot read certification manifest {path}: {exc}"
        ) from exc
    try:
        return json.loads(rendered, object_pairs_hook=_reject_duplicate_keys)
    except PromotionManifestError:
        raise
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise PromotionManifestError(
            f"certification manifest is not valid UTF-8 JSON: {exc}"
        ) from exc


def validate_promotion_manifest_file(
    path: Path,
    *,
    target_sha: str,
    signing_key: bytes,
) -> dict[str, object]:
    """Load, authenticate, and validate a persisted promotion manifest."""

    return validate_promotion_manifest(
        load_promotion_manifest(path),
        target_sha=target_sha,
        signing_key=signing_key,
    )


__all__ = [
    "PromotionManifestError",
    "load_promotion_manifest",
    "validate_commit_sha",
    "validate_promotion_manifest",
    "validate_promotion_manifest_file",
]
