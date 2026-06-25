#!/usr/bin/env python3
"""Verify restored object samples from a non-secret manifest.

The manifest must contain hashes/sizes for sampled restore objects. This tool
prints only aggregate counts and never emits object keys or payload bytes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
from dataclasses import dataclass
from typing import Any, Sequence
from urllib.parse import urlparse


class ObjectRestoreManifestError(ValueError):
    """Operator-facing manifest or verification error."""


@dataclass(frozen=True)
class ObjectSpec:
    location: str
    sha256: str
    size_bytes: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument(
        "--max-objects",
        type=int,
        default=1000,
        help="Safety cap for one verification run.",
    )
    return parser


def _parse_manifest(path: pathlib.Path, *, max_objects: int) -> list[ObjectSpec]:
    if max_objects <= 0:
        raise ObjectRestoreManifestError("--max-objects must be positive")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ObjectRestoreManifestError("failed to read manifest") from exc
    except json.JSONDecodeError as exc:
        raise ObjectRestoreManifestError("manifest must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ObjectRestoreManifestError("manifest must be a JSON object")
    objects = payload.get("objects")
    if not isinstance(objects, list) or not objects:
        raise ObjectRestoreManifestError("manifest.objects must be a non-empty list")
    if len(objects) > max_objects:
        raise ObjectRestoreManifestError("manifest exceeds --max-objects")

    specs: list[ObjectSpec] = []
    for index, raw in enumerate(objects):
        if not isinstance(raw, dict):
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}] must be an object"
            )
        location = raw.get("location")
        sha256 = raw.get("sha256")
        size_bytes = raw.get("size_bytes")
        if not isinstance(location, str) or not location:
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}].location is required"
            )
        if not isinstance(sha256, str) or len(sha256) != 64:
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}].sha256 must be a hex sha256"
            )
        try:
            int(sha256, 16)
        except ValueError as exc:
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}].sha256 must be hex"
            ) from exc
        if not isinstance(size_bytes, int) or size_bytes < 0:
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}].size_bytes must be a non-negative int"
            )
        specs.append(
            ObjectSpec(location=location, sha256=sha256.lower(), size_bytes=size_bytes)
        )
    return specs


def _read_location(location: str) -> bytes:
    parsed = urlparse(location)
    if parsed.scheme in {"", "file"}:
        path = pathlib.Path(parsed.path if parsed.scheme == "file" else location)
        try:
            return path.read_bytes()
        except OSError as exc:
            raise ObjectRestoreManifestError("failed to read restored object") from exc
    if parsed.scheme == "s3":
        return _read_s3_object(parsed.netloc, parsed.path.lstrip("/"))
    raise ObjectRestoreManifestError("unsupported object location scheme")


def _read_s3_object(bucket: str, key: str) -> bytes:
    if not bucket or not key:
        raise ObjectRestoreManifestError("s3 object location is invalid")
    try:
        import boto3  # type: ignore[import-not-found]
    except ImportError as exc:
        raise ObjectRestoreManifestError(
            "boto3 is required to verify s3:// restored objects"
        ) from exc
    client = boto3.client("s3")
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    except Exception as exc:  # noqa: BLE001
        raise ObjectRestoreManifestError("failed to read restored s3 object") from exc
    return bytes(body)


def verify_manifest(path: pathlib.Path, *, max_objects: int = 1000) -> dict[str, Any]:
    specs = _parse_manifest(path, max_objects=max_objects)
    checked = 0
    total_bytes = 0
    for index, spec in enumerate(specs):
        data = _read_location(spec.location)
        digest = hashlib.sha256(data).hexdigest()
        if len(data) != spec.size_bytes:
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}] restored object size mismatch"
            )
        if digest != spec.sha256:
            raise ObjectRestoreManifestError(
                f"manifest.objects[{index}] restored object sha256 mismatch"
            )
        checked += 1
        total_bytes += len(data)
    return {"ok": True, "objects_checked": checked, "bytes_checked": total_bytes}


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = verify_manifest(args.manifest, max_objects=args.max_objects)
    except ObjectRestoreManifestError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
