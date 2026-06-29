from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.verify_object_restore_manifest import (
    ObjectRestoreManifestError,
    verify_manifest,
)


def _write_manifest(path: Path, *, location: str, payload: bytes) -> None:
    path.write_text(
        json.dumps(
            {
                "objects": [
                    {
                        "location": location,
                        "sha256": hashlib.sha256(payload).hexdigest(),
                        "size_bytes": len(payload),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )


def test_verify_manifest_accepts_matching_local_file(tmp_path: Path) -> None:
    restored = tmp_path / "restored-object.bin"
    payload = b"restored raw object sample"
    restored.write_bytes(payload)
    manifest = tmp_path / "manifest.json"
    _write_manifest(manifest, location=str(restored), payload=payload)

    assert verify_manifest(manifest) == {
        "ok": True,
        "objects_checked": 1,
        "bytes_checked": len(payload),
    }


def test_verify_manifest_detects_hash_mismatch_without_leaking_location(
    tmp_path: Path,
) -> None:
    restored = tmp_path / "sensitive-tenant-object.bin"
    restored.write_bytes(b"0123456789")
    manifest = tmp_path / "manifest.json"
    _write_manifest(manifest, location=str(restored), payload=b"abcdefghij")

    with pytest.raises(ObjectRestoreManifestError) as exc:
        verify_manifest(manifest)

    message = str(exc.value)
    assert "sha256 mismatch" in message
    assert "sensitive-tenant-object" not in message


def test_verify_manifest_rejects_empty_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"objects":[]}', encoding="utf-8")

    with pytest.raises(ObjectRestoreManifestError, match="non-empty list"):
        verify_manifest(manifest)
