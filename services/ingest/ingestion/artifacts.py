"""Durable, tenant-scoped artifact references for ingestion observations.

The raw tier is intentionally short-lived and contains transport envelopes.
This module is the separate durable tier for source documents that must remain
retrievable after ingestion.  It keeps the public observation contract small:
observations contain a ``blob_id`` and safe integrity metadata, while the
private bucket/key live in the tenant-scoped ``blobs`` catalog.

The write ordering for a fetched design is:

    Figma JSON -> S3 PutIfAbsent -> raw backfill envelope -> observation tx

An S3 object may therefore briefly be unreferenced if downstream ingestion
fails.  It is content-addressed and safe to reuse; a retention/GC job can
remove unreferenced objects later.  An observation is never committed unless
its catalog row and observation_artifacts link commit in the same transaction.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Iterable
from uuid import UUID, uuid5

import asyncpg
import orjson

from services.ingest.ingestion.raw_tier.s3 import S3Client, compute_content_hash


_BLOB_NAMESPACE = UUID("cc546d3c-3184-4cc5-b74d-a0e6fd9a825d")
_SOURCE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_HASH_PREFIX = "blake2b:"
_DEFAULT_BUCKET = "fyralis-blobs"
_BUCKET_ALIAS = "durable-artifacts"


class ArtifactDescriptorError(ValueError):
    """A fetched record supplied an invalid private artifact descriptor."""


def _hash_with_algorithm(content_hash: str) -> str:
    return (
        content_hash
        if content_hash.startswith(_HASH_PREFIX)
        else f"{_HASH_PREFIX}{content_hash}"
    )


def _hash_without_algorithm(content_hash: str) -> str:
    return content_hash.removeprefix(_HASH_PREFIX)


def durable_blob_bucket() -> str:
    """Configured durable bucket, intentionally never exposed in content."""
    return os.environ.get("S3_BLOB_BUCKET", _DEFAULT_BUCKET)


def build_blob_s3_key(
    *,
    tenant_id: UUID | str,
    source: str,
    content_hash: str,
    env: str | None = None,
) -> str:
    """Return the opaque durable-object key for a content-addressed artifact."""
    if not _SOURCE_RE.fullmatch(source):
        raise ValueError(f"invalid artifact source {source!r}")
    raw_hash = _hash_without_algorithm(content_hash)
    if len(raw_hash) < 2:
        raise ValueError("content_hash must contain at least two characters")
    environment = env or os.environ.get("FYRALIS_ENV") or "local"
    if not environment:
        raise ValueError("artifact environment is required")
    return (
        f"{environment}/artifacts/{source}/{tenant_id}/{raw_hash[:2]}/"
        f"{raw_hash}.json"
    )


def blob_id_for(*, tenant_id: UUID | str, content_hash: str) -> UUID:
    """Stable id: retries of byte-identical tenant content share one blob."""
    return uuid5(_BLOB_NAMESPACE, f"{tenant_id}:{_hash_with_algorithm(content_hash)}")


def artifact_object_metadata(*, content_hash: str) -> dict[str, str]:
    return {
        "fyralis-content-hash": _hash_with_algorithm(content_hash),
        "fyralis-data-class": "durable-artifact",
    }


@dataclass(frozen=True)
class StoredArtifact:
    """Private catalog descriptor for one already-durable object.

    ``private_descriptor`` travels on the normalized Kafka message so the
    writer can create the catalog/link transaction.  It is deliberately not
    copied into observations.content.  ``public_ref`` is the safe content
    shape visible to the product/API.
    """

    blob_id: UUID
    kind: str
    storage_provider: str
    bucket: str
    object_key: str
    content_hash: str
    content_type: str
    size_bytes: int
    content_encoding: str | None = None

    def public_ref(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "blob_id": str(self.blob_id),
            "content_type": self.content_type,
            "content_encoding": self.content_encoding,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
        }

    def private_descriptor(self) -> dict[str, Any]:
        return {
            **self.public_ref(),
            "storage_provider": self.storage_provider,
            "bucket": self.bucket,
            "bucket_alias": _BUCKET_ALIAS,
            "object_key": self.object_key,
        }

    @classmethod
    def from_private_descriptor(cls, value: dict[str, Any]) -> "StoredArtifact":
        try:
            blob_id = UUID(str(value["blob_id"]))
            kind = str(value["kind"])
            storage_provider = str(value["storage_provider"])
            bucket = str(value["bucket"])
            object_key = str(value["object_key"])
            content_hash = _hash_with_algorithm(str(value["content_hash"]))
            content_type = str(value["content_type"])
            size_bytes = int(value["size_bytes"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactDescriptorError("artifact descriptor is incomplete") from exc
        encoding_raw = value.get("content_encoding")
        content_encoding = str(encoding_raw) if encoding_raw is not None else None
        if (
            not kind
            or storage_provider != "s3"
            or not bucket
            or not object_key
            or not content_type
            or size_bytes < 0
        ):
            raise ArtifactDescriptorError("artifact descriptor has invalid values")
        return cls(
            blob_id=blob_id,
            kind=kind,
            storage_provider=storage_provider,
            bucket=bucket,
            object_key=object_key,
            content_hash=content_hash,
            content_type=content_type,
            size_bytes=size_bytes,
            content_encoding=content_encoding,
        )


async def store_json_artifact(
    payload: dict[str, Any],
    *,
    tenant_id: UUID | str,
    source: str,
    kind: str,
    s3_client: S3Client | None = None,
) -> StoredArtifact:
    """Serialize one JSON document deterministically and store it durably.

    Callers can inject an ``S3Client`` in tests.  The normal production path
    owns and closes the client for this operation.
    """
    if not isinstance(payload, dict):
        raise TypeError("durable JSON artifact payload must be an object")
    body = orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    raw_hash = compute_content_hash(body)
    content_hash = _hash_with_algorithm(raw_hash)
    key = build_blob_s3_key(
        tenant_id=tenant_id,
        source=source,
        content_hash=content_hash,
    )
    bucket = durable_blob_bucket()
    owned = s3_client is None
    client = s3_client or S3Client(
        bucket,
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        region_name=os.environ.get("S3_REGION_NAME", "auto"),
    )
    try:
        await client.put_if_absent(
            key,
            body,
            content_type="application/json",
            metadata=artifact_object_metadata(content_hash=content_hash),
            tagging="fyralis-data-class=durable-artifact",
        )
    finally:
        if owned:
            await client.close()
    return StoredArtifact(
        blob_id=blob_id_for(tenant_id=tenant_id, content_hash=content_hash),
        kind=kind,
        storage_provider="s3",
        bucket=bucket,
        object_key=key,
        content_hash=content_hash,
        content_type="application/json",
        size_bytes=len(body),
    )


async def persist_observation_artifacts(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    descriptors: Iterable[dict[str, Any]],
) -> None:
    """Upsert catalog rows and links inside the caller's observation tx.

    The ``blobs`` unique key is ``(tenant_id, content_hash)``.  The blob id is
    deterministic from that same tuple, so retries and concurrent workers
    agree on a single catalog row before the observation link is inserted.
    """
    for raw in descriptors:
        if not isinstance(raw, dict):
            raise ArtifactDescriptorError("artifact descriptor must be an object")
        artifact = StoredArtifact.from_private_descriptor(raw)
        expected_id = blob_id_for(
            tenant_id=tenant_id,
            content_hash=artifact.content_hash,
        )
        if artifact.blob_id != expected_id:
            raise ArtifactDescriptorError("artifact blob_id does not match tenant/hash")
        blob_id = await conn.fetchval(
            """
            INSERT INTO blobs (
                id, tenant_id, storage_provider, bucket, object_key,
                content_hash, content_type, content_encoding, size_bytes, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'ready')
            ON CONFLICT (tenant_id, content_hash) DO UPDATE
              SET updated_at = now(),
                  status = 'ready'
            RETURNING id
            """,
            artifact.blob_id,
            tenant_id,
            artifact.storage_provider,
            artifact.bucket,
            artifact.object_key,
            artifact.content_hash,
            artifact.content_type,
            artifact.content_encoding,
            artifact.size_bytes,
        )
        if blob_id is None:
            raise ArtifactDescriptorError("catalog upsert did not return a blob id")
        await conn.execute(
            """
            INSERT INTO observation_artifacts (
                tenant_id, observation_id, blob_id, artifact_kind
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, observation_id, blob_id, artifact_kind)
            DO NOTHING
            """,
            tenant_id,
            observation_id,
            blob_id,
            artifact.kind,
        )


async def update_figma_snapshot_watermark(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    content: dict[str, Any],
    descriptors: Iterable[dict[str, Any]],
) -> None:
    """Advance a file's snapshot high-water mark in the observation tx.

    It is intentionally a no-op for all non-Figma drafts.  The update happens
    *after* the blob catalog/link upsert, so a version is never marked current
    unless the durable object has an observation reference.  Replays/deduped
    observations repeat the same update safely.
    """
    if content.get("object_type") != "figma_file_snapshot":
        return
    locator = content.get("source_locator")
    if not isinstance(locator, dict):
        return
    installation_raw = locator.get("installation_id")
    file_key = locator.get("file_key") or content.get("file_key")
    version = locator.get("version") or content.get("figma_version")
    if not isinstance(file_key, str) or not file_key or version is None:
        return
    try:
        installation_id = UUID(str(installation_raw))
    except (TypeError, ValueError):
        return
    descriptor_list = list(descriptors)
    if not descriptor_list:
        return
    first = descriptor_list[0]
    try:
        blob_id = UUID(str(first["blob_id"]))
    except (KeyError, TypeError, ValueError):
        return
    await conn.execute(
        """
        UPDATE figma_files
           SET snapshot_version = $1,
               snapshot_blob_id = $2,
               last_snapshot_at = now(),
               last_synced_at = now(),
               last_error = NULL
         WHERE tenant_id = $3
           AND figma_installation_id = $4
           AND file_key = $5
        """,
        str(version),
        blob_id,
        tenant_id,
        installation_id,
        file_key,
    )
    # OAuth onboarding remains pending until the first selected design has
    # reached the durable artifact + observation transaction.  This is stronger
    # than treating an OAuth callback as connected: the user can immediately
    # see a real Figma observation after this state becomes visible.
    await conn.execute(
        """
        UPDATE figma_installations
           SET connection_state = 'connected',
               last_error = NULL,
               connected_at = COALESCE(connected_at, now())
         WHERE id = $1 AND tenant_id = $2 AND disabled_at IS NULL
        """,
        installation_id,
        tenant_id,
    )


__all__ = [
    "ArtifactDescriptorError",
    "StoredArtifact",
    "artifact_object_metadata",
    "blob_id_for",
    "build_blob_s3_key",
    "durable_blob_bucket",
    "persist_observation_artifacts",
    "store_json_artifact",
    "update_figma_snapshot_watermark",
]
