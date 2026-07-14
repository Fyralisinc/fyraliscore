"""Authenticated retrieval of durable Figma design snapshots.

The observation stores only a safe blob reference.  This router resolves that
reference through tenant-scoped catalog rows, reads the private S3 location on
the server, verifies its hash, and streams JSON bytes back to the caller.  A
bucket name, object key, Figma credential, or presigned URL never crosses the
HTTP boundary.
"""
from __future__ import annotations

import logging
import os
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

from lib.shared.tenant_context import tenant_transaction
from services.ingest.ingestion.raw_tier.s3 import S3Client, compute_content_hash


log = logging.getLogger("integrations.figma.artifacts")

# Included into the Figma OAuth router, which owns the /integrations/figma
# prefix. Keeping this child router prefix-free avoids a second global mount.
router = APIRouter(tags=["figma"])


_LOOKUP_SQL = """
SELECT b.bucket, b.object_key, b.content_hash, b.content_type,
       b.content_encoding, b.size_bytes
  FROM observation_artifacts oa
  JOIN blobs b
    ON b.id = oa.blob_id
   AND b.tenant_id = oa.tenant_id
  JOIN observations o
    ON o.id = oa.observation_id
   AND o.tenant_id = oa.tenant_id
 WHERE oa.tenant_id = $1
   AND oa.observation_id = $2
   AND oa.blob_id = $3
   AND oa.artifact_kind = 'figma_document_json'
   AND b.status = 'ready'
   AND b.content_type LIKE 'application/json%'
   AND b.content_encoding IS NULL
   AND o.source_channel = 'figma:file_snapshot'
   AND o.content ->> 'object_type' = 'figma_file_snapshot'
   -- Prove that the safe public reference on the observation agrees with the
   -- private link row; a link alone must not authorize an arbitrary blob.
   AND o.content @> jsonb_build_object(
     'artifacts', jsonb_build_array(jsonb_build_object(
       'blob_id', $3::text,
       'kind', 'figma_document_json'
     ))
   )
   -- Prove that the snapshot's source installation belongs to this tenant.
   AND EXISTS (
     SELECT 1
       FROM figma_installations fi
      WHERE fi.tenant_id = oa.tenant_id
        AND fi.id::text = (o.content #>> '{source_locator,installation_id}')
   )
 LIMIT 1
"""


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    raw_tenant_id = getattr(auth, "tenant_id", None) if auth is not None else None
    if raw_tenant_id is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    try:
        return raw_tenant_id if isinstance(raw_tenant_id, UUID) else UUID(str(raw_tenant_id))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="unauthenticated") from exc


def _pool_from_request(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="database pool unavailable")
    return pool


def _expected_hash(value: Any) -> str:
    raw = str(value or "")
    return raw.removeprefix("blake2b:")


async def _load_artifact_bytes(
    *,
    bucket: str,
    object_key: str,
    expected_content_hash: str,
) -> bytes:
    """Read and integrity-check a private durable artifact server-side."""
    client = S3Client(
        bucket,
        endpoint_url=os.environ.get("S3_ENDPOINT_URL") or None,
        region_name=os.environ.get("S3_REGION_NAME", "auto"),
    )
    try:
        body = await client.get(object_key)
    finally:
        await client.close()
    actual = compute_content_hash(body)
    if not expected_content_hash or actual != expected_content_hash:
        raise ValueError("artifact content hash mismatch")
    return body


@router.get(
    "/observations/{observation_id}/artifacts/{blob_id}",
    response_class=Response,
    responses={
        401: {"description": "Authentication required"},
        404: {"description": "Figma snapshot artifact not found"},
        502: {"description": "Artifact storage unavailable or invalid"},
    },
)
async def get_figma_snapshot_artifact(
    observation_id: UUID,
    blob_id: UUID,
    request: Request,
) -> Response:
    """Return the complete stored Figma document JSON for this tenant only."""
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    async with tenant_transaction(tenant_id, pool=pool) as conn:
        row = await conn.fetchrow(_LOOKUP_SQL, tenant_id, observation_id, blob_id)
    if row is None:
        # Deliberately indistinguishable for absent, cross-tenant, non-Figma,
        # and detached artifacts.  This prevents a blob/observation oracle.
        raise HTTPException(status_code=404, detail="artifact not found")

    bucket = row["bucket"]
    object_key = row["object_key"]
    expected = _expected_hash(row["content_hash"])
    if not isinstance(bucket, str) or not bucket or not isinstance(object_key, str) or not object_key:
        raise HTTPException(status_code=502, detail="artifact unavailable")
    try:
        body = await _load_artifact_bytes(
            bucket=bucket,
            object_key=object_key,
            expected_content_hash=expected,
        )
    except Exception as exc:  # noqa: BLE001 - storage boundary must not leak internals
        log.warning(
            "figma_artifact_read_failed",
            extra={
                "tenant_id": str(tenant_id),
                "observation_id": str(observation_id),
                "blob_id": str(blob_id),
                "error_type": type(exc).__name__,
            },
        )
        raise HTTPException(status_code=502, detail="artifact unavailable") from exc

    return Response(
        content=body,
        media_type="application/json",
        headers={
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


__all__ = ["get_figma_snapshot_artifact", "router"]
