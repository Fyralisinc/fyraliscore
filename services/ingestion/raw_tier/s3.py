"""S3 raw-tier client wrapping aioboto3.

Per ingestion LLD §5.1 and HLD §"Raw Tier" (key scheme + envelope).

Two operations matter:
  - PutIfAbsent (S3 `If-None-Match: *`) — write the body only if no
    object exists at the key. Two callers writing the same content
    produce the same content_hash → same key → idempotent. The 412
    PreconditionFailed response is therefore a SUCCESS in our world,
    not an error.
  - Get — pull the body for the normalizer (DLQ replay, reconciler).

The key shape is a pure function (no I/O) so callers and tests can
compute keys without any S3 client.

NOTE — boto3 vs HTTP semantics: at the time of writing, aioboto3's
`put_object` does NOT forward `If-None-Match` as a header by default;
the canonical pattern is to call `put_object(..., IfNoneMatch="*")`
which boto3 maps to the `x-amz-if-none-match` request directive. We
treat both 200 and 412 as success. A few S3-compatible stores
implement `IfNoneMatch` only via the lower-level HTTP API; for
those a small adapter sits over `S3Client.put_if_absent` (out of
scope for M1).
"""
from __future__ import annotations

import hashlib
from datetime import date
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    # aioboto3 client types only matter for static checking; runtime
    # avoids importing the module to keep test-time import light.
    pass


def compute_content_hash(body: bytes) -> str:
    """blake2b-160 hex digest of the raw bytes. Per LLD §5.1.

    blake2b with 20-byte digest size matches the key scheme's
    `{content_hash}` placeholder length. Deterministic across pods.
    """
    if not isinstance(body, (bytes, bytearray)):
        raise TypeError(
            f"compute_content_hash expects bytes, got {type(body).__name__}"
        )
    return hashlib.blake2b(bytes(body), digest_size=20).hexdigest()


def build_raw_s3_key(
    *,
    env: str,
    source: str,
    tenant_id: UUID | str,
    ymd: date,
    content_hash: str,
) -> str:
    """Compute the canonical S3 key for one raw body.

    Per HLD §"Raw Tier":
        s3://fyralis-raw/{env}/{source}/{tenant_id}/{yyyy-mm}/
                        {content_hash[:2]}/{content_hash}.json.zst

    This function returns the KEY (path after the bucket), not the
    full s3:// URL. Callers compose the bucket prefix.

    No I/O. Pure. Tested.
    """
    if not env:
        raise ValueError("env is required")
    if source not in ("slack", "github", "discord", "gmail"):
        raise ValueError(f"unknown source {source!r}")
    if not content_hash:
        raise ValueError("content_hash is required")
    yyyy_mm = ymd.strftime("%Y-%m")
    prefix = content_hash[:2]
    return (
        f"{env}/{source}/{tenant_id}/{yyyy_mm}/{prefix}/"
        f"{content_hash}.json.zst"
    )


class S3Client:
    """Thin async wrapper around aioboto3's S3 client.

    Owns one bucket name + one session. Two callable methods:
    `put_if_absent` and `get`. Stateless beyond the session — safe
    to share across asyncio tasks in one process.

    Construction note: aioboto3.Session.client is an async context
    manager; we hold the entered client for the lifetime of the
    instance. Use the `connect()`/`close()` pair, or
    `async with S3Client(...) as client` if you prefer.
    """

    def __init__(
        self,
        bucket: str,
        *,
        endpoint_url: str | None = None,
        region_name: str = "auto",
    ) -> None:
        self._bucket = bucket
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._session: Any | None = None
        self._cm: Any | None = None
        self._client: Any | None = None

    async def connect(self) -> None:
        if self._client is not None:
            return
        import aioboto3  # lazy import — keeps non-S3 codepaths slim

        self._session = aioboto3.Session()
        self._cm = self._session.client(
            "s3",
            endpoint_url=self._endpoint_url,
            region_name=self._region_name,
        )
        self._client = await self._cm.__aenter__()

    async def close(self) -> None:
        if self._cm is not None:
            await self._cm.__aexit__(None, None, None)
        self._cm = None
        self._client = None
        self._session = None

    async def __aenter__(self) -> "S3Client":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def put_if_absent(self, key: str, body: bytes) -> None:
        """Write `body` at `key` only if the key is currently absent.

        Treats both 200 (created) and 412 PreconditionFailed
        (already existed with same/different content — we don't
        care because the key encodes content_hash) as success.

        Per LLD §5.1.
        """
        await self.connect()
        assert self._client is not None
        from botocore.exceptions import ClientError

        try:
            await self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=body,
                IfNoneMatch="*",
            )
        except ClientError as e:
            # 412 PreconditionFailed → key already exists → idempotent
            # success path. Anything else → propagate.
            code = (e.response or {}).get("Error", {}).get("Code", "")
            if code in ("PreconditionFailed", "412"):
                return
            raise

    async def get(self, key: str) -> bytes:
        """Fetch the raw body at `key`. Raises on miss / error."""
        await self.connect()
        assert self._client is not None
        resp = await self._client.get_object(Bucket=self._bucket, Key=key)
        # aioboto3 streams the body; read it all.
        body_stream = resp["Body"]
        try:
            return await body_stream.read()
        finally:
            close = getattr(body_stream, "close", None)
            if close is not None:
                maybe_coro = close()
                if hasattr(maybe_coro, "__await__"):
                    await maybe_coro


__all__ = ["S3Client", "build_raw_s3_key", "compute_content_hash"]
