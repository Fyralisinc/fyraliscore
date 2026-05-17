"""Tests for the S3 raw-tier client + key builder + content hash.

Pure unit tests for `compute_content_hash` and `build_raw_s3_key`.
S3 wiring tests mock at the aioboto3 client level to avoid needing
moto's S3 server fixture for every test — the M1.4 prompt accepts
either moto or client-level mocking, and grep showed no existing
moto usage in the codebase. Mocking at the aioboto3 client level
matches existing test patterns.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from services.ingestion.raw_tier.s3 import (
    S3Client,
    build_raw_s3_key,
    compute_content_hash,
)


# ---------------------------------------------------------------------
# build_raw_s3_key — pure function.
# ---------------------------------------------------------------------

def test_build_raw_s3_key_format() -> None:
    """The key shape must match LLD §5.1 / HLD §"Raw Tier" exactly.
    Downstream code (replay, reconciler) parses this format.
    """
    tenant_id = UUID("019e34fb-ab3a-7000-9463-5f51662b2be3")
    content_hash = "abcd1234" * 5  # 40 hex chars
    key = build_raw_s3_key(
        env="dev",
        source="slack",
        tenant_id=tenant_id,
        ymd=date(2026, 5, 17),
        content_hash=content_hash,
    )
    assert key == (
        f"dev/slack/{tenant_id}/2026-05/ab/{content_hash}.json.zst"
    )


def test_build_raw_s3_key_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="unknown source"):
        build_raw_s3_key(
            env="dev",
            source="linkedin",  # type: ignore[arg-type]
            tenant_id=UUID("019e34fb-ab3a-7000-9463-5f51662b2be3"),
            ymd=date(2026, 5, 17),
            content_hash="x" * 40,
        )


@pytest.mark.parametrize("source", ["slack", "github", "discord", "gmail"])
def test_build_raw_s3_key_accepts_all_known_sources(source: str) -> None:
    key = build_raw_s3_key(
        env="prod",
        source=source,
        tenant_id="t-1",
        ymd=date(2026, 1, 1),
        content_hash="ab" + "0" * 38,
    )
    assert key.startswith(f"prod/{source}/t-1/2026-01/ab/")


# ---------------------------------------------------------------------
# compute_content_hash — deterministic blake2b-160.
# ---------------------------------------------------------------------

def test_content_hash_deterministic() -> None:
    h1 = compute_content_hash(b"hello world")
    h2 = compute_content_hash(b"hello world")
    assert h1 == h2
    # Sanity: 20-byte digest → 40-char hex.
    assert len(h1) == 40


def test_content_hash_distinguishes_inputs() -> None:
    assert compute_content_hash(b"foo") != compute_content_hash(b"bar")
    # Single-byte difference still flips.
    assert compute_content_hash(b"foo") != compute_content_hash(b"foO")


def test_content_hash_rejects_str() -> None:
    with pytest.raises(TypeError):
        compute_content_hash("not bytes")  # type: ignore[arg-type]


# ---------------------------------------------------------------------
# S3Client.put_if_absent — aioboto3 client-level mocking.
# ---------------------------------------------------------------------

def _patched_client():
    """Return (client_mock, session_mock, ctx_mgr_mock) wired so that
    `aioboto3.Session().client(...)` returns an async-context-manager
    yielding the mocked client.
    """
    mock_client = MagicMock(name="aioboto3_s3_client")
    mock_client.put_object = AsyncMock()
    mock_client.get_object = AsyncMock()

    ctx_mgr = MagicMock(name="client_cm")
    ctx_mgr.__aenter__ = AsyncMock(return_value=mock_client)
    ctx_mgr.__aexit__ = AsyncMock(return_value=None)

    session = MagicMock(name="aioboto3_session")
    session.client = MagicMock(return_value=ctx_mgr)
    return mock_client, session


@pytest.mark.asyncio
async def test_put_if_absent_succeeds_on_200(monkeypatch):
    mock_client, session = _patched_client()
    fake_aioboto3 = MagicMock()
    fake_aioboto3.Session = MagicMock(return_value=session)
    with patch.dict(
        "sys.modules", {"aioboto3": fake_aioboto3}, clear=False,
    ):
        async with S3Client("test-bucket") as s3:
            await s3.put_if_absent("k/foo", b"body-bytes")
    mock_client.put_object.assert_awaited_once()
    call_kwargs = mock_client.put_object.await_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == "k/foo"
    assert call_kwargs["Body"] == b"body-bytes"
    assert call_kwargs["IfNoneMatch"] == "*"


@pytest.mark.asyncio
async def test_put_if_absent_idempotent_412() -> None:
    """A 412 PreconditionFailed must NOT raise — it means the key
    already exists, which is the success path of an idempotent write.
    Per LLD §5.1.
    """
    from botocore.exceptions import ClientError

    mock_client, session = _patched_client()
    err = ClientError(
        {"Error": {"Code": "PreconditionFailed", "Message": "exists"}},
        "PutObject",
    )
    mock_client.put_object = AsyncMock(side_effect=err)

    fake_aioboto3 = MagicMock()
    fake_aioboto3.Session = MagicMock(return_value=session)
    with patch.dict(
        "sys.modules", {"aioboto3": fake_aioboto3}, clear=False,
    ):
        async with S3Client("test-bucket") as s3:
            # Must not raise.
            await s3.put_if_absent("k/foo", b"body")
    mock_client.put_object.assert_awaited_once()


@pytest.mark.asyncio
async def test_put_if_absent_propagates_real_errors() -> None:
    """Any non-412 error must propagate. Operationally important —
    a 500 from S3 cannot be silently swallowed.
    """
    from botocore.exceptions import ClientError

    mock_client, session = _patched_client()
    err = ClientError(
        {"Error": {"Code": "InternalError", "Message": "500"}},
        "PutObject",
    )
    mock_client.put_object = AsyncMock(side_effect=err)

    fake_aioboto3 = MagicMock()
    fake_aioboto3.Session = MagicMock(return_value=session)
    with patch.dict(
        "sys.modules", {"aioboto3": fake_aioboto3}, clear=False,
    ):
        async with S3Client("test-bucket") as s3:
            with pytest.raises(ClientError):
                await s3.put_if_absent("k/foo", b"body")


@pytest.mark.asyncio
async def test_get_returns_body_bytes() -> None:
    mock_client, session = _patched_client()
    # aioboto3 streams via resp["Body"].read() (async).
    body_stream = MagicMock(name="streaming_body")
    body_stream.read = AsyncMock(return_value=b"the-body")
    body_stream.close = MagicMock(return_value=None)
    mock_client.get_object = AsyncMock(return_value={"Body": body_stream})

    fake_aioboto3 = MagicMock()
    fake_aioboto3.Session = MagicMock(return_value=session)
    with patch.dict(
        "sys.modules", {"aioboto3": fake_aioboto3}, clear=False,
    ):
        async with S3Client("test-bucket") as s3:
            data = await s3.get("k/foo")
    assert data == b"the-body"
