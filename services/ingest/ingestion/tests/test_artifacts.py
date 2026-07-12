"""Focused tests for the durable artifact catalog contract."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.ingestion.artifacts import (
    ArtifactDescriptorError,
    blob_id_for,
    persist_observation_artifacts,
    store_json_artifact,
    update_figma_snapshot_watermark,
)


class _FakeS3:
    def __init__(self) -> None:
        self.writes: list[tuple[str, bytes, dict]] = []

    async def put_if_absent(self, key: str, body: bytes, **kwargs) -> None:
        self.writes.append((key, body, kwargs))


class _FakeConn:
    def __init__(self) -> None:
        self.fetches: list[tuple[str, tuple]] = []
        self.executes: list[tuple[str, tuple]] = []

    async def fetchval(self, query: str, *args):
        self.fetches.append((query, args))
        return args[0]

    async def execute(self, query: str, *args):
        self.executes.append((query, args))
        return "INSERT 0 1"


@pytest.mark.asyncio
async def test_store_json_artifact_keeps_private_location_out_of_public_ref(monkeypatch):
    tenant_id = uuid4()
    s3 = _FakeS3()
    monkeypatch.setenv("S3_BLOB_BUCKET", "customer-artifacts")
    monkeypatch.setenv("FYRALIS_ENV", "test")

    artifact = await store_json_artifact(
        {"name": "Checkout", "document": {"type": "DOCUMENT"}},
        tenant_id=tenant_id,
        source="figma",
        kind="figma_document_json",
        s3_client=s3,
    )

    assert len(s3.writes) == 1
    key, body, kwargs = s3.writes[0]
    assert key.startswith(f"test/artifacts/figma/{tenant_id}/")
    assert body
    assert kwargs["content_type"] == "application/json"
    public = artifact.public_ref()
    assert public["blob_id"] == str(artifact.blob_id)
    assert "bucket" not in public
    assert "object_key" not in public
    private = artifact.private_descriptor()
    assert private["bucket"] == "customer-artifacts"
    assert private["object_key"] == key


@pytest.mark.asyncio
async def test_catalog_and_link_use_deterministic_tenant_blob_id():
    tenant_id = uuid4()
    artifact = await store_json_artifact(
        {"document": {"name": "A"}},
        tenant_id=tenant_id,
        source="figma",
        kind="figma_document_json",
        s3_client=_FakeS3(),
    )
    conn = _FakeConn()
    observation_id = uuid4()

    await persist_observation_artifacts(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        observation_id=observation_id,
        descriptors=[artifact.private_descriptor()],
    )

    assert conn.fetches[0][1][0] == blob_id_for(
        tenant_id=tenant_id, content_hash=artifact.content_hash,
    )
    assert conn.executes[0][1] == (
        tenant_id,
        observation_id,
        artifact.blob_id,
        "figma_document_json",
    )


@pytest.mark.asyncio
async def test_catalog_rejects_a_blob_id_not_bound_to_the_tenant_hash():
    tenant_id = uuid4()
    artifact = await store_json_artifact(
        {"document": {"name": "A"}},
        tenant_id=tenant_id,
        source="figma",
        kind="figma_document_json",
        s3_client=_FakeS3(),
    )
    descriptor = artifact.private_descriptor()
    descriptor["blob_id"] = str(uuid4())

    with pytest.raises(ArtifactDescriptorError, match="tenant/hash"):
        await persist_observation_artifacts(
            _FakeConn(),  # type: ignore[arg-type]
            tenant_id=tenant_id,
            observation_id=uuid4(),
            descriptors=[descriptor],
        )


@pytest.mark.asyncio
async def test_figma_snapshot_watermark_advances_only_after_catalog_link():
    tenant_id = uuid4()
    installation_id = uuid4()
    artifact = await store_json_artifact(
        {"document": {"name": "A"}},
        tenant_id=tenant_id,
        source="figma",
        kind="figma_document_json",
        s3_client=_FakeS3(),
    )
    conn = _FakeConn()

    await update_figma_snapshot_watermark(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        content={
            "object_type": "figma_file_snapshot",
            "file_key": "file-a",
            "figma_version": "v-42",
            "source_locator": {
                "installation_id": str(installation_id),
                "file_key": "file-a",
                "version": "v-42",
            },
        },
        descriptors=[artifact.private_descriptor()],
    )

    query, args = conn.executes[0]
    assert "UPDATE figma_files" in query
    assert args == ("v-42", artifact.blob_id, tenant_id, installation_id, "file-a")
    install_query, install_args = conn.executes[1]
    assert "UPDATE figma_installations" in install_query
    assert install_args == (installation_id, tenant_id)
