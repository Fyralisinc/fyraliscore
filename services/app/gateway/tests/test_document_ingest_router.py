from __future__ import annotations

import datetime as dt
import io
import zipfile
from types import SimpleNamespace
from uuid import UUID

import httpx
import orjson
import pytest
from fastapi import FastAPI

from services.app.gateway.document_ingest_router import (
    _extract_docx_text,
    build_document_ingest_router,
)


_TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")
_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


class _Acquire:
    def __init__(self, pool: _Pool) -> None:
        self.pool = pool

    async def __aenter__(self) -> _Conn:
        return _Conn(self.pool)

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _Conn:
    def __init__(self, pool: _Pool) -> None:
        self.pool = pool

    async def execute(self, query: str, *args: object) -> str:
        self.pool.executed.append((query, args))
        return "OK"

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.pool.fetched.append((query, args))
        return self.pool.row

    def transaction(self) -> _Conn:
        return self

    async def __aenter__(self) -> _Conn:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _Pool:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.fetched: list[tuple[str, tuple[object, ...]]] = []
        self.row: dict[str, object] | None = None

    def acquire(self) -> _Acquire:
        return _Acquire(self)


class _S3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self.objects.setdefault(key, body)


class _Producer:
    def __init__(self) -> None:
        self.produced: list[dict[str, object]] = []
        self.flush_timeouts: list[float] = []

    async def produce(self, *, topic: str, value: bytes, key: bytes) -> None:
        self.produced.append({"topic": topic, "value": value, "key": key})

    async def flush(self, timeout_seconds: float = 10.0) -> int:
        self.flush_timeouts.append(timeout_seconds)
        return 0


def _docx_bytes(*paragraphs: str) -> bytes:
    paragraph_xml = "".join(
        f"<w:p><w:r><w:t>{text}</w:t></w:r></w:p>" for text in paragraphs
    )
    document_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body>{paragraph_xml}</w:body>"
        "</w:document>"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as docx:
        docx.writestr("word/document.xml", document_xml)
    return buf.getvalue()


def _app() -> tuple[FastAPI, _Pool, _S3, _Producer]:
    app = FastAPI()
    app.include_router(build_document_ingest_router())
    pool = _Pool()
    s3 = _S3()
    producer = _Producer()
    app.state.deps = SimpleNamespace(pool=pool)
    app.state.s3_raw_client = s3
    app.state.kafka_producer = producer
    return app, pool, s3, producer


def test_extract_docx_text_preserves_paragraphs() -> None:
    body = _docx_bytes("First paragraph", "Second paragraph")

    assert _extract_docx_text(body) == "First paragraph\nSecond paragraph"


@pytest.mark.asyncio
async def test_upload_document_publishes_google_drive_raw_envelope() -> None:
    app, pool, s3, producer = _app()
    body = _docx_bytes("Quarterly plan", "Revenue expansion")

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post(
            f"/debug/document-ingest/upload?tenant_id={_TENANT_ID}&filename=plan.docx",
            content=body,
            headers={"content-type": _DOCX_MIME},
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["external_id"].startswith("gdrive:local-upload-")
    assert payload["extracted_chars"] == len("Quarterly plan\nRevenue expansion")
    assert producer.produced[0]["topic"] == "ingestion.raw.google_drive"
    assert producer.flush_timeouts
    assert pool.executed[0][1][0] == _TENANT_ID

    raw_body = next(iter(s3.objects.values()))
    raw_payload = orjson.loads(raw_body)
    record = raw_payload["record"]
    assert record["name"] == "plan.docx"
    assert record["mimeType"] == _DOCX_MIME
    assert record["_fyralis_extracted_text"] == "Quarterly plan\nRevenue expansion"
    assert raw_payload["shard_context"]["source"] == "document_ingest_ui"


@pytest.mark.asyncio
async def test_document_status_reports_complete_summary_metadata() -> None:
    app, pool, _s3, _producer = _app()
    now = dt.datetime(2026, 6, 17, tzinfo=dt.timezone.utc)
    pool.row = {
        "id": UUID("11111111-1111-1111-1111-111111111111"),
        "source_channel": "google_drive:file",
        "external_id": "gdrive:local-upload-abc:123",
        "occurred_at": now,
        "ingested_at": now,
        "content": {
            "name": "plan.docx",
            "extracted_chars": 24000,
            "summarization": {
                "status": "complete",
                "model": "gpt-5.3-codex-spark",
                "summary_chars": 512,
                "source_chars": 24000,
                "raw_s3_key": "dev/google_drive/key.json.zst",
            },
        },
        "content_text": "Summary text",
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/debug/document-ingest/status",
            params={"tenant_id": str(_TENANT_ID), "external_id": "gdrive:local-upload-abc:123"},
        )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "complete"
    assert payload["model"] == "gpt-5.3-codex-spark"
    assert payload["content_text"] == "Summary text"
    assert payload["raw_s3_key"] == "dev/google_drive/key.json.zst"


@pytest.mark.asyncio
async def test_document_status_reports_normalizing_before_row_exists() -> None:
    app, _pool, _s3, _producer = _app()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get(
            "/debug/document-ingest/status",
            params={"tenant_id": str(_TENANT_ID), "external_id": "gdrive:local-upload-missing:1"},
        )

    assert resp.status_code == 200
    assert resp.json()["status"] == "normalizing"
