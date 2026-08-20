"""Local document ingestion UI for exercising the raw-tier pipeline."""
from __future__ import annotations

import datetime as dt
import inspect
import io
import json
import os
import uuid
import zipfile
from typing import Any
from uuid import UUID
from xml.etree import ElementTree

import orjson
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pypdf import PdfReader

from services.app.gateway.deps import get_gateway_deps
from services.app.gateway.html_responses import trusted_static_html_response
from services.ingest.ingestion.raw_emission import (
    CUTOVER_FLUSH_TIMEOUT_SEC,
    emit_raw,
)


_DEFAULT_TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")
_MAX_UPLOAD_BYTES = int(os.environ.get("DOCUMENT_INGEST_UPLOAD_MAX_BYTES", "52428800"))
_OWNER_EMAIL = os.environ.get("DOCUMENT_INGEST_OWNER_EMAIL", "local-upload@fyralis.test")
_DOCX_TEXT_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t"
_DOCX_PARAGRAPH_NS = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p"


_UPLOAD_PAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Document Ingest</title>
  <style nonce="__CSP_NONCE__">
    :root {
      color-scheme: light;
      --bg: #f6f7f8;
      --panel: #ffffff;
      --ink: #172126;
      --muted: #5c6b73;
      --line: #d8dee2;
      --accent: #087f8c;
      --accent-strong: #065f69;
      --warn: #9a5b00;
      --danger: #b42318;
      --ok: #136f45;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      max-width: 1120px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 24px;
      font-weight: 700;
      letter-spacing: 0;
    }
    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
    }
    input[type="text"] {
      width: 360px;
      max-width: 44vw;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      color: var(--ink);
      background: #fff;
      font: inherit;
      text-transform: none;
    }
    .workspace {
      display: grid;
      grid-template-columns: minmax(280px, 380px) 1fr;
      gap: 18px;
      align-items: start;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      min-width: 0;
    }
    .dropzone {
      min-height: 238px;
      display: grid;
      place-items: center;
      padding: 24px;
      border: 1px dashed #98a6ad;
      border-radius: 8px;
      background: #fbfcfc;
      cursor: pointer;
      transition: border-color 120ms ease, background 120ms ease;
    }
    .dropzone.is-hot {
      border-color: var(--accent);
      background: #eef9f8;
    }
    .drop-inner {
      display: grid;
      justify-items: center;
      gap: 12px;
      text-align: center;
    }
    .drop-title {
      font-size: 18px;
      font-weight: 700;
    }
    .drop-meta {
      color: var(--muted);
      font-size: 13px;
    }
    button {
      appearance: none;
      border: 1px solid var(--accent);
      border-radius: 6px;
      background: var(--accent);
      color: #fff;
      padding: 9px 14px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    button:hover { background: var(--accent-strong); }
    button:disabled {
      cursor: default;
      opacity: 0.55;
    }
    .side {
      display: grid;
      gap: 12px;
      padding: 16px;
    }
    .field {
      display: grid;
      grid-template-columns: 128px minmax(0, 1fr);
      gap: 10px;
      padding: 10px 0;
      border-top: 1px solid var(--line);
      font-size: 14px;
    }
    .field:first-child { border-top: 0; }
    .key {
      color: var(--muted);
      font-weight: 700;
    }
    .value {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .status {
      display: inline-flex;
      width: fit-content;
      align-items: center;
      min-height: 28px;
      border-radius: 999px;
      padding: 4px 10px;
      border: 1px solid var(--line);
      background: #f9fafb;
      font-weight: 700;
      color: var(--muted);
    }
    .status.pending,
    .status.normalizing { color: var(--warn); background: #fff8eb; border-color: #f3d59c; }
    .status.complete,
    .status.stored { color: var(--ok); background: #effaf4; border-color: #b8e3c9; }
    .status.failed { color: var(--danger); background: #fff2f0; border-color: #f3b8b2; }
    .summary {
      margin: 0;
      min-height: 300px;
      max-height: 62vh;
      overflow: auto;
      padding: 16px;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 13px;
      line-height: 1.55;
      color: #233036;
    }
    .empty {
      color: var(--muted);
    }
    .error {
      color: var(--danger);
      font-weight: 700;
    }
    input[type="file"] { display: none; }
    @media (max-width: 820px) {
      main { padding: 18px 12px 28px; }
      header { display: grid; }
      input[type="text"] { width: 100%; max-width: none; }
      .workspace { grid-template-columns: 1fr; }
      .dropzone { min-height: 190px; }
      .field { grid-template-columns: 96px minmax(0, 1fr); }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>Document Ingest</h1>
      <label>
        Tenant
        <input id="tenant" type="text" autocomplete="off" />
      </label>
    </header>
    <section class="workspace">
      <div class="panel side">
        <input id="file" type="file" accept=".pdf,.docx,application/pdf,application/vnd.openxmlformats-officedocument.wordprocessingml.document" />
        <div id="dropzone" class="dropzone" role="button" tabindex="0">
          <div class="drop-inner">
            <div class="drop-title">Drop PDF or DOCX</div>
            <div id="fileMeta" class="drop-meta">No file selected</div>
            <button id="browse" type="button">Browse</button>
          </div>
        </div>
        <div class="field">
          <div class="key">Status</div>
          <div class="value"><span id="status" class="status">idle</span></div>
        </div>
        <div class="field">
          <div class="key">File</div>
          <div id="name" class="value empty">-</div>
        </div>
        <div class="field">
          <div class="key">External ID</div>
          <div id="externalId" class="value empty">-</div>
        </div>
        <div class="field">
          <div class="key">Model</div>
          <div id="model" class="value empty">-</div>
        </div>
        <div class="field">
          <div class="key">Chars</div>
          <div id="chars" class="value empty">-</div>
        </div>
      </div>
      <div class="panel">
        <pre id="summary" class="summary empty">Summary will appear here.</pre>
      </div>
    </section>
  </main>
  <script nonce="__CSP_NONCE__">
    const defaultTenantId = __DEFAULT_TENANT_ID__;
    const dropzone = document.getElementById("dropzone");
    const fileInput = document.getElementById("file");
    const browse = document.getElementById("browse");
    const tenant = document.getElementById("tenant");
    const fileMeta = document.getElementById("fileMeta");
    const statusEl = document.getElementById("status");
    const nameEl = document.getElementById("name");
    const externalEl = document.getElementById("externalId");
    const modelEl = document.getElementById("model");
    const charsEl = document.getElementById("chars");
    const summaryEl = document.getElementById("summary");
    let pollTimer = null;

    tenant.value = defaultTenantId;

    function setStatus(value) {
      statusEl.textContent = value;
      statusEl.className = "status " + String(value || "idle").toLowerCase();
    }

    function setText(el, value) {
      el.textContent = value || "-";
      el.classList.toggle("empty", !value);
    }

    function setSummary(value, isError) {
      summaryEl.textContent = value || "Summary will appear here.";
      summaryEl.classList.toggle("empty", !value);
      summaryEl.classList.toggle("error", Boolean(isError));
    }

    function renderUpload(data) {
      setStatus("normalizing");
      setText(nameEl, data.name);
      setText(externalEl, data.external_id);
      setText(modelEl, "");
      setText(charsEl, String(data.extracted_chars || 0));
      setSummary("Queued through raw-tier ingestion.");
    }

    function renderStatus(data) {
      const status = data.status || data.state || "unknown";
      setStatus(status);
      setText(modelEl, data.model || "");
      setText(charsEl, data.summary_chars ? String(data.summary_chars) : String(data.extracted_chars || ""));
      if (data.content_text) {
        setSummary(data.content_text, status === "failed");
      } else if (status === "normalizing") {
        setSummary("Waiting for the observation row.");
      } else if (status === "pending") {
        setSummary("Summarization queued.");
      }
      if (status === "complete" || status === "failed" || status === "stored") {
        window.clearInterval(pollTimer);
        pollTimer = null;
      }
    }

    async function poll(externalId) {
      const qs = new URLSearchParams({
        external_id: externalId,
        tenant_id: tenant.value.trim()
      });
      const response = await fetch("/debug/document-ingest/status?" + qs.toString());
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || data.error || "status failed");
      }
      renderStatus(data);
    }

    async function upload(file) {
      if (!file) return;
      window.clearInterval(pollTimer);
      pollTimer = null;
      fileMeta.textContent = file.name + " / " + Math.ceil(file.size / 1024) + " KB";
      setStatus("uploading");
      setSummary("");
      setText(nameEl, file.name);
      setText(externalEl, "");
      setText(modelEl, "");
      setText(charsEl, "");

      const qs = new URLSearchParams({
        filename: file.name,
        tenant_id: tenant.value.trim()
      });
      const response = await fetch("/debug/document-ingest/upload?" + qs.toString(), {
        method: "POST",
        headers: { "Content-Type": file.type || "application/octet-stream" },
        body: file
      });
      const data = await response.json();
      if (!response.ok) {
        setStatus("failed");
        setSummary(data.detail || data.error || "Upload failed.", true);
        return;
      }
      renderUpload(data);
      await poll(data.external_id);
      pollTimer = window.setInterval(() => poll(data.external_id).catch((err) => {
        setStatus("failed");
        setSummary(err.message, true);
        window.clearInterval(pollTimer);
        pollTimer = null;
      }), 2000);
    }

    browse.addEventListener("click", () => fileInput.click());
    fileInput.addEventListener("change", () => upload(fileInput.files[0]));
    dropzone.addEventListener("click", () => fileInput.click());
    dropzone.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        fileInput.click();
      }
    });
    for (const eventName of ["dragenter", "dragover"]) {
      dropzone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropzone.classList.add("is-hot");
      });
    }
    for (const eventName of ["dragleave", "drop"]) {
      dropzone.addEventListener(eventName, (event) => {
        event.preventDefault();
        dropzone.classList.remove("is-hot");
      });
    }
    dropzone.addEventListener("drop", (event) => upload(event.dataTransfer.files[0]));
  </script>
</body>
</html>
""".strip()


def build_document_ingest_router() -> APIRouter:
    router = APIRouter(prefix="/debug/document-ingest", tags=["debug"])

    @router.get("", include_in_schema=False)
    @router.get("/", include_in_schema=False)
    async def upload_page() -> HTMLResponse:
        page = _UPLOAD_PAGE.replace(
            "__DEFAULT_TENANT_ID__", json.dumps(str(_default_tenant_id()))
        )
        return trusted_static_html_response(page)

    @router.post("/upload")
    async def upload_document(
        request: Request,
        filename: str = Query(..., min_length=1),
        tenant_id: str | None = Query(None),
    ) -> dict[str, Any]:
        tenant_uuid = _tenant_id_from_request(request, tenant_id)
        body = await request.body()
        if not body:
            raise HTTPException(status_code=400, detail="empty_file")
        if len(body) > _MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="file_too_large")

        clean_name = _clean_filename(filename)
        content_type = _content_type(request)
        text = _extract_text(clean_name, content_type, body)
        if not text.strip():
            raise HTTPException(status_code=422, detail="no_text_extracted")

        app_state = request.app.state
        s3_client = getattr(app_state, "s3_raw_client", None)
        kafka_producer = getattr(app_state, "kafka_producer", None)
        if s3_client is None or kafka_producer is None:
            raise HTTPException(status_code=503, detail="ingestion_data_plane_unavailable")

        pool = _pool_from_request(request)
        await _ensure_tenant(pool, tenant_uuid)

        now = dt.datetime.now(tz=dt.timezone.utc)
        file_id = f"local-upload-{uuid.uuid4().hex}"
        version = str(int(now.timestamp() * 1000))
        external_id = f"gdrive:{file_id}:{version}"
        record = _build_drive_record(
            file_id=file_id,
            filename=clean_name,
            content_type=_drive_mime(clean_name, content_type),
            body=body,
            text=text,
            version=version,
            now=now,
        )
        raw_body = orjson.dumps(
            {
                "record": record,
                "shard_context": {
                    "source": "document_ingest_ui",
                    "submitted_at": _iso_z(now),
                    "external_id": external_id,
                },
                "webhook_metadata": {},
            },
            option=orjson.OPT_SORT_KEYS,
        )

        raw_s3_key = await emit_raw(
            tenant_id=tenant_uuid,
            source="google_drive",
            ingress_kind="backfill",
            raw_body=raw_body,
            s3_client=s3_client,
            kafka_producer=kafka_producer,
            ingress_metadata={
                "source": "document_ingest_ui",
                "filename": clean_name,
                "external_id": external_id,
            },
            idem_hints={
                "file_id": file_id,
                "version": version,
            },
            now=now,
        )
        await _flush_producer(kafka_producer)

        return {
            "tenant_id": str(tenant_uuid),
            "name": clean_name,
            "external_id": external_id,
            "raw_s3_key": raw_s3_key,
            "extracted_chars": len(text),
        }

    @router.get("/status")
    async def document_status(
        request: Request,
        external_id: str = Query(..., min_length=1),
        tenant_id: str | None = Query(None),
    ) -> dict[str, Any]:
        tenant_uuid = _tenant_id_from_request(request, tenant_id)
        pool = _pool_from_request(request)
        row = await _fetch_observation(pool, tenant_uuid, external_id)
        if row is None:
            return {
                "tenant_id": str(tenant_uuid),
                "external_id": external_id,
                "state": "normalizing",
                "status": "normalizing",
            }

        content = _decode_content(row["content"])
        summary = content.get("summarization") if isinstance(content, dict) else None
        summary = summary if isinstance(summary, dict) else {}
        status = str(summary.get("status") or "stored")
        return {
            "tenant_id": str(tenant_uuid),
            "external_id": row["external_id"],
            "observation_id": str(row["id"]),
            "source_channel": row["source_channel"],
            "state": status,
            "status": status,
            "model": summary.get("model"),
            "summary_chars": summary.get("summary_chars"),
            "source_chars": summary.get("source_chars"),
            "raw_s3_key": summary.get("raw_s3_key"),
            "extracted_chars": content.get("extracted_chars") if isinstance(content, dict) else None,
            "content_text": row["content_text"],
            "content": content,
            "ingested_at": row["ingested_at"].isoformat() if row["ingested_at"] else None,
            "occurred_at": row["occurred_at"].isoformat() if row["occurred_at"] else None,
        }

    return router


def _default_tenant_id() -> UUID:
    for key in ("DOCUMENT_INGEST_TENANT_ID", "DEFAULT_TENANT_ID", "COMPANY_OS_TENANT_ID"):
        value = os.environ.get(key)
        if value:
            return UUID(value)
    return _DEFAULT_TENANT_ID


def _tenant_id_from_request(request: Request, tenant_id: str | None) -> UUID:
    raw = tenant_id or request.headers.get("x-tenant-id")
    if not raw:
        return _default_tenant_id()
    try:
        return UUID(raw)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="bad_tenant_id") from exc


def _pool_from_request(request: Request) -> Any:
    try:
        deps = get_gateway_deps(request)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail="service_unavailable") from exc
    pool = getattr(deps, "pool", None)
    if pool is None:
        raise HTTPException(status_code=503, detail="service_unavailable")
    return pool


async def _ensure_tenant(pool: Any, tenant_id: UUID) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tenants (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            tenant_id,
            "document-ingest-ui",
        )


async def _fetch_observation(pool: Any, tenant_id: UUID, external_id: str) -> Any | None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT set_config('app.current_tenant', $1::text, true)",
                str(tenant_id),
            )
            return await conn.fetchrow(
                """
                SELECT id, source_channel, external_id, occurred_at, ingested_at,
                       content, content_text
                  FROM observations
                 WHERE tenant_id = $1
                   AND source_channel = 'google_drive:file'
                   AND external_id = $2
                 ORDER BY occurred_at DESC
                 LIMIT 1
                """,
                tenant_id,
                external_id,
            )


async def _flush_producer(kafka_producer: Any) -> None:
    flush = getattr(kafka_producer, "flush", None)
    if not callable(flush):
        return
    result = flush(timeout_seconds=CUTOVER_FLUSH_TIMEOUT_SEC)
    if inspect.isawaitable(result):
        await result


def _content_type(request: Request) -> str:
    return (request.headers.get("content-type") or "application/octet-stream").split(";", 1)[0].lower()


def _clean_filename(filename: str) -> str:
    clean = filename.replace("\\", "/").rsplit("/", 1)[-1].strip()
    return clean[:180] or "document"


def _drive_mime(filename: str, content_type: str) -> str:
    lower = filename.lower()
    if lower.endswith(".pdf") or content_type == "application/pdf":
        return "application/pdf"
    if lower.endswith(".docx") or content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if content_type.startswith("text/"):
        return content_type
    return "application/octet-stream"


def _extract_text(filename: str, content_type: str, body: bytes) -> str:
    lower = filename.lower()
    if lower.endswith(".pdf") or content_type == "application/pdf":
        return _extract_pdf_text(body)
    if lower.endswith(".docx") or content_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return _extract_docx_text(body)
    if lower.endswith(".doc"):
        raise HTTPException(status_code=415, detail="legacy_doc_unsupported")
    if content_type.startswith("text/") or lower.endswith((".txt", ".md")):
        return body.decode("utf-8", errors="replace")
    raise HTTPException(status_code=415, detail="unsupported_file_type")


def _extract_pdf_text(body: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(body))
        parts: list[str] = []
        for page in reader.pages:
            text = page.extract_text() or ""
            if text.strip():
                parts.append(text.strip())
        return "\n\n".join(parts)
    except Exception as exc:  # noqa: BLE001 - PDF parser errors vary by file
        raise HTTPException(status_code=422, detail="pdf_text_extraction_failed") from exc


def _extract_docx_text(body: bytes) -> str:
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as docx:
            xml = docx.read("word/document.xml")
    except (KeyError, zipfile.BadZipFile) as exc:
        raise HTTPException(status_code=422, detail="docx_text_extraction_failed") from exc
    try:
        root = ElementTree.fromstring(xml)
    except ElementTree.ParseError as exc:
        raise HTTPException(status_code=422, detail="docx_text_extraction_failed") from exc

    paragraphs: list[str] = []
    for paragraph in root.iter(_DOCX_PARAGRAPH_NS):
        text = "".join(node.text or "" for node in paragraph.iter(_DOCX_TEXT_NS))
        if text.strip():
            paragraphs.append(text.strip())
    return "\n".join(paragraphs)


def _build_drive_record(
    *,
    file_id: str,
    filename: str,
    content_type: str,
    body: bytes,
    text: str,
    version: str,
    now: dt.datetime,
) -> dict[str, Any]:
    now_z = _iso_z(now)
    owner = {"emailAddress": _OWNER_EMAIL, "displayName": "Local Upload"}
    return {
        "id": file_id,
        "name": filename,
        "mimeType": content_type,
        "version": version,
        "trashed": False,
        "createdTime": now_z,
        "modifiedTime": now_z,
        "webViewLink": f"local://document-ingest/{file_id}",
        "owners": [owner],
        "lastModifyingUser": owner,
        "shared": False,
        "size": str(len(body)),
        "_fyralis_drive_id": "local-upload",
        "_fyralis_drive_kind": "local",
        "_fyralis_owner_email": _OWNER_EMAIL,
        "_fyralis_removed": False,
        "_fyralis_record_type": "file",
        "_fyralis_text_yield": "text",
        "_fyralis_upload_source": "document_ingest_ui",
        "_fyralis_extracted_text": text,
    }


def _iso_z(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _decode_content(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, (bytes, bytearray)):
        return orjson.loads(value)
    return {}


__all__ = ["build_document_ingest_router"]
