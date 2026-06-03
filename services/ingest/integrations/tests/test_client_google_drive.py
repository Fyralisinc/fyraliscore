"""Tests for services/ingest/integrations/google_drive/client.py (IN-16)."""
from __future__ import annotations

import pytest

from services.ingest.integrations.google_drive.client import (
    DRIVE_READONLY_SCOPE,
    GoogleDriveClient,
    is_extractable,
    resolve_scope,
)


pytestmark = pytest.mark.asyncio


class _FakeHttp:
    """Records request(...) kwargs; returns canned JSON bodies + raw bytes."""

    def __init__(self, responses=None, raw=b""):
        self._responses = list(responses or [])
        self._raw = raw
        self.requests: list[dict] = []
        self.byte_requests: list[dict] = []

    async def request(self, method, url, *, user_email, scopes, params=None, json_body=None):
        self.requests.append({
            "method": method, "url": url, "user_email": user_email,
            "scopes": tuple(scopes), "params": params or {},
            "json_body": json_body,
        })
        return self._responses.pop(0)

    async def request_bytes(self, method, url, *, user_email, scopes, params=None):
        self.byte_requests.append({"url": url, "params": params or {}})
        return self._raw


def _client(responses=None, raw=b""):
    http = _FakeHttp(responses, raw)
    return GoogleDriveClient(http, base_url="https://drive.test/v3"), http


async def test_resolve_scope_and_extractable():
    assert resolve_scope("drive.readonly") == DRIVE_READONLY_SCOPE
    with pytest.raises(ValueError):
        resolve_scope("nope")
    assert is_extractable("application/vnd.google-apps.document")
    assert is_extractable("text/markdown")
    assert not is_extractable("image/png")
    assert not is_extractable(None)


async def test_list_files_my_drive_shape():
    client, http = _client([{"files": [{"id": "f1"}]}])
    body = await client.list_files(
        user_email="alice@acme.com", modified_after="2026-01-01T00:00:00Z",
    )
    assert body["files"][0]["id"] == "f1"
    req = http.requests[0]
    assert req["url"] == "https://drive.test/v3/files"
    assert req["scopes"] == (DRIVE_READONLY_SCOPE,)
    assert req["params"]["corpora"] == "user"
    assert "modifiedTime > '2026-01-01T00:00:00Z'" in req["params"]["q"]
    assert "trashed = false" in req["params"]["q"]
    assert "driveId" not in req["params"]


async def test_list_files_shared_drive_shape():
    client, http = _client([{"files": []}])
    await client.list_files(user_email="admin@acme.com", drive_id="0ABC")
    req = http.requests[0]
    assert req["params"]["corpora"] == "drive"
    assert req["params"]["driveId"] == "0ABC"
    assert req["params"]["includeItemsFromAllDrives"] == "true"


async def test_get_start_page_token():
    client, http = _client([{"startPageToken": "spt-7"}])
    tok = await client.get_start_page_token(user_email="alice@acme.com")
    assert tok == "spt-7"
    assert http.requests[0]["url"].endswith("/changes/startPageToken")


async def test_list_changes_shape():
    client, http = _client([{"changes": [], "newStartPageToken": "spt-8"}])
    body = await client.list_changes(user_email="alice@acme.com", page_token="spt-7")
    assert body["newStartPageToken"] == "spt-8"
    req = http.requests[0]
    assert req["params"]["pageToken"] == "spt-7"
    assert req["params"]["includeRemoved"] == "true"


async def test_has_changes_since():
    client, _ = _client([{"changes": [{"fileId": "f1"}]}])
    assert await client.has_changes_since(user_email="a@x.com", page_token="t")
    client2, _ = _client([{"changes": []}])
    assert not await client2.has_changes_since(user_email="a@x.com", page_token="t")


async def test_export_text_google_native_truncates():
    client, http = _client(raw=b"hello world body")
    text = await client.export_text(
        user_email="a@x.com", file_id="f1",
        mime_type="application/vnd.google-apps.document", max_bytes=5,
    )
    assert text == "hello"
    assert http.byte_requests[0]["url"].endswith("/files/f1/export")
    assert http.byte_requests[0]["params"]["mimeType"] == "text/plain"


async def test_export_text_plain_uses_media():
    client, http = _client(raw=b"plain")
    text = await client.export_text(
        user_email="a@x.com", file_id="f2", mime_type="text/plain", max_bytes=100,
    )
    assert text == "plain"
    assert http.byte_requests[0]["params"]["alt"] == "media"


async def test_export_text_binary_returns_none():
    client, http = _client(raw=b"\x89PNG")
    text = await client.export_text(
        user_email="a@x.com", file_id="f3", mime_type="image/png", max_bytes=100,
    )
    assert text is None
    assert http.byte_requests == []


def _make_pdf(text: str) -> bytes:
    """A real one-page PDF carrying `text`, built with reportlab if available,
    else a hand-rolled minimal PDF that pypdf can extract."""
    import io
    from pypdf import PdfWriter
    # pypdf can't author text content; write a minimal PDF with a text stream.
    # Hand-roll a tiny valid PDF with one Tj operator.
    content = f"BT /F1 12 Tf 72 720 Td ({text}) Tj ET".encode()
    objs = []
    objs.append(b"<< /Type /Catalog /Pages 2 0 R >>")
    objs.append(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    objs.append(
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"
    )
    objs.append(b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content))
    objs.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    pdf = b"%PDF-1.4\n"
    offsets = []
    for i, body in enumerate(objs, start=1):
        offsets.append(len(pdf))
        pdf += b"%d 0 obj\n%s\nendobj\n" % (i, body)
    xref_pos = len(pdf)
    pdf += b"xref\n0 %d\n" % (len(objs) + 1)
    pdf += b"0000000000 65535 f \n"
    for off in offsets:
        pdf += b"%010d 00000 n \n" % off
    pdf += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (
        len(objs) + 1, xref_pos,
    )
    return pdf


async def test_export_text_pdf_extracts_with_pypdf():
    pdf_bytes = _make_pdf("Quarterly contract terms and obligations")
    client, http = _client(raw=pdf_bytes)
    text = await client.export_text(
        user_email="a@x.com", file_id="f-pdf", mime_type="application/pdf",
        max_bytes=10000, pdf_max_pages=5,
    )
    assert text is not None
    assert "contract terms" in text
    # PDFs download via alt=media (not export).
    assert http.byte_requests[0]["params"]["alt"] == "media"


async def test_list_comments_shape():
    client, http = _client([{"comments": [{"id": "c1", "content": "hi"}]}])
    body = await client.list_comments(user_email="a@x.com", file_id="f1")
    assert body["comments"][0]["id"] == "c1"
    req = http.requests[0]
    assert req["url"].endswith("/files/f1/comments")
    assert "replies" in req["params"]["fields"]


async def test_list_revisions_shape():
    client, http = _client([{"revisions": [{"id": "r1"}]}])
    body = await client.list_revisions(user_email="a@x.com", file_id="f1")
    assert body["revisions"][0]["id"] == "r1"
    assert http.requests[0]["url"].endswith("/files/f1/revisions")


async def test_watch_changes_request_shape():
    client, http = _client([{"id": "ch1", "resourceId": "res1", "expiration": "123"}])
    body = await client.watch_changes(
        user_email="alice@acme.com", page_token="tok-0", channel_id="ch1",
        address="https://app.test/webhooks/google_drive/push", token="secret-tok",
        drive_id="0ABC", ttl_seconds=604800,
    )
    assert body["resourceId"] == "res1"
    req = http.requests[0]
    assert req["method"] == "POST"
    assert req["url"] == "https://drive.test/v3/changes/watch"
    assert req["params"]["pageToken"] == "tok-0"
    assert req["params"]["supportsAllDrives"] == "true"
    assert req["params"]["driveId"] == "0ABC"
    jb = req["json_body"]
    assert jb["id"] == "ch1" and jb["type"] == "web_hook"
    assert jb["address"] == "https://app.test/webhooks/google_drive/push"
    assert jb["token"] == "secret-tok"


async def test_watch_changes_my_drive_omits_drive_id():
    client, http = _client([{"id": "ch2", "resourceId": "res2"}])
    await client.watch_changes(
        user_email="alice@acme.com", page_token="tok-0", channel_id="ch2",
        address="https://app.test/webhooks/google_drive/push", token="t",
        drive_id="my-drive",
    )
    # The My-Drive sentinel must not leak into the request as a driveId.
    assert "driveId" not in http.requests[0]["params"]


async def test_stop_channel_request_shape():
    client, http = _client([{}])
    await client.stop_channel(
        user_email="alice@acme.com", channel_id="ch1", resource_id="res1",
    )
    req = http.requests[0]
    assert req["method"] == "POST"
    assert req["url"] == "https://drive.test/v3/channels/stop"
    assert req["json_body"] == {"id": "ch1", "resourceId": "res1"}
