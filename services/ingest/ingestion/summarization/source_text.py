"""Source-text recovery helpers for summarization workers."""
from __future__ import annotations

import json
import logging
from typing import Any

import orjson

from services.ingest.ingestion.raw_tier.s3 import S3Client


log = logging.getLogger(__name__)


def _unwrap_raw_payload(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    record = payload.get("record")
    if isinstance(record, dict):
        return record
    return payload


def _extract_rich_text(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    parts: list[str] = []
    for item in value:
        if isinstance(item, dict):
            text = item.get("plain_text")
            if isinstance(text, str):
                parts.append(text)
    return "".join(parts)


def raw_document_text(record: dict[str, Any]) -> str | None:
    extracted = record.get("_fyralis_extracted_text")
    if isinstance(extracted, str) and extracted.strip():
        return extracted.strip()
    for key in ("transcript", "text", "body", "content"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    block_type = record.get("type")
    inner = record.get(block_type) if isinstance(block_type, str) else None
    if isinstance(inner, dict):
        text = _extract_rich_text(inner.get("rich_text"))
        if text.strip():
            return text.strip()
    return None


async def source_text_from_raw_s3(
    s3: S3Client | None,
    raw_s3_key: str | None,
) -> str | None:
    if s3 is None or not raw_s3_key:
        return None
    try:
        raw = await s3.get(raw_s3_key)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "summarization.raw_s3_get_failed",
            extra={
                "raw_s3_key": raw_s3_key,
                "error_type": type(exc).__name__,
                "error": str(exc)[:200],
            },
        )
        return None
    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError:
        try:
            import zstandard as zstd

            payload = orjson.loads(zstd.ZstdDecompressor().decompress(raw))
        except Exception:  # noqa: BLE001
            return None
    record = _unwrap_raw_payload(payload)
    return raw_document_text(record) if record is not None else None


def source_text_from_content(content: dict[str, Any], content_text: str) -> str | None:
    summary = content.get("summarization")
    if isinstance(summary, dict):
        source_text = summary.get("source_text")
        if isinstance(source_text, str) and source_text.strip():
            return source_text.strip()
    for key in ("text", "summary"):
        value = content.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if content_text and "queued for summarization" not in content_text:
        return content_text
    return None


def metadata_from_content(
    *,
    content: dict[str, Any],
    source_channel: str,
) -> dict[str, Any]:
    metadata = {"source_channel": source_channel}
    for key in ("title", "name", "file_name", "mime_type", "object_type"):
        value = content.get(key)
        if value is not None:
            metadata[key] = value
    return metadata


def decode_json_content(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    return {}


__all__ = [
    "decode_json_content",
    "metadata_from_content",
    "raw_document_text",
    "source_text_from_content",
    "source_text_from_raw_s3",
]
