"""Shared raw JSON payload validation for inline and Kafka ingest paths."""
from __future__ import annotations

import json
from typing import Any

from lib.shared.errors import CompanyOSError, ValidationError


MAX_PAYLOAD_BYTES = 1 * 1024 * 1024


class PayloadTooLarge(CompanyOSError):
    default_code = "payload_too_large"


def validate_ingest_json_payload(
    payload: Any,
    *,
    channel: str,
) -> None:
    """Apply the uniform ingest JSON guards before handler execution."""
    if not isinstance(payload, dict):
        raise ValidationError("raw_payload must be a JSON object")
    encoded = json.dumps(payload, default=str)
    encoded_len = len(encoded.encode("utf-8"))
    if encoded_len > MAX_PAYLOAD_BYTES:
        raise PayloadTooLarge(
            f"payload size > {MAX_PAYLOAD_BYTES} bytes",
            channel=channel,
            size=encoded_len,
        )
    if _contains_nul(payload):
        raise ValidationError(
            "payload contains NUL byte (0x00) which cannot be stored",
            channel=channel,
        )


def _contains_nul(obj: Any) -> bool:
    if isinstance(obj, str):
        return "\x00" in obj
    if isinstance(obj, dict):
        return any(
            (isinstance(k, str) and "\x00" in k) or _contains_nul(v)
            for k, v in obj.items()
        )
    if isinstance(obj, list):
        return any(_contains_nul(v) for v in obj)
    return False


__all__ = [
    "MAX_PAYLOAD_BYTES",
    "PayloadTooLarge",
    "validate_ingest_json_payload",
]
