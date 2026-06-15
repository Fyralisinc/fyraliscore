from __future__ import annotations

from lib.shared.http_headers import (
    REDACTED_HEADER_VALUE,
    redact_header_mapping,
    safe_headers,
)


def test_safe_headers_redacts_credentials_and_preserves_routing_headers() -> None:
    headers = {
        "Authorization": "Bearer secret-token",
        "X-Bootstrap-Secret": "bootstrap",
        "X-Hub-Signature-256": "sha256=abc",
        "X-GitHub-Event": "push",
        "X-GitHub-Delivery": "delivery-1",
    }

    assert safe_headers(headers) == {
        "Authorization": REDACTED_HEADER_VALUE,
        "X-Bootstrap-Secret": REDACTED_HEADER_VALUE,
        "X-Hub-Signature-256": REDACTED_HEADER_VALUE,
        "X-GitHub-Event": "push",
        "X-GitHub-Delivery": "delivery-1",
    }


def test_redact_header_mapping_handles_nested_header_bags() -> None:
    event = {
        "headers": {
            "authorization": "Bearer secret",
            "content-type": "application/json",
        },
        "context": {
            "request_headers": {
                "stripe-signature": "t=1,v1=secret",
                "stripe-account": "acct_1",
            },
            "api_key": "secret",
        },
    }

    redacted = redact_header_mapping(event)

    assert redacted["headers"]["authorization"] == REDACTED_HEADER_VALUE
    assert redacted["headers"]["content-type"] == "application/json"
    assert redacted["context"]["request_headers"]["stripe-signature"] == (
        REDACTED_HEADER_VALUE
    )
    assert redacted["context"]["request_headers"]["stripe-account"] == "acct_1"
    assert redacted["context"]["api_key"] == REDACTED_HEADER_VALUE
