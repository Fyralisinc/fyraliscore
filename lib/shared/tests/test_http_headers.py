from __future__ import annotations

from lib.shared.http_headers import (
    REDACTED_HEADER_VALUE,
    redact_header_mapping,
    redact_log_mapping,
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


def test_redact_log_mapping_masks_json_body_and_pii_context() -> None:
    event = {
        "payload": {
            "access_token": "tok_live_123",
            "email": "alice@example.com",
        },
        "context": {
            "owner_email": "owner@example.com",
            "bank": {
                "account_number": "123456789",
                "routing_number": "021000021",
            },
            "input_tokens": 128,
        },
        "prompt": "raw customer prompt",
    }

    redacted = redact_log_mapping(event)

    assert redacted["payload"] == REDACTED_HEADER_VALUE
    assert redacted["context"]["owner_email"] == REDACTED_HEADER_VALUE
    assert redacted["context"]["bank"]["account_number"] == REDACTED_HEADER_VALUE
    assert redacted["context"]["bank"]["routing_number"] == REDACTED_HEADER_VALUE
    assert redacted["context"]["input_tokens"] == 128
    assert redacted["prompt"] == REDACTED_HEADER_VALUE


def test_pii_egress_probe_redacts_logs_for_sensitive_fields() -> None:
    event = {
        "owner_email": "owner@example.com",
        "source_channel": "slack:secret-board-room",
        "channel_name": "#acquisition-war-room",
        "content_text": "raw customer payload text",
        "prompt": "raw customer prompt",
        "message": (
            "provider failed for alice@example.com with "
            "Authorization=Bearer sk-test-secret"
        ),
    }

    redacted = redact_log_mapping(event)

    assert redacted["owner_email"] == REDACTED_HEADER_VALUE
    assert redacted["source_channel"] == REDACTED_HEADER_VALUE
    assert redacted["channel_name"] == REDACTED_HEADER_VALUE
    assert redacted["content_text"] == REDACTED_HEADER_VALUE
    assert redacted["prompt"] == REDACTED_HEADER_VALUE
    assert "alice@example.com" not in redacted["message"]
    assert "sk-test-secret" not in redacted["message"]
    assert "[redacted-email]" in redacted["message"]


def test_redact_log_mapping_masks_secret_patterns_inside_strings() -> None:
    event = {
        "message": (
            "provider failed for alice@example.com with "
            "Authorization=Bearer sk-test and password=hunter2"
        )
    }

    redacted = redact_log_mapping(event)

    message = redacted["message"]
    assert "alice@example.com" not in message
    assert "sk-test" not in message
    assert "hunter2" not in message
    assert "[redacted-email]" in message
