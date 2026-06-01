"""Tests for the Pub/Sub push envelope decoder."""
from __future__ import annotations

import base64
import json

import pytest

from services.integrations.gmail.push_handler import (
    GmailPushError,
    decode_pubsub_message,
)


def _b64(d: dict[str, object]) -> str:
    return base64.b64encode(json.dumps(d).encode("utf-8")).decode("ascii")


class TestDecodePubsubMessage:
    def test_typical_envelope(self) -> None:
        envelope = {
            "message": {
                "data": _b64({"emailAddress": "alice@acme.com", "historyId": "12345"}),
                "messageId": "abc",
                "publishTime": "2026-01-01T00:00:00Z",
            },
            "subscription": "projects/fyralis-prod/subscriptions/gmail-tenant-sub",
        }
        sub, decoded = decode_pubsub_message(envelope)
        assert sub == "projects/fyralis-prod/subscriptions/gmail-tenant-sub"
        assert decoded == {"emailAddress": "alice@acme.com", "historyId": "12345"}

    def test_missing_subscription(self) -> None:
        envelope = {"message": {"data": _b64({"emailAddress": "x@y"})}}
        with pytest.raises(GmailPushError):
            decode_pubsub_message(envelope)

    def test_missing_data_returns_empty(self) -> None:
        envelope = {
            "message": {},
            "subscription": "projects/p/subscriptions/s",
        }
        sub, decoded = decode_pubsub_message(envelope)
        assert sub == "projects/p/subscriptions/s"
        assert decoded == {}

    def test_invalid_data_base64(self) -> None:
        envelope = {
            "message": {"data": "%%%not-base64%%%"},
            "subscription": "projects/p/subscriptions/s",
        }
        with pytest.raises(GmailPushError):
            decode_pubsub_message(envelope)

    def test_invalid_json_in_data(self) -> None:
        envelope = {
            "message": {"data": base64.b64encode(b"{not-json").decode("ascii")},
            "subscription": "projects/p/subscriptions/s",
        }
        with pytest.raises(GmailPushError):
            decode_pubsub_message(envelope)

    def test_non_dict_envelope(self) -> None:
        with pytest.raises(GmailPushError):
            decode_pubsub_message(["not a dict"])  # type: ignore[arg-type]
