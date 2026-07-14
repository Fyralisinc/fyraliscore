"""Pure identity tests for Instagram conversation discovery."""
from __future__ import annotations

from services.ingest.integrations.instagram.onboarding import _conversation_participant


def test_conversation_participant_excludes_delivery_alias() -> None:
    participant_id, username, display_name = _conversation_participant(
        {
            "participants": {
                "data": [
                    {"id": "meta-delivery-id"},
                    {"id": "cust-1", "username": "customer", "name": "Customer"},
                ],
            },
        },
        ig_business_account_id="ig-business",
        webhook_delivery_account_id="meta-delivery-id",
    )

    assert (participant_id, username, display_name) == (
        "cust-1", "customer", "Customer",
    )
