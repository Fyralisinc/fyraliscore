from __future__ import annotations

from scripts.webhook_install import _PROVIDERS
from services.ingest.source_contract import WEBHOOK_INGRESS_ROUTE_IDS


def test_webhook_install_provider_choices_are_contract_derived() -> None:
    assert _PROVIDERS is WEBHOOK_INGRESS_ROUTE_IDS
