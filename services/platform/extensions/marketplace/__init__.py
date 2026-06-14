"""services.platform.extensions.marketplace — the curated extension registry (E4).

submit → automated gate (lint + scope justification + callback domain) → approved →
(public: manual review + signing) → published → tenant install (signature-verified).
"""
from services.platform.extensions.marketplace.registry import (
    MarketplaceError, MarketplaceRepo,
)
from services.platform.extensions.marketplace.review import GateResult, automated_gate

__all__ = ["MarketplaceRepo", "MarketplaceError", "automated_gate", "GateResult"]
