"""Layer-4-local Protocol for SUTs that can surface emitted anomalies.

The shared `SystemUnderTest` interface already exposes `query_at_risk_at`,
which is enough for sub-evaluations 1 and 2 (at-risk commitment / customer
risk precision-recall). Sub-evaluations 3 and 4 (anomaly precision, alert
fatigue) require an additional surface that not every baseline will support:
the ability to replay the anomalies the SUT emitted during ingestion over a
window.

SUTs that expose this surface should conform to
:class:`AnomalyEmittingSUT`; when the SUT lacks it the Layer 4 evaluator
reports ``layer_not_applicable`` for those two metrics only.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AnomalyEmittingSUT(Protocol):
    """Optional extra surface a SUT may expose for anomaly auditing.

    Each returned dict should carry at least a ``timestamp`` (datetime) key.
    Optional keys used by the evaluator include ``kind``, ``entity_ref``, and
    ``rationale``. Any additional keys are preserved in the breakdown.
    """

    async def emitted_anomalies(
        self, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        """Return the anomalies the SUT emitted in ``[start, end)``."""
        ...
