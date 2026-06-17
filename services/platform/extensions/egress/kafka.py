"""services/platform/extensions/egress/kafka.py — egress Kafka topic constants.

The projector produces the capability-filtered, redacted projection here (keyed by
extension_id); external/partner infra or the delivery worker consume it. Kept
separate from the ingestion `ingestion.<stage>.<source>` scheme — egress is a
platform concern, not an ingestion stage.
"""
from __future__ import annotations

EGRESS_TOPIC = "ext.egress.v1"
EGRESS_DLQ_TOPIC = "ext.egress.dlq"


def egress_topics() -> list[str]:
    """The egress topics a provisioner must create (alongside ingestion topics)."""
    return [EGRESS_TOPIC, EGRESS_DLQ_TOPIC]


__all__ = ["EGRESS_TOPIC", "EGRESS_DLQ_TOPIC", "egress_topics"]
