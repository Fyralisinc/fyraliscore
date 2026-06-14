"""services.platform.extensions.egress — the capability-filtered egress plane (E3.1).

The faithful Kafka projection that delivers redacted observations to
developer-hosted extensions:

  normalized observation ──[projector]──▶ ext.egress.v1 (Kafka) ──[delivery]──▶
      extension_egress outbox (cursor PULL)  +  HMAC webhook PUSH

``planner.plan_egress`` is the pure decision core (which extensions get this
observation, redacted how) shared by the projector and the unit tests. The Kafka
workers (``projector``/``delivery``) are thin wrappers; the pull endpoint + SDK
consumer read the outbox.
"""
from services.platform.extensions.egress.planner import EgressItem, plan_egress

__all__ = ["EgressItem", "plan_egress"]
