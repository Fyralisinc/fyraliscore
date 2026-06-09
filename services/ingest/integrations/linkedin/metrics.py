"""services/ingest/integrations/linkedin/metrics.py — lightweight in-process counters."""
from __future__ import annotations


_counters: dict[str, int] = {}


def record_request(outcome: str) -> None:
    """outcome ∈ {ok, rate_limited, error, unauthorized}."""
    key = f"linkedin.request.{outcome}"
    _counters[key] = _counters.get(key, 0) + 1


def record_provision_outcome(outcome: str) -> None:
    """outcome ∈ {success, no_entities, error}."""
    key = f"linkedin.provision.{outcome}"
    _counters[key] = _counters.get(key, 0) + 1


def snapshot() -> dict[str, int]:
    return dict(_counters)


def _reset_for_tests() -> None:
    _counters.clear()


__all__ = ["record_request", "record_provision_outcome", "snapshot"]
