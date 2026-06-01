"""services/integrations/discord/gateway/metrics.py — IN-12 Gateway counters.

In-process counters/gauges scraped by structlog log emission. Matches the
shape of `services/integrations/discord/metrics.py` for consistency.

Eight surfaces per FR-011:
  discord_gateway_connection_state{state}         gauge: 0|1 per state
  discord_gateway_reconnect_total{reason}         counter
  discord_gateway_dispatch_total{event}           counter
  discord_gateway_messages_total                  counter (post-filter)
  discord_gateway_filtered_bot_total{source}      counter
  discord_gateway_dropped_unknown_installation_total  counter
  discord_gateway_connect_failure_total           counter
  discord_gateway_heartbeat_miss_total            counter
"""
from __future__ import annotations

from collections import Counter, defaultdict


_counters: Counter[tuple[str, frozenset[tuple[str, str]]]] = Counter()
_gauges: dict[tuple[str, frozenset[tuple[str, str]]], float] = defaultdict(float)


def _key(name: str, labels: dict[str, str]) -> tuple[str, frozenset[tuple[str, str]]]:
    return (name, frozenset(labels.items()))


def inc(name: str, **labels: str) -> None:
    """Increment a counter by 1."""
    _counters[_key(name, labels)] += 1


def add(name: str, value: float, **labels: str) -> None:
    """Increment a counter by an arbitrary value."""
    _counters[_key(name, labels)] += value


def set_gauge(name: str, value: float, **labels: str) -> None:
    """Set a gauge to an absolute value."""
    _gauges[_key(name, labels)] = value


def get(name: str, **labels: str) -> float:
    """Return the current counter value (0.0 if never incremented)."""
    return float(_counters.get(_key(name, labels), 0))


def get_gauge(name: str, **labels: str) -> float:
    """Return the current gauge value."""
    return _gauges.get(_key(name, labels), 0.0)


def snapshot() -> dict[str, float]:
    """Return a flat name+labels → value mapping for log emission."""
    out: dict[str, float] = {}
    for (name, label_set), value in _counters.items():
        label_str = ",".join(f"{k}={v}" for k, v in sorted(label_set))
        key = f"{name}{{{label_str}}}" if label_set else name
        out[key] = value
    for (name, label_set), value in _gauges.items():
        label_str = ",".join(f"{k}={v}" for k, v in sorted(label_set))
        key = f"{name}{{{label_str}}}" if label_set else name
        out[key] = value
    return out


def reset() -> None:
    """Reset all counters and gauges (test helper)."""
    _counters.clear()
    _gauges.clear()


__all__ = ["inc", "add", "set_gauge", "get", "get_gauge", "snapshot", "reset"]
