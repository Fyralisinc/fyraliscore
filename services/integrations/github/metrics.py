"""services/integrations/github/metrics.py — IN-13 counters.

Aggregate-only labels (Clarifications Q5): no per-installation labels.
Per-installation drill-down is via structured log fields
(`installation_row_id`, `installation_id_hash`), not Prometheus labels.
"""
from __future__ import annotations

from threading import Lock
from typing import Final


# Test helpers: in-memory counters mirroring what we'd ship to the
# Prometheus registry. The actual Prometheus wiring is handled by the
# gateway's metrics shim; this module provides the canonical names plus
# count helpers used by both production code paths and tests.

_LOCK: Final[Lock] = Lock()
_COUNTERS: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
_HIST: dict[str, list[float]] = {}


def _key(name: str, **labels: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    return (name, tuple(sorted(labels.items())))


def _inc(name: str, **labels: str) -> None:
    with _LOCK:
        k = _key(name, **labels)
        _COUNTERS[k] = _COUNTERS.get(k, 0) + 1


def _observe(name: str, value: float, *, cap: int = 1024) -> None:
    with _LOCK:
        bucket = _HIST.setdefault(name, [])
        if len(bucket) >= cap:
            bucket.pop(0)
        bucket.append(value)


# ---------------------------------------------------------------------
# Webhook router counters (FR-017)
# ---------------------------------------------------------------------

def record_webhook_received() -> None:
    _inc("github_webhook_received_total")


def record_webhook_verified(result: str) -> None:
    """result ∈ {'ok', 'signature_failed', 'unknown_installation'}"""
    _inc("github_webhook_verified_total", result=result)


def record_signature_failure(reason: str) -> None:
    """reason ∈ {'signature_mismatch', 'malformed_signature_header',
    'missing_signature', 'unknown_installation', 'secret_not_configured'}"""
    _inc("github_webhook_signature_failure_total", reason=reason)


def record_replay_dropped() -> None:
    _inc("github_webhook_replay_dropped_total")


def record_replay_cache_bypass() -> None:
    _inc("github_webhook_replay_cache_bypass_total")


def record_filtered_repo(reason: str = "not_selected") -> None:
    _inc("github_webhook_filtered_repo_total", reason=reason)


def record_lifecycle(event: str, action: str) -> None:
    _inc("github_webhook_lifecycle_total", event=event, action=action)


# ---------------------------------------------------------------------
# Outbound client counters (FR-017)
# ---------------------------------------------------------------------

def record_installation_token_mint(result: str) -> None:
    """result ∈ {'ok', 'error'}"""
    _inc("github_installation_token_mint_total", result=result)


def record_outbound_request(path: str, status: int) -> None:
    """`path` is the endpoint template (not the substituted URL) for
    low-cardinality bounded labels."""
    _inc(
        "github_outbound_request_total",
        path=path,
        status=str(status),
    )


def record_outbound_chokepoint(reason: str) -> None:
    """reason ∈ {'bad_credentials', 'installation_not_found'}"""
    _inc("github_outbound_chokepoint_total", reason=reason)


# ---------------------------------------------------------------------
# Install/callback counters
# ---------------------------------------------------------------------

def record_install_callback(outcome: str) -> None:
    """outcome ∈ {'ok', 'state_invalid', 'state_expired', 'state_consumed',
    'installation_collision', 'token_mint_failed',
    'repository_fetch_failed', 'missing_installation_id'}"""
    _inc("github_install_callback_total", outcome=outcome)


# ---------------------------------------------------------------------
# Test introspection
# ---------------------------------------------------------------------

def get_counter(name: str, **labels: str) -> int:
    with _LOCK:
        return _COUNTERS.get(_key(name, **labels), 0)


def reset() -> None:
    """Test-only: clear all counters between tests."""
    with _LOCK:
        _COUNTERS.clear()
        _HIST.clear()


__all__ = [
    "record_webhook_received",
    "record_webhook_verified",
    "record_signature_failure",
    "record_replay_dropped",
    "record_replay_cache_bypass",
    "record_filtered_repo",
    "record_lifecycle",
    "record_installation_token_mint",
    "record_outbound_request",
    "record_outbound_chokepoint",
    "record_install_callback",
    "get_counter",
    "reset",
]
