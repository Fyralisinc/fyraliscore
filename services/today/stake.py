"""services/greeting/stake.py — Track BC card-payload enrichment.

Derive a structured `stake` value from a card's focus dict so the CEO
view's later Map surface can rank cards by stake (Tier 2 — UI rendering
not yet done). This module is the Tier 1 substrate plumbing: it produces
the data, the UI consumes it.

Rules (apply in order; first match wins):

  1. If a customer Resource is in focus with `revenue_at_risk` like
     "$487K", "$1.2M", or "$500,000" → `{"unit": "usd", "value": <int>}`.
     The parser handles suffixes K/M/B (case-insensitive), commas, and
     bare integers. Returns None on parse failure rather than crashing.
  2. Else if a Commitment in focus has `pressure` set → map
     low/medium/high to `{"unit": "risk", "value": 1/2/3}`.
  3. Else return None.

The `card_focus: dict` input is a permissive shape — see
`build_card_focus_dict_from_snapshot` in services/greeting/freshness.py
for the canonical extractor that the scheduler uses.
"""
from __future__ import annotations

import logging
import re
from typing import Any


log = logging.getLogger(__name__)


_PRESSURE_VALUES: dict[str, int] = {
    "low": 1,
    "medium": 2,
    "med": 2,        # tolerant of common abbreviation
    "high": 3,
}


# Match: optional currency symbol + digits/commas + optional decimal +
# optional suffix character (K/M/B). Tolerant of whitespace. We do not
# match negative numbers — revenue at risk is always positive or zero.
_USD_RE = re.compile(
    r"""
    ^\s*
    \$?\s*                          # optional dollar sign
    (?P<num>\d{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)
    \s*
    (?P<suffix>[KkMmBb])?
    \s*
    $
    """,
    re.VERBOSE,
)


def parse_revenue_at_risk(raw: Any) -> int | None:
    """Parse a pre-formatted revenue string into integer USD.

    Accepts: "$487K", "$1.2M", "$500,000", "$2B", "1500", "$1,234.50".
    Returns the integer dollar value (no fractional cents) or None when
    the input doesn't parse.

    Returns None — not zero — when parsing fails so callers can
    distinguish "no stake" from "$0 stake".
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if raw < 0:
            return None
        try:
            return int(raw)
        except (ValueError, OverflowError):
            return None
    if not isinstance(raw, str):
        return None
    m = _USD_RE.match(raw)
    if m is None:
        return None
    num_str = m.group("num").replace(",", "")
    try:
        value = float(num_str)
    except ValueError:
        return None
    if value < 0:
        return None
    suffix = (m.group("suffix") or "").lower()
    multiplier = {
        "": 1,
        "k": 1_000,
        "m": 1_000_000,
        "b": 1_000_000_000,
    }.get(suffix)
    if multiplier is None:
        return None
    return int(value * multiplier)


def derive_stake(card_focus: dict[str, Any] | None) -> dict[str, Any] | None:
    """Derive a structured stake from a card-focus dict.

    Returns one of:
      - `{"unit": "usd", "value": <int>}` — when a customer Resource
        with parseable `revenue_at_risk` is in focus.
      - `{"unit": "risk", "value": 1|2|3}` — when a Commitment with
        `pressure` in {low, medium, high} is in focus.
      - None — neither rule fires.

    All failures are silent (return None). The caller logs at the
    aggregate level so a single bad card doesn't pollute the log.
    """
    if not isinstance(card_focus, dict):
        return None

    # Rule 1: customer resource revenue_at_risk
    resource = card_focus.get("resource")
    if isinstance(resource, dict):
        # Only customer-ish resources count. Two conventions live in
        # the substrate: rendering uses `kind == "customer"`, the GRT
        # snapshot uses `kind == "relational"`. Either qualifies.
        kind = resource.get("kind")
        if kind in ("customer", "relational") or kind is None:
            rev = resource.get("revenue_at_risk")
            if rev is None:
                rev = resource.get("revenue_at_risk_usd")
            value = parse_revenue_at_risk(rev)
            if value is not None:
                return {"unit": "usd", "value": value}

    # Rule 2: commitment pressure
    commitment = card_focus.get("commitment")
    if isinstance(commitment, dict):
        pressure_raw = commitment.get("pressure")
        if isinstance(pressure_raw, str):
            mapped = _PRESSURE_VALUES.get(pressure_raw.strip().lower())
            if mapped is not None:
                return {"unit": "risk", "value": mapped}

    return None


__all__ = ["derive_stake", "parse_revenue_at_risk"]
