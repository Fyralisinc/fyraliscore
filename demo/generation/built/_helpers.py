"""Helpers for hand-authored demo bundles. Deterministic UUIDs so the
emitted SQL is reproducible without a network round-trip."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone


# Namespace for demo bundle UUIDs. Random fixed UUID — anchor for
# uuid5() so that company_id + entity-key produces the same UUID on
# every run. Different companies get distinct keys via the company_id
# prefix in the name argument.
DEMO_NS = uuid.UUID("e0c8a000-0000-0000-0000-000000000d34")


def did(company: str, kind: str, key: str) -> str:
    """Deterministic UUID v5 keyed on (company_id, kind, key)."""
    return str(uuid.uuid5(DEMO_NS, f"{company}|{kind}|{key}"))


def days_ago(n: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()


def days_from_now(n: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=n)).isoformat()


def find_signals_containing(signals, *phrases, limit: int = 4) -> list[str]:
    """Helper used by Model authors: pick up to `limit` signal ids whose
    content_text contains any of the lower-cased phrases."""
    out: list[str] = []
    for s in signals:
        for p in phrases:
            if p.lower() in s.content_text.lower():
                out.append(s.id)
                break
        if len(out) >= limit:
            break
    return out
