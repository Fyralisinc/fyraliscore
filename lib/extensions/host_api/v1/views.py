"""lib.extensions.host_api.v1.views — frozen read *projections*.

Extensions read the substrate through these internals-hiding views, **never** raw
ORM rows. A projection deliberately omits operational/internal columns (embeddings,
sequence numbers, the raw provider payload at ``content["_raw"]``) so a core schema
change can't silently break an extension and so sensitive internals never leak by
default. Pure dataclasses + stdlib → safe under the ``lib`` →/→ ``services`` floor.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

# Keys stripped from `content` in any view by default — the raw provider payload
# is audit/replay-only and must not reach extension code through a read.
_REDACTED_CONTENT_KEYS = ("_raw",)


def _as_dict(value: Any) -> dict[str, Any]:
    """Coerce JSONB (dict, or str/bytes when no codec is registered) to a dict."""
    if value is None:
        return {}
    if isinstance(value, (str, bytes, bytearray)):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, bytes, bytearray)):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return []
    return list(value) if isinstance(value, (list, tuple)) else []


def _project_content(content: Any) -> dict[str, Any]:
    d = _as_dict(content)
    return {k: v for k, v in d.items() if k not in _REDACTED_CONTENT_KEYS}


@dataclass(frozen=True)
class ObservationView:
    """Stable read projection of an observation (system-of-record signal)."""

    id: UUID
    tenant_id: UUID
    occurred_at: datetime
    kind: str
    source_channel: str
    content: dict[str, Any]
    content_text: str
    trust_tier: str
    external_id: str | None = None
    entities_mentioned: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_row(cls, row: Any) -> "ObservationView":
        """Build from an asyncpg.Record / mapping / ObservationRow-like object."""
        g = row.get if hasattr(row, "get") else (lambda k, d=None: getattr(row, k, d))
        return cls(
            id=g("id"),
            tenant_id=g("tenant_id"),
            occurred_at=g("occurred_at"),
            kind=str(g("kind")),
            source_channel=g("source_channel"),
            content=_project_content(g("content")),
            content_text=g("content_text") or "",
            trust_tier=str(g("trust_tier")),
            external_id=g("external_id"),
            entities_mentioned=_as_list(g("entities_mentioned")),
        )


@dataclass(frozen=True)
class DraftView:
    """Read projection of a not-yet-persisted draft (for inspection)."""

    source_channel: str
    content: dict[str, Any]
    content_text: str
    occurred_at: datetime
    trust_tier: str
    external_id: str | None = None

    @classmethod
    def from_draft(cls, draft: Any) -> "DraftView":
        return cls(
            source_channel=draft.source_channel,
            content=_project_content(getattr(draft, "content", None)),
            content_text=getattr(draft, "content_text", "") or "",
            occurred_at=draft.occurred_at,
            trust_tier=str(getattr(draft, "trust_tier", "")),
            external_id=getattr(draft, "external_id", None),
        )


@dataclass(frozen=True)
class ModelView:
    """Stable read projection of a Model (a synthesized belief)."""

    id: UUID
    tenant_id: UUID
    proposition_kind: str
    status: str
    confidence: float
    proposition: dict[str, Any]
    natural: str
    created_at: datetime | None = None

    @classmethod
    def from_row(cls, row: Any) -> "ModelView":
        g = row.get if hasattr(row, "get") else (lambda k, d=None: getattr(row, k, d))
        return cls(
            id=g("id"),
            tenant_id=g("tenant_id"),
            proposition_kind=str(g("proposition_kind")),
            status=str(g("status")),
            confidence=float(g("confidence") or 0.0),
            proposition=_as_dict(g("proposition")),
            natural=g("natural") or "",
            created_at=g("created_at"),
        )


__all__ = ["ObservationView", "DraftView", "ModelView"]
