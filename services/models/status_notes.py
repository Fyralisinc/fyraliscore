"""
services/models/status_notes.py — sidecar model_status_notes writer.

Post-Wave-0 A4: pseudo-code's `model.status_note = "..."` is translated
to INSERT INTO `model_status_notes (id, model_id, note, authored_by,
authored_at, kind)`. The `kind` column is one of:

  - 'first_person_override' — actor contested / amended a Model about them
  - 'manual'                — human-authored annotation
  - 'system'                — system lifecycle commentary
                              (falsifier triggered, decay archival, etc.)

This module exposes two async helpers:

  add_note(model_id, note, kind, authored_by=None, conn=...) -> ModelStatusNoteRow
  list_notes(model_id, conn=...)                             -> list[ModelStatusNoteRow]

Both accept an optional asyncpg connection; if none supplied they
acquire from the process-wide pool (lib.shared.db.get_pool). The
INSERT uses UUID v7 for the note id so note ordering is time-sortable.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.db import get_pool
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelStatusNoteKind, ModelStatusNoteRow


_LEGAL_KINDS: frozenset[str] = frozenset(
    ("first_person_override", "manual", "system")
)


async def add_note(
    model_id: UUID,
    note: str,
    kind: ModelStatusNoteKind,
    *,
    authored_by: UUID | None = None,
    conn: asyncpg.Connection | None = None,
) -> ModelStatusNoteRow:
    """
    Insert a new row into `model_status_notes` and return it.

    `kind` must be one of the three values in A4. Raises
    ValidationError for anything else so callers get a deterministic
    pre-DB error message.
    """
    if kind not in _LEGAL_KINDS:
        raise ValidationError(
            f"invalid model_status_notes.kind {kind!r}; "
            f"must be one of {sorted(_LEGAL_KINDS)}",
            field="kind",
            value=kind,
        )
    if not note or not note.strip():
        raise ValidationError(
            "note must be non-empty",
            field="note",
        )

    runner: Any = conn if conn is not None else get_pool()
    row = await runner.fetchrow(
        """
        INSERT INTO model_status_notes (
            id, model_id, note, authored_by, authored_at, kind
        ) VALUES (
            $1, $2, $3, $4, now(), $5
        )
        RETURNING id, model_id, note, authored_by, authored_at, kind
        """,
        uuid7(),
        model_id,
        note,
        authored_by,
        kind,
    )
    assert row is not None
    return ModelStatusNoteRow.model_validate(dict(row))


async def list_notes(
    model_id: UUID,
    *,
    conn: asyncpg.Connection | None = None,
    limit: int = 100,
) -> list[ModelStatusNoteRow]:
    """Return newest-first notes for a given model, up to `limit`."""
    runner: Any = conn if conn is not None else get_pool()
    rows = await runner.fetch(
        """
        SELECT id, model_id, note, authored_by, authored_at, kind
        FROM model_status_notes
        WHERE model_id = $1
        ORDER BY authored_at DESC, id DESC
        LIMIT $2
        """,
        model_id,
        limit,
    )
    return [ModelStatusNoteRow.model_validate(dict(r)) for r in rows]


__all__ = ["add_note", "list_notes"]
