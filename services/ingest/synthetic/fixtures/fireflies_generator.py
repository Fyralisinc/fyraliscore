"""Fireflies transcripts fixture generator (meeting-transcript source).

`make_fireflies(*, workspace_id, transcripts=4, ...)` produces a deterministic
Fireflies install fixture shaped to feed `MockFirefliesClient`. It mirrors the
brex/github/gmail generators: every field is derived via `hashlib` (stable
across runs), timestamps land in 2026-01, and the shape is exactly what the mock
client paginates over.

Fixture shape (consumed by `MockFirefliesClient(fixture=...)`):
    {
      "workspace_id": "<workspace_id>",
      # newest-first transcript list paginated by the mock client.
      "transcripts": [ {<transcript>}, ... ],
      "page_size": 50,
    }

The fetcher (services/ingest/ingestion/fetchers/fireflies.py) emits, per
workspace: ONE `transcript` record per transcript (NO snapshot/balance record,
unlike the Brex archetype) — so the observation count per workspace is EXACTLY
`transcripts` (4 by default).

The transcript_id (and the workspace_id) are what the transcript `external_id`
keys on (`fireflies:{workspace_id}:transcript:{transcript_id}:{version}`), so a
distinct workspace_id per tenant + distinct transcript_ids per workspace makes
every observation's external_id tenant-unique — mirroring production where each
tenant's Fireflies workspace has distinct transcript ids. Without that, a
multi-tenant synthetic run collides on the global
`observations` UNIQUE(source_channel, external_id, occurred_at) index.
"""
from __future__ import annotations

import hashlib
from typing import Any


# Meeting titles cycled across a workspace's transcript stream.
_MEETING_TITLES = (
    "Weekly Engineering Sync",
    "Customer Discovery Call",
    "Product Roadmap Review",
    "Sales Pipeline Standup",
)


def make_fireflies(
    *,
    workspace_id: str,
    transcripts: int = 4,
    base_iso: str = "2026-01-05T00:00:00Z",
    page_size: int = 50,
    seed: str | None = None,
) -> dict[str, Any]:
    """Build a deterministic Fireflies install fixture.

    Args:
      workspace_id: The Fireflies workspace the install is scoped to (namespaces
        every transcript's external_id).
      transcripts: Number of meeting transcripts on the workspace stream. The
        handler emits exactly one observation per transcript, so this is the
        per-workspace observation count.
      base_iso: Anchor timestamp (2026-01); transcripts are spaced backwards from
        it so the list is newest-first (Fireflies' ordering).
      page_size: The mock client's per-page cap for `list_transcripts`.
      seed: Optional namespacing salt mixed into the synthetic transcript ids.
        None/"" reproduces the original ids; a per-tenant value makes the
        transcript ids — and therefore every observation's external_id —
        additionally tenant-unique (the workspace_id already namespaces them).

    Returns:
      Fixture dict consumable by `MockFirefliesClient(fixture=...)`.
    """
    base_date = base_iso[:10]  # YYYY-MM-DD anchor for spacing.
    salt = seed or ""

    txns = [
        _transcript(workspace_id, salt, idx, base_date)
        for idx in range(transcripts)
    ]

    return {
        "workspace_id": workspace_id,
        "transcripts": txns,
        "page_size": page_size,
    }


def _transcript(
    workspace_id: str, salt: str, idx: int, base_date: str,
) -> dict[str, Any]:
    """One deterministic Fireflies transcript, newest-first by `idx`.

    idx=0 is the newest; later indices are older — matching Fireflies' listing
    order. `dateTime` lands in 2026-01 so the handler's occurred_at is always a
    2026 timestamp.
    """
    _id_parts = (workspace_id, "transcript", idx) if not salt else (workspace_id, salt, "transcript", idx)
    transcript_id = f"ts_{_digest(*_id_parts)[:20]}"
    title = _MEETING_TITLES[idx % len(_MEETING_TITLES)] + f" #{idx + 1}"
    # Space transcripts one hour apart, newest first: idx 0 -> 23:00, etc.
    hour = 23 - (idx % 24)
    iso = f"{base_date}T{hour:02d}:00:00Z"
    speaker_a = f"Alice {_digest(transcript_id, 'a')[:4]}"
    speaker_b = f"Bob {_digest(transcript_id, 'b')[:4]}"
    return {
        "id": transcript_id,
        "transcriptId": transcript_id,
        "workspaceId": workspace_id,
        "title": title,
        "dateTime": iso,
        "date": iso,
        # A content version so the external_id is stable but re-version-able.
        "version": iso,
        "duration": 30 + (idx % 4) * 15,
        "organizerEmail": f"organizer-{idx}@acme.example",
        "participants": [
            {"name": speaker_a, "email": f"alice{idx}@acme.example"},
            {"name": speaker_b, "email": f"bob{idx}@acme.example"},
        ],
        "summary": {
            "overview": f"Discussion of {title.lower()} and follow-ups.",
            "action_items": [f"Follow up on item {idx}"],
        },
        "meetingLink": f"https://app.fireflies.ai/view/{transcript_id}",
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()


__all__ = ["make_fireflies"]
