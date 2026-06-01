"""services/integrations/gmail/threading.py — RFC 5322 thread canonicalization.

The 10-mailbox duplication problem stated in the spec context:

    A single internal thread of 10 participants exists as 10 mailbox
    copies. Ingested per-user (which is the only way Gmail lets us
    ingest), the same thread arrives 10 times. If we write naively,
    every signal is multiplied by participant count.

This module collapses those arrivals to one canonical row per RFC 5322
thread, keyed by the chain Message-ID → In-Reply-To → References. The
resolved canonical_thread_id is stamped onto every observation so the
downstream Bridge Layer reads a thread as one unit regardless of how
many mailboxes saw it.

Resolution algorithm
--------------------
For a new message with Message-ID = M, In-Reply-To = R0, References = [R1, R2, …]:

  1. If gmail_thread_members already maps M → some canonical id → return it.
     (Idempotent fast-path: same Message-ID across mailboxes collapses here.)

  2. Walk R0, then References last-to-first, against gmail_thread_members.
     First hit wins — adopt that canonical id; insert M as a new member.

  3. No hit anywhere → M is a new root. Insert a gmail_threads_canonical
     row keyed on M; insert M as its first member.

  4. Update participant_emails (set union), last_seen_at, message_count
     atomically for the resolved canonical row.

Edge cases (documented, accepted in v1):
  - Forwarded threads with broken References → new root (no subject heuristic).
  - Out-of-order arrival (child before parent) → child becomes its own root;
    when parent arrives later, we do NOT merge. The observation carries
    content._orphan_thread=true for downstream visibility.
  - Same root Message-ID hash-collision across installs is impossible:
    PK includes gmail_installation_id.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.tenant_context import TenantContext


_RE_PREFIX = re.compile(r"^\s*(?:re|fwd|fw|aw)\s*:\s*", flags=re.IGNORECASE)
_WS_RUN = re.compile(r"\s+")


def normalize_message_id(raw: str | None) -> str | None:
    """Strip surrounding whitespace and angle brackets. Lowercase the
    domain portion only — local-part of a Message-ID is case-sensitive
    per RFC 5322 §3.6.4, even though most generators are case-insensitive.
    For dedup purposes we keep the original local-part as-is and only
    fold whitespace + brackets.
    """
    if not raw:
        return None
    s = raw.strip()
    if s.startswith("<") and s.endswith(">"):
        s = s[1:-1]
    s = s.strip()
    return s or None


def split_references(raw: str | None) -> list[str]:
    """Split a References header value into individual Message-IDs."""
    if not raw:
        return []
    out: list[str] = []
    for token in raw.replace(",", " ").split():
        norm = normalize_message_id(token)
        if norm:
            out.append(norm)
    return out


def normalize_subject(raw: str | None) -> str | None:
    if not raw:
        return None
    s = _RE_PREFIX.sub("", raw)
    # Strip nested 'Re:' chains, e.g. "Re: Re: Re: foo".
    while True:
        new = _RE_PREFIX.sub("", s)
        if new == s:
            break
        s = new
    s = _WS_RUN.sub(" ", s).strip()
    return s or None


def normalize_participants(raw: list[str]) -> list[str]:
    """Lowercase + de-dupe a list of participant emails."""
    seen: set[str] = set()
    out: list[str] = []
    for addr in raw:
        if not isinstance(addr, str):
            continue
        lower = addr.strip().lower()
        if not lower or lower in seen:
            continue
        seen.add(lower)
        out.append(lower)
    return out


@dataclass(frozen=True)
class ThreadCanonicalization:
    canonical_id: UUID
    is_new_thread: bool
    orphan: bool  # True if no parent reference matched and References was non-empty


async def canonicalize_thread(
    tctx: TenantContext,
    *,
    gmail_installation_id: UUID,
    message_id: str,
    in_reply_to: str | None,
    references: list[str],
    subject: str | None,
    participants: list[str],
) -> ThreadCanonicalization:
    """Resolve the canonical thread for a Gmail message.

    Idempotent: re-running for the same (installation, message_id)
    returns the same canonical_id without inserting duplicates.

    Caller must already hold a tenant-bound transaction on `tctx`.
    """
    msg_id = normalize_message_id(message_id)
    if not msg_id:
        raise ValueError("message_id is required and must be non-empty after normalization")

    parent_candidates: list[str] = []
    if in_reply_to:
        parent = normalize_message_id(in_reply_to)
        if parent:
            parent_candidates.append(parent)
    # References last-to-first: the last entry is the immediate parent.
    parent_candidates.extend(reversed(split_references(",".join(references))))

    parts_norm = normalize_participants(participants)
    subject_norm = normalize_subject(subject)

    # --- step 1: fast-path: this message_id already a member?
    existing = await tctx.fetchrow(
        """
        SELECT thread_canonical_id
        FROM gmail_thread_members
        WHERE gmail_installation_id = $1 AND message_id = $2
        """,
        gmail_installation_id, msg_id,
    )
    if existing is not None:
        canonical_id: UUID = existing["thread_canonical_id"]
        await _touch_thread(
            tctx,
            canonical_id=canonical_id,
            participants=parts_norm,
        )
        return ThreadCanonicalization(canonical_id=canonical_id, is_new_thread=False, orphan=False)

    # --- step 2: walk parent chain.
    canonical_id = None  # type: ignore[assignment]
    for parent in parent_candidates:
        row = await tctx.fetchrow(
            """
            SELECT thread_canonical_id
            FROM gmail_thread_members
            WHERE gmail_installation_id = $1 AND message_id = $2
            """,
            gmail_installation_id, parent,
        )
        if row is not None:
            canonical_id = row["thread_canonical_id"]
            break

    is_new_thread = canonical_id is None
    orphan = bool(parent_candidates) and is_new_thread

    if is_new_thread:
        # --- step 3: new root.
        canonical_id = uuid7()
        await tctx.execute(
            """
            INSERT INTO gmail_threads_canonical (
              id, tenant_id, gmail_installation_id,
              canonical_message_id, subject_normalized,
              participant_emails, message_count
            ) VALUES (
              $1, $2, $3, $4, $5, $6::text[], 1
            )
            ON CONFLICT (gmail_installation_id, canonical_message_id) DO NOTHING
            """,
            canonical_id, tctx.tenant_id, gmail_installation_id,
            msg_id, subject_norm, parts_norm,
        )
        # Re-fetch in case ON CONFLICT skipped (a parallel arrival of the
        # same root): the existing row wins.
        row = await tctx.fetchrow(
            """
            SELECT id FROM gmail_threads_canonical
            WHERE gmail_installation_id = $1 AND canonical_message_id = $2
            """,
            gmail_installation_id, msg_id,
        )
        if row is None:
            raise RuntimeError("thread row vanished after upsert — invariant broken")
        canonical_id = row["id"]
    else:
        await _touch_thread(
            tctx,
            canonical_id=canonical_id,  # type: ignore[arg-type]
            participants=parts_norm,
        )

    # --- step 4: insert membership. ON CONFLICT covers concurrent same-message arrivals.
    await tctx.execute(
        """
        INSERT INTO gmail_thread_members (
          gmail_installation_id, message_id, tenant_id, thread_canonical_id
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (gmail_installation_id, message_id) DO NOTHING
        """,
        gmail_installation_id, msg_id, tctx.tenant_id, canonical_id,
    )

    return ThreadCanonicalization(
        canonical_id=canonical_id,  # type: ignore[arg-type]
        is_new_thread=is_new_thread,
        orphan=orphan,
    )


async def _touch_thread(
    tctx: TenantContext,
    *,
    canonical_id: UUID,
    participants: list[str],
) -> None:
    """Update last_seen_at, message_count, and union participant_emails.

    Done as a single UPDATE; race with another concurrent ingest is
    benign because both will produce the same union (set semantics) and
    last_seen_at is monotone non-decreasing under now().
    """
    await tctx.execute(
        """
        UPDATE gmail_threads_canonical
           SET last_seen_at = now(),
               message_count = message_count + 1,
               participant_emails = (
                 SELECT ARRAY(
                   SELECT DISTINCT unnest(
                     participant_emails || $2::text[]
                   )
                 )
               )
         WHERE id = $1
        """,
        canonical_id, participants,
    )


__all__ = [
    "ThreadCanonicalization",
    "canonicalize_thread",
    "normalize_message_id",
    "normalize_participants",
    "normalize_subject",
    "split_references",
]
