#!/usr/bin/env python3
"""breakglass — the break-glass emergency-access workflow for the Fyralis BYOC control plane.

WS-AUDIT deliverable for **FR-G / invariant I5**: *break-glass access is customer-granted, scoped,
time-boxed, and audit-logged.* This module implements exactly that contract:

* **customer-granted** — a grant is *requested* by an operator (``request_grant``) but is INERT
  until the **customer approves** it (``approve_grant``). An unapproved (or denied) grant never
  authorizes anything. (No tenant can self-approve its own access on the vendor's behalf — the
  approval step is a distinct actor.)
* **scoped** — a grant authorizes one ``scope`` string (e.g. ``tenant:acme/logs:read``). It
  authorizes nothing else; ``check_access`` matches the scope exactly (with an explicit, opt-in
  wildcard form ``tenant:acme/*`` for sub-scopes — see :func:`scope_matches`).
* **time-boxed** — every grant carries a ``ttl`` (seconds). It auto-expires ``ttl`` seconds after
  approval; after that, access is DENIED. Expiry is wall-clock and lazy: any access check past the
  window denies and emits an ``expire`` audit event exactly once.
* **audit-logged** — every lifecycle transition (request, approve, deny, USE, expire, revoke) is
  written to the **hash-chained** :class:`audit_log.AuditLog` (so the break-glass trail is itself
  tamper-evident, I5 + the WS-AUDIT chain). The grant store persists alongside the log.

Lifecycle::

    request_grant ──approve_grant──▶ APPROVED ──(check_access within ttl)──▶ allowed (audited USE)
         │                              │
         │ deny_grant                   │ ttl elapses / revoke_grant
         ▼                              ▼
       DENIED                         EXPIRED / REVOKED  ──▶ check_access DENIED

State is persisted to ``<store>`` (JSON), and the **audit log is the source of truth for the
event history** — the store is a fast-lookup projection. Everything tamper-evident rides the
hash-chained log (which itself reuses ``control-plane/signing`` for its signed checkpoint, I6).
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import audit_log as al  # noqa: E402

# Grant lifecycle states.
STATE_REQUESTED = "requested"
STATE_APPROVED = "approved"
STATE_DENIED = "denied"
STATE_EXPIRED = "expired"
STATE_REVOKED = "revoked"

# Audit actions emitted by the break-glass workflow (FR-G / I5).
ACTION_REQUEST = "breakglass.request"
ACTION_APPROVE = "breakglass.approve"
ACTION_DENY = "breakglass.deny"
ACTION_USE = "breakglass.use"
ACTION_EXPIRE = "breakglass.expire"
ACTION_REVOKE = "breakglass.revoke"
ACTION_CHECK_DENIED = "breakglass.check_denied"

DEFAULT_STORE_NAME = "breakglass_grants.json"


# --------------------------------------------------------------------------- #
# Grant model                                                                  #
# --------------------------------------------------------------------------- #


@dataclass
class Grant:
    """One break-glass grant: a scoped, time-boxed, customer-approvable access token."""

    grant_id: str
    actor: str            # the operator who will use the elevated access
    scope: str            # the single scope this grant authorizes
    ttl_seconds: float    # time-box (seconds) — starts counting at approval
    requested_by: str     # who requested it (usually == actor)
    state: str = STATE_REQUESTED
    requested_at: float = 0.0          # epoch seconds
    approved_at: Optional[float] = None  # epoch seconds, set on approval (starts the clock)
    approved_by: Optional[str] = None    # the CUSTOMER principal who approved (customer-granted)
    expires_at: Optional[float] = None   # epoch seconds == approved_at + ttl
    reason: str = ""
    use_count: int = 0

    def is_active(self, now: Optional[float] = None) -> bool:
        """True iff approved AND not past its expiry window AND not revoked/denied."""
        if self.state != STATE_APPROVED:
            return False
        now = time.time() if now is None else now
        return self.expires_at is not None and now < self.expires_at

    def has_expired(self, now: Optional[float] = None) -> bool:
        """True iff this is an approved grant whose window has elapsed (needs an expire event)."""
        if self.state != STATE_APPROVED or self.expires_at is None:
            return False
        now = time.time() if now is None else now
        return now >= self.expires_at

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Grant":
        return cls(**d)


# --------------------------------------------------------------------------- #
# Scope matching                                                               #
# --------------------------------------------------------------------------- #


def scope_matches(granted: str, requested: str) -> bool:
    """Does a grant for ``granted`` authorize an access request for ``requested``?

    * Exact match authorizes (``a/b == a/b``).
    * A trailing ``/*`` wildcard on the *granted* scope authorizes any sub-scope:
      ``tenant:acme/*`` authorizes ``tenant:acme/logs:read`` but NOT ``tenant:other/...``.
    * Nothing else — a grant is scoped; it never authorizes a broader or sibling scope. (There is
      deliberately no implicit prefix matching; you must opt into ``/*``.)
    """
    if granted == requested:
        return True
    if granted.endswith("/*"):
        prefix = granted[:-1]  # keep the trailing slash: "tenant:acme/"
        return requested.startswith(prefix)
    return False


# --------------------------------------------------------------------------- #
# The break-glass manager                                                      #
# --------------------------------------------------------------------------- #


@dataclass
class AccessDecision:
    """Result of :meth:`BreakGlass.check_access`."""

    allowed: bool
    reason: str
    grant_id: Optional[str] = None
    scope: Optional[str] = None


class BreakGlass:
    """Break-glass workflow over a hash-chained audit log (FR-G / I5).

    Parameters
    ----------
    audit:
        The :class:`audit_log.AuditLog` every lifecycle event is written to (the tamper-evident
        trail). Required — break-glass with no audit trail would violate I5.
    store_path:
        JSON file the grant projection is persisted to (defaults next to the audit log). The audit
        log is the authoritative history; this is the fast-lookup state.
    """

    def __init__(self, audit: al.AuditLog, *, store_path: str | None = None) -> None:
        self.audit = audit
        if store_path is None:
            store_path = os.path.join(os.path.dirname(audit.path) or ".", DEFAULT_STORE_NAME)
        self.store_path = os.path.abspath(store_path)
        self._lock = threading.RLock()
        self._grants: dict[str, Grant] = {}
        self._load()

    # -- persistence -------------------------------------------------------- #

    def _load(self) -> None:
        if os.path.exists(self.store_path):
            with open(self.store_path, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            for g in doc.get("grants", []):
                grant = Grant.from_dict(g)
                self._grants[grant.grant_id] = grant

    def _save(self) -> None:
        doc = {"version": 1, "grants": [g.to_dict() for g in self._grants.values()]}
        tmp = self.store_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, self.store_path)  # atomic projection swap

    # -- lifecycle: request -> approve/deny -> use -> expire/revoke --------- #

    def request_grant(
        self,
        actor: str,
        scope: str,
        ttl: float,
        *,
        reason: str = "",
        requested_by: str | None = None,
    ) -> Grant:
        """Request a break-glass grant. Creates a ``requested`` grant (INERT until approved) and
        audits the request.

        ``actor`` will hold the elevated access; ``scope`` is the single scope; ``ttl`` is the
        time-box in seconds (counted from *approval*, not from request). Returns the :class:`Grant`.
        """
        if ttl <= 0:
            raise ValueError("ttl must be positive (a time-boxed grant)")
        with self._lock:
            grant = Grant(
                grant_id="bg-" + uuid.uuid4().hex[:12],
                actor=actor,
                scope=scope,
                ttl_seconds=float(ttl),
                requested_by=requested_by or actor,
                state=STATE_REQUESTED,
                requested_at=time.time(),
                reason=reason,
            )
            self._grants[grant.grant_id] = grant
            self._save()
            self.audit.append(
                actor=grant.requested_by,
                action=ACTION_REQUEST,
                target=scope,
                metadata={
                    "grant_id": grant.grant_id,
                    "for_actor": actor,
                    "ttl_seconds": grant.ttl_seconds,
                    "reason": reason,
                },
            )
            return grant

    def approve_grant(self, grant_id: str, approved_by: str) -> Grant:
        """**Customer-grant** the request: approve it, starting the time-box.

        ``approved_by`` is the CUSTOMER principal (distinct from the vendor operator requesting).
        Sets ``approved_at``/``expires_at`` and audits the approval. Only a ``requested`` grant can
        be approved.
        """
        with self._lock:
            grant = self._require(grant_id)
            if grant.state != STATE_REQUESTED:
                raise ValueError(
                    f"grant {grant_id} cannot be approved from state {grant.state!r}"
                )
            now = time.time()
            grant.state = STATE_APPROVED
            grant.approved_at = now
            grant.approved_by = approved_by
            grant.expires_at = now + grant.ttl_seconds
            self._save()
            self.audit.append(
                actor=approved_by,
                action=ACTION_APPROVE,
                target=grant.scope,
                metadata={
                    "grant_id": grant.grant_id,
                    "for_actor": grant.actor,
                    "approved_by": approved_by,
                    "ttl_seconds": grant.ttl_seconds,
                    "expires_at": al.now_rfc3339(),  # human-readable approval instant
                },
            )
            return grant

    def deny_grant(self, grant_id: str, denied_by: str, *, reason: str = "") -> Grant:
        """Customer denies the request. The grant authorizes nothing; audited."""
        with self._lock:
            grant = self._require(grant_id)
            if grant.state != STATE_REQUESTED:
                raise ValueError(f"grant {grant_id} cannot be denied from state {grant.state!r}")
            grant.state = STATE_DENIED
            self._save()
            self.audit.append(
                actor=denied_by,
                action=ACTION_DENY,
                target=grant.scope,
                metadata={"grant_id": grant.grant_id, "for_actor": grant.actor, "reason": reason},
            )
            return grant

    def revoke_grant(self, grant_id: str, revoked_by: str, *, reason: str = "") -> Grant:
        """Revoke an approved grant before its TTL elapses (kill-switch). Audited."""
        with self._lock:
            grant = self._require(grant_id)
            if grant.state not in (STATE_APPROVED, STATE_REQUESTED):
                raise ValueError(f"grant {grant_id} cannot be revoked from state {grant.state!r}")
            grant.state = STATE_REVOKED
            self._save()
            self.audit.append(
                actor=revoked_by,
                action=ACTION_REVOKE,
                target=grant.scope,
                metadata={"grant_id": grant.grant_id, "for_actor": grant.actor, "reason": reason},
            )
            return grant

    def check_access(self, actor: str, scope: str, *, now: float | None = None) -> AccessDecision:
        """Decide whether ``actor`` may access ``scope`` RIGHT NOW under any live grant.

        Honors ONLY grants that are approved (customer-granted), unexpired (time-boxed), and whose
        scope authorizes ``scope`` (scoped). A successful check is itself audited as a **USE**
        event (the break-glass access is recorded each time it is exercised, I5). Lazily expires
        any approved grant whose window has elapsed, emitting an ``expire`` audit event exactly
        once. Returns an :class:`AccessDecision`.
        """
        now = time.time() if now is None else now
        with self._lock:
            self._sweep_expirations(now=now)  # emit expire events for any elapsed grants first

            for grant in self._grants.values():
                if grant.actor != actor:
                    continue
                if not grant.is_active(now=now):
                    continue
                if not scope_matches(grant.scope, scope):
                    continue
                # Live, in-scope grant — ALLOW and audit the USE.
                grant.use_count += 1
                self._save()
                self.audit.append(
                    actor=actor,
                    action=ACTION_USE,
                    target=scope,
                    metadata={
                        "grant_id": grant.grant_id,
                        "granted_scope": grant.scope,
                        "use_count": grant.use_count,
                    },
                )
                return AccessDecision(
                    allowed=True,
                    reason=f"granted by {grant.grant_id} (scope {grant.scope!r})",
                    grant_id=grant.grant_id,
                    scope=grant.scope,
                )

            # No live grant authorizes this — DENY (and audit the denied attempt).
            self.audit.append(
                actor=actor,
                action=ACTION_CHECK_DENIED,
                target=scope,
                metadata={"reason": "no unexpired approved grant authorizes this scope"},
            )
            return AccessDecision(
                allowed=False,
                reason="denied: no unexpired, approved, in-scope break-glass grant",
            )

    # -- expiry sweep ------------------------------------------------------- #

    def sweep_expirations(self, *, now: float | None = None) -> list[str]:
        """Public entrypoint: expire any elapsed grants now, audit each, return their ids.

        A daemon/cron can call this so expiry is recorded promptly even with no access check.
        """
        with self._lock:
            return self._sweep_expirations(now=time.time() if now is None else now)

    def _sweep_expirations(self, *, now: float) -> list[str]:
        expired: list[str] = []
        changed = False
        for grant in self._grants.values():
            if grant.has_expired(now=now):
                grant.state = STATE_EXPIRED
                changed = True
                expired.append(grant.grant_id)
                self.audit.append(
                    actor="system",
                    action=ACTION_EXPIRE,
                    target=grant.scope,
                    metadata={
                        "grant_id": grant.grant_id,
                        "for_actor": grant.actor,
                        "ttl_seconds": grant.ttl_seconds,
                    },
                )
        if changed:
            self._save()
        return expired

    # -- accessors ---------------------------------------------------------- #

    def _require(self, grant_id: str) -> Grant:
        grant = self._grants.get(grant_id)
        if grant is None:
            raise KeyError(f"unknown grant: {grant_id}")
        return grant

    def get(self, grant_id: str) -> Optional[Grant]:
        return self._grants.get(grant_id)

    def grants(self) -> list[Grant]:
        return list(self._grants.values())

    def active_grants(self, *, now: float | None = None) -> list[Grant]:
        now = time.time() if now is None else now
        return [g for g in self._grants.values() if g.is_active(now=now)]


__all__ = [
    "Grant",
    "BreakGlass",
    "AccessDecision",
    "scope_matches",
    "STATE_REQUESTED",
    "STATE_APPROVED",
    "STATE_DENIED",
    "STATE_EXPIRED",
    "STATE_REVOKED",
    "ACTION_REQUEST",
    "ACTION_APPROVE",
    "ACTION_DENY",
    "ACTION_USE",
    "ACTION_EXPIRE",
    "ACTION_REVOKE",
    "ACTION_CHECK_DENIED",
]
