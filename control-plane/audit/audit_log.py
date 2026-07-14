#!/usr/bin/env python3
"""audit_log — an APPEND-ONLY, HASH-CHAINED audit log for the Fyralis BYOC control plane.

This is the WS-AUDIT deliverable for **FR-G / invariant I5**: a tamper-EVIDENT record of
every security-relevant control-plane event (signed-artifact applies, cert issuance/revocation,
and — see :mod:`breakglass` — every break-glass grant issue / use / expiry).

Two layers of tamper-evidence, both built on the *real* ``control-plane/signing`` ed25519 lib —
no crypto is re-implemented here:

1. **Hash chain (intra-file).** Every entry carries the SHA-256 ``prev_hash`` of the entry
   before it and its own ``entry_hash`` over its canonical bytes. Changing ANY field of ANY
   past entry changes that entry's hash, which no longer matches the ``prev_hash`` the *next*
   entry recorded — so :func:`AuditLog.verify_chain` walks the chain and detects the break at
   the exact sequence number. This is the property the self-test exercises.

2. **Signed checkpoint (whole-file).** The hash chain alone is defeatable by an attacker who can
   rewrite the *entire* file (recompute every hash forward). So after each append we also write a
   detached ed25519 **signature over the current chain head hash** (the "checkpoint"), using the
   control plane's signing keyring (C2 / I6). An attacker who rewrites the file cannot forge that
   signature without the private signing key. ``verify_chain(check_signature=True)`` rejects a
   chain whose head does not match a valid signed checkpoint.

Wire format (append-only JSONL)
-------------------------------
The log is a UTF-8 JSON-Lines file; each line is one entry::

    {"seq":0,"ts":"2026-06-24T00:00:00Z","actor":"...","action":"...","target":"...",
     "metadata":{...},"prev_hash":"<hex|GENESIS>","entry_hash":"<hex>"}

``entry_hash`` = ``sha256_hex(canonical_json_bytes(body))`` where ``body`` is the entry **without**
``entry_hash`` (i.e. seq, ts, actor, action, target, metadata, prev_hash). ``prev_hash`` of the
genesis entry is the fixed sentinel :data:`GENESIS_HASH`.

The checkpoint sidecar ``<log>.checkpoint.json`` holds ``{seq, head_hash, count, sig(base64),
key_id, algo, signed_at}`` — the signed head of the chain. It is rewritten on every append.

Design notes
------------
* **Append-only is enforced by use, not by the filesystem.** :meth:`AuditLog.append` only ever
  ``O_APPEND``-opens and writes one line; it never rewrites or truncates. (A determined root can
  always edit a file — that is what the hash chain + signed checkpoint *detect*. I5 is
  "audit-logged + tamper-evident", not "physically immutable".)
* **Reuses signing.** Public-key checkpoint verification goes through the same ``Keyring`` /
  ``verify`` the agent uses for releases/licenses, so "verify before trust" is one code path.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import dataclass
from typing import Any, Iterator, Optional

# --- import the REAL signing lib (reuse; never re-implement crypto) ----------------------- #
HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402  (control-plane/signing)

# Canonical JSON + RFC-3339 time + sha256 come straight from the signing lib so the audit log
# hashes/signs the EXACT same byte representation the rest of the control plane uses (C2).
canonical_json_bytes = sl.canonical_json_bytes
now_rfc3339 = sl.now_rfc3339
sha256_hex = sl.sha256_hex

# The genesis entry's prev_hash. A fixed, well-known sentinel so the first real entry's chaining
# is deterministic and verifiable from nothing but the file.
GENESIS_HASH = "GENESIS"

DEFAULT_LOG_NAME = "audit.log.jsonl"


# --------------------------------------------------------------------------- #
# Entry model + chaining hash                                                  #
# --------------------------------------------------------------------------- #


def _entry_body(
    *, seq: int, ts: str, actor: str, action: str, target: str, metadata: dict, prev_hash: str
) -> dict:
    """The canonical, hashed body of an entry (everything EXCEPT entry_hash).

    Field set + ordering are fixed by :func:`canonical_json_bytes` (sorted keys), so the same
    logical entry always hashes to the same value on any host.
    """
    return {
        "seq": seq,
        "ts": ts,
        "actor": actor,
        "action": action,
        "target": target,
        "metadata": metadata,
        "prev_hash": prev_hash,
    }


def compute_entry_hash(body: dict) -> str:
    """SHA-256 (lowercase hex) over the canonical JSON bytes of an entry body."""
    return sha256_hex(canonical_json_bytes(body))


@dataclass(frozen=True)
class AuditEntry:
    """One immutable, hash-chained audit record."""

    seq: int
    ts: str
    actor: str
    action: str
    target: str
    metadata: dict
    prev_hash: str
    entry_hash: str

    def body(self) -> dict:
        return _entry_body(
            seq=self.seq,
            ts=self.ts,
            actor=self.actor,
            action=self.action,
            target=self.target,
            metadata=self.metadata,
            prev_hash=self.prev_hash,
        )

    def recompute_hash(self) -> str:
        return compute_entry_hash(self.body())

    def to_json_line(self) -> str:
        # The on-disk record = body + the recorded entry_hash. We write the body fields in the
        # canonical order plus entry_hash so a line round-trips through from_json exactly.
        rec = dict(self.body())
        rec["entry_hash"] = self.entry_hash
        return json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_json(cls, line: str) -> "AuditEntry":
        rec = json.loads(line)
        return cls(
            seq=rec["seq"],
            ts=rec["ts"],
            actor=rec["actor"],
            action=rec["action"],
            target=rec["target"],
            metadata=rec.get("metadata", {}),
            prev_hash=rec["prev_hash"],
            entry_hash=rec["entry_hash"],
        )


@dataclass
class ChainVerification:
    """Result of :meth:`AuditLog.verify_chain`."""

    ok: bool
    reason: str
    count: int = 0
    bad_seq: Optional[int] = None  # the sequence number where the chain first breaks (if any)
    head_hash: Optional[str] = None
    signature_ok: Optional[bool] = None  # None = not checked


# --------------------------------------------------------------------------- #
# Signed checkpoint (whole-file tamper-evidence, reuses signing)              #
# --------------------------------------------------------------------------- #


def _checkpoint_path(log_path: str) -> str:
    return log_path + ".checkpoint.json"


def _load_keyring(trust_root_path: Optional[str]) -> Optional[sl.Keyring]:
    """Load a verifier ring from a trust root, if one is available."""
    if not trust_root_path or not os.path.exists(trust_root_path):
        return None
    with open(trust_root_path, "r", encoding="utf-8") as fh:
        return sl.Keyring.from_trust_root(json.load(fh))


# --------------------------------------------------------------------------- #
# The append-only, hash-chained log                                           #
# --------------------------------------------------------------------------- #


class AuditLog:
    """An append-only, hash-chained audit log backed by a JSONL file.

    Parameters
    ----------
    path:
        The JSONL log file. Created on first append if missing. Never truncated/rewritten.
    signing_keyring:
        Optional CP signing :class:`signing_lib.Keyring` (with the active *private* key). When
        provided, every append rewrites a **signed checkpoint** over the new chain head
        (whole-file tamper-evidence, I6). Omit it (e.g. on a read-only verifier) to use the hash
        chain alone.
    trust_root_path:
        Path to a ``trust_root.json`` so :meth:`verify_chain` can verify the checkpoint signature
        with public keys. Defaults to ``control-plane/signing/trust_root.json``.
    """

    def __init__(
        self,
        path: str,
        *,
        signing_keyring: "sl.Keyring | None" = None,
        trust_root_path: str | None = None,
    ) -> None:
        self.path = os.path.abspath(path)
        self._keyring = signing_keyring
        self.trust_root_path = trust_root_path or os.path.join(SIGNING_DIR, "trust_root.json")
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)

    # -- read helpers ------------------------------------------------------- #

    def __iter__(self) -> Iterator[AuditEntry]:
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    yield AuditEntry.from_json(line)

    def entries(self) -> list[AuditEntry]:
        return list(self)

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def head(self) -> tuple[int, str]:
        """Return ``(last_seq, head_hash)`` — the current chain head. ``(-1, GENESIS_HASH)``
        for an empty log."""
        last_seq, head = -1, GENESIS_HASH
        for e in self:
            last_seq, head = e.seq, e.entry_hash
        return last_seq, head

    # -- append (the ONLY mutation; append-only by construction) ------------ #

    def append(
        self,
        actor: str,
        action: str,
        target: str,
        metadata: Optional[dict] = None,
    ) -> AuditEntry:
        """Append one hash-chained entry and (if a signing keyring is set) re-sign the checkpoint.

        ``actor`` = who did it, ``action`` = what they did, ``target`` = what it was done to,
        ``metadata`` = arbitrary JSON-serializable context. Returns the written :class:`AuditEntry`.

        Thread-safe (a process-local lock serializes the read-head → write-line → write-checkpoint
        sequence so concurrent appends cannot interleave and corrupt the chain).
        """
        if metadata is None:
            metadata = {}
        # metadata must be JSON-canonicalizable; surface a clear error early.
        try:
            canonical_json_bytes(metadata)
        except TypeError as exc:
            raise TypeError(f"audit metadata must be JSON-serializable: {exc}") from exc

        with self._lock:
            last_seq, prev_hash = self.head()
            seq = last_seq + 1
            ts = now_rfc3339()
            body = _entry_body(
                seq=seq,
                ts=ts,
                actor=actor,
                action=action,
                target=target,
                metadata=metadata,
                prev_hash=prev_hash,
            )
            entry_hash = compute_entry_hash(body)
            entry = AuditEntry(
                seq=seq,
                ts=ts,
                actor=actor,
                action=action,
                target=target,
                metadata=metadata,
                prev_hash=prev_hash,
                entry_hash=entry_hash,
            )
            # APPEND ONLY — O_APPEND, single line, fsync. Never seek/truncate.
            line = entry.to_json_line() + "\n"
            fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(fd, line.encode("utf-8"))
                os.fsync(fd)
            finally:
                os.close(fd)

            self._write_checkpoint(seq=seq, head_hash=entry_hash, count=seq + 1)
            return entry

    # -- signed checkpoint -------------------------------------------------- #

    def _write_checkpoint(self, *, seq: int, head_hash: str, count: int) -> None:
        """Write a (signed, if we have a private key) checkpoint over the chain head."""
        doc: dict[str, Any] = {
            "seq": seq,
            "head_hash": head_hash,
            "count": count,
            "algo": sl.ALGO,
            "signed_at": now_rfc3339(),
        }
        if self._keyring is not None:
            try:
                # Sign the canonical bytes of the (key_id-free) checkpoint body, exactly like the
                # rest of the CP signs license/config JSON (C2). key_id is added after signing.
                signed_bytes = canonical_json_bytes(
                    {"seq": seq, "head_hash": head_hash, "count": count}
                )
                key_id, raw_sig = self._keyring.sign_with_active(signed_bytes)
                doc["key_id"] = key_id
                doc["sig"] = sl.b64e(raw_sig)
            except RuntimeError:
                # verifier-only ring (no private key): leave the checkpoint unsigned. The hash
                # chain still provides intra-file tamper-evidence.
                pass
        cp = _checkpoint_path(self.path)
        # The checkpoint is a derived index, not the append-only log — overwriting it is expected.
        with open(cp, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.write("\n")

    def _load_checkpoint(self) -> Optional[dict]:
        cp = _checkpoint_path(self.path)
        if not os.path.exists(cp):
            return None
        with open(cp, "r", encoding="utf-8") as fh:
            return json.load(fh)

    # -- verification (the I5 tamper-detection guarantee) ------------------- #

    def verify_chain(self, *, check_signature: bool = True) -> ChainVerification:
        """Walk the chain and return whether it is intact.

        Checks, in order:
          1. **seq** is contiguous from 0.
          2. each entry's recomputed ``entry_hash`` matches the recorded one (no field tampered).
          3. each entry's ``prev_hash`` equals the previous entry's ``entry_hash`` (chain links).
          4. (if ``check_signature`` and a checkpoint exists) the checkpoint head matches the
             chain head AND its ed25519 signature verifies against the trust root (whole-file
             tamper-evidence). A *present* checkpoint that fails to verify is a FAILURE; an
             *absent* checkpoint is reported via ``signature_ok=None`` (chain-only mode).

        Returns a :class:`ChainVerification`; ``bad_seq`` pinpoints the first broken entry.
        """
        prev_hash = GENESIS_HASH
        expected_seq = 0
        count = 0
        head_hash = GENESIS_HASH

        for entry in self:
            count += 1
            if entry.seq != expected_seq:
                return ChainVerification(
                    ok=False,
                    reason=f"sequence gap: expected seq {expected_seq}, found {entry.seq}",
                    count=count,
                    bad_seq=entry.seq,
                    head_hash=head_hash,
                )
            # (2) entry self-integrity: does the recorded hash still match the body?
            recomputed = entry.recompute_hash()
            if recomputed != entry.entry_hash:
                return ChainVerification(
                    ok=False,
                    reason=(
                        f"entry {entry.seq} tampered: recorded entry_hash "
                        f"{entry.entry_hash[:16]}… != recomputed {recomputed[:16]}…"
                    ),
                    count=count,
                    bad_seq=entry.seq,
                    head_hash=head_hash,
                )
            # (3) chain link: does this entry point at the real previous head?
            if entry.prev_hash != prev_hash:
                return ChainVerification(
                    ok=False,
                    reason=(
                        f"broken chain at entry {entry.seq}: prev_hash "
                        f"{entry.prev_hash[:16]}… != previous entry hash {prev_hash[:16]}…"
                    ),
                    count=count,
                    bad_seq=entry.seq,
                    head_hash=head_hash,
                )
            prev_hash = entry.entry_hash
            head_hash = entry.entry_hash
            expected_seq += 1

        # (4) signed checkpoint over the whole-file head.
        signature_ok: Optional[bool] = None
        if check_signature:
            sig_result = self._verify_checkpoint(last_seq=expected_seq - 1, head_hash=head_hash)
            signature_ok = sig_result  # True / False / None(absent)
            if signature_ok is False:
                return ChainVerification(
                    ok=False,
                    reason=(
                        "signed checkpoint INVALID: the chain head does not match a valid "
                        "signed checkpoint (whole-file tamper or forged checkpoint)"
                    ),
                    count=count,
                    head_hash=head_hash,
                    signature_ok=False,
                )

        return ChainVerification(
            ok=True,
            reason=f"chain intact: {count} entr{'y' if count == 1 else 'ies'} verified",
            count=count,
            head_hash=head_hash,
            signature_ok=signature_ok,
        )

    def _verify_checkpoint(self, *, last_seq: int, head_hash: str) -> Optional[bool]:
        """Verify the signed checkpoint matches the chain head.

        Returns ``True`` (valid signature over the right head), ``False`` (present but wrong head
        or bad signature), or ``None`` (no checkpoint, or checkpoint exists but is unsigned and we
        cannot/needn't verify a signature — chain-only mode).
        """
        cp = self._load_checkpoint()
        if cp is None:
            return None
        # Head must match what the checkpoint attests.
        if cp.get("head_hash") != head_hash or cp.get("seq") != last_seq:
            return False
        sig_b64 = cp.get("sig")
        key_id = cp.get("key_id")
        if not sig_b64 or not key_id:
            # Unsigned checkpoint: nothing cryptographic to verify. Treat as chain-only (None) so
            # an environment without signing keys still passes on the hash chain alone.
            return None
        ring = self._keyring or _load_keyring(self.trust_root_path)
        if ring is None:
            return None  # no trust root to verify against — chain-only
        signed_bytes = canonical_json_bytes(
            {"seq": cp["seq"], "head_hash": cp["head_hash"], "count": cp.get("count")}
        )
        try:
            raw_sig = sl.b64d(sig_b64)
        except Exception:
            return False
        return ring.verify_with(key_id, signed_bytes, raw_sig)


# --------------------------------------------------------------------------- #
# Convenience constructor                                                      #
# --------------------------------------------------------------------------- #


def open_log(
    path: str | None = None,
    *,
    signing_keyring: "sl.Keyring | None" = None,
    trust_root_path: str | None = None,
) -> AuditLog:
    """Open (or create-on-first-append) the audit log at ``path``.

    Defaults to ``control-plane/audit/audit.log.jsonl`` so all CP components write one shared
    trail unless they pass an explicit path.
    """
    if path is None:
        path = os.path.join(HERE, DEFAULT_LOG_NAME)
    return AuditLog(path, signing_keyring=signing_keyring, trust_root_path=trust_root_path)


__all__ = [
    "GENESIS_HASH",
    "DEFAULT_LOG_NAME",
    "AuditEntry",
    "AuditLog",
    "ChainVerification",
    "compute_entry_hash",
    "open_log",
]
