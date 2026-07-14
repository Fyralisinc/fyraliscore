#!/usr/bin/env python3
"""revoke.py — the license revocation list (LRL) the validator consults (FR-F).

Why a separate list and not "just let it expire": FR-F requires we can **revoke a license
before its ``expires_at``** (tenant churned, plan downgraded, key compromise, contract
breach). The signature on a license is immortal — once signed it verifies forever — so the
only way to pull a still-valid, still-signed license is an out-of-band *deny list* the
validator checks on every ``validate()``. This file is that list.

Revocation is by **license_id** (the precise grant), or by **deployment_id**, or by
**tenant_id** (revoke everything that tenant holds). The validator denies if a license
matches ANY entry. The control plane ships this list to the agent the same way it ships
config (the agent re-reads it; a revocation written here takes effect on the next validate
without re-issuing anything).

On-disk format (``revocations.json``)::

    {
      "version": 1,
      "updated_at": "2026-06-24T00:00:00Z",
      "revocations": [
        {"type": "license_id",    "value": "lic-acme-3f9c1a2b", "reason": "compromised key", "revoked_at": "..."},
        {"type": "deployment_id", "value": "acme-use1-7f3a",    "reason": "decommissioned",  "revoked_at": "..."},
        {"type": "tenant_id",     "value": "acme",              "reason": "churned",         "revoked_at": "..."}
      ]
    }

CLI::

    python revoke.py add   --license-id lic-acme-3f9c1a2b --reason "key compromise"
    python revoke.py add   --deployment-id acme-use1-7f3a --reason "decommissioned"
    python revoke.py add   --tenant-id acme               --reason "churned"
    python revoke.py remove --license-id lic-acme-3f9c1a2b      # un-revoke (e.g. mistake)
    python revoke.py list
    python revoke.py check --license-id lic-acme-3f9c1a2b       # exit 0 = revoked, 1 = not

``is_revoked(license_dict_or_License, path=...)`` is the programmatic entry the validator imports.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Iterable

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from license_model import License, now_rfc3339  # noqa: E402

# The revocation list lives next to this module by default; ``REVOCATIONS_PATH`` overrides it
# (so a container can point at a persisted volume without a code change — see service.compose.yml).
DEFAULT_REVOCATIONS_PATH = os.environ.get(
    "REVOCATIONS_PATH", os.path.join(HERE, "revocations.json")
)

REVOKE_TYPES = ("license_id", "deployment_id", "tenant_id")


# --------------------------------------------------------------------------- #
# Load / save                                                                 #
# --------------------------------------------------------------------------- #


def _empty_list() -> dict:
    return {"version": 1, "updated_at": now_rfc3339(), "revocations": []}


def _resolve_path(path: str | None) -> str:
    """Resolve the revocation-list path at CALL time.

    Every public function defaults ``path`` to ``None`` and resolves it here, rather than
    binding ``DEFAULT_REVOCATIONS_PATH`` as a default argument value (which would freeze the
    path at import time). This keeps the read path (validator) and the write path (operator
    CLI/service) pointed at the *same* list even if a caller/test repoints
    ``DEFAULT_REVOCATIONS_PATH`` after import.
    """
    return path if path is not None else DEFAULT_REVOCATIONS_PATH


def load_revocations(path: str | None = None) -> dict:
    """Load the revocation list. A MISSING file means an EMPTY list (nothing revoked).

    Fail-closed nuance: a present-but-*unparseable* file raises (callers treat that as a
    hard error, not "nothing revoked") so a corrupted LRL never silently un-revokes
    everything. The validator surfaces such an error as deny.
    """
    path = _resolve_path(path)
    if not os.path.exists(path):
        return _empty_list()
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    if not isinstance(doc, dict) or not isinstance(doc.get("revocations"), list):
        raise ValueError(f"malformed revocation list at {path}: missing 'revocations' array")
    return doc


def save_revocations(doc: dict, path: str | None = None) -> None:
    path = _resolve_path(path)
    doc["updated_at"] = now_rfc3339()
    doc.setdefault("version", 1)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2, sort_keys=True)
        fh.write("\n")


def _entries(doc: dict) -> Iterable[dict]:
    return doc.get("revocations", [])


# --------------------------------------------------------------------------- #
# The predicate the validator calls                                           #
# --------------------------------------------------------------------------- #


def _as_license(lic) -> License:
    if isinstance(lic, License):
        return lic
    if isinstance(lic, dict):
        return License.from_dict(lic)
    raise TypeError("is_revoked expects a License, a license dict, or a path")


def revocation_match(lic: "License | dict", *, path: str | None = None) -> dict | None:
    """Return the FIRST matching revocation entry for ``lic``, or ``None`` if not revoked.

    Matching keys, in the order checked: ``license_id`` → ``deployment_id`` → ``tenant_id``.
    A license is revoked if it matches on *any* of them.
    """
    license_obj = _as_license(lic)
    doc = load_revocations(path)
    target = {
        "license_id": license_obj.license_id,
        "deployment_id": license_obj.deployment_id,
        "tenant_id": license_obj.tenant_id,
    }
    for entry in _entries(doc):
        etype = entry.get("type")
        evalue = entry.get("value")
        if etype in target and evalue and target[etype] == evalue:
            return entry
    return None


def is_revoked(lic: "License | dict", *, path: str | None = None) -> bool:
    """True iff ``lic`` is on the revocation list (what ``validator.validate`` consults)."""
    return revocation_match(lic, path=path) is not None


# --------------------------------------------------------------------------- #
# Mutators (CLI)                                                              #
# --------------------------------------------------------------------------- #


def add_revocation(
    *, rtype: str, value: str, reason: str = "", path: str | None = None
) -> dict:
    if rtype not in REVOKE_TYPES:
        raise ValueError(f"type must be one of {REVOKE_TYPES}, got {rtype!r}")
    value = (value or "").strip()
    if not value:
        raise ValueError("revocation value is required")
    doc = load_revocations(path)
    for entry in _entries(doc):
        if entry.get("type") == rtype and entry.get("value") == value:
            return entry  # idempotent: already revoked
    entry = {"type": rtype, "value": value, "reason": reason, "revoked_at": now_rfc3339()}
    doc["revocations"].append(entry)
    save_revocations(doc, path)
    return entry


def remove_revocation(
    *, rtype: str, value: str, path: str | None = None
) -> bool:
    doc = load_revocations(path)
    before = len(doc["revocations"])
    doc["revocations"] = [
        e for e in doc["revocations"] if not (e.get("type") == rtype and e.get("value") == value)
    ]
    removed = len(doc["revocations"]) < before
    if removed:
        save_revocations(doc, path)
    return removed


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def _one_target(args) -> tuple[str, str]:
    provided = [
        ("license_id", args.license_id),
        ("deployment_id", args.deployment_id),
        ("tenant_id", args.tenant_id),
    ]
    chosen = [(t, v) for t, v in provided if v]
    if len(chosen) != 1:
        raise SystemExit(
            "supply exactly one of --license-id / --deployment-id / --tenant-id"
        )
    return chosen[0]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="License revocation list (FR-F).")
    ap.add_argument("--path", default=DEFAULT_REVOCATIONS_PATH, help="revocations.json path")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def _add_targets(p):
        p.add_argument("--license-id")
        p.add_argument("--deployment-id")
        p.add_argument("--tenant-id")

    p_add = sub.add_parser("add", help="revoke a license / deployment / tenant")
    _add_targets(p_add)
    p_add.add_argument("--reason", default="")

    p_rm = sub.add_parser("remove", help="un-revoke (remove an entry)")
    _add_targets(p_rm)

    p_chk = sub.add_parser("check", help="exit 0 if the target is revoked, 1 if not")
    _add_targets(p_chk)

    sub.add_parser("list", help="print the revocation list")

    args = ap.parse_args(argv)

    if args.cmd == "add":
        rtype, value = _one_target(args)
        entry = add_revocation(rtype=rtype, value=value, reason=args.reason, path=args.path)
        print(f"revoked {rtype}={value!r}  reason={entry.get('reason')!r}  at={entry.get('revoked_at')}")
        return 0

    if args.cmd == "remove":
        rtype, value = _one_target(args)
        ok = remove_revocation(rtype=rtype, value=value, path=args.path)
        print(("removed" if ok else "no such entry: ") + f" {rtype}={value!r}")
        return 0 if ok else 1

    if args.cmd == "check":
        rtype, value = _one_target(args)
        doc = load_revocations(args.path)
        hit = any(e.get("type") == rtype and e.get("value") == value for e in _entries(doc))
        print(("REVOKED" if hit else "not revoked") + f": {rtype}={value!r}")
        return 0 if hit else 1

    if args.cmd == "list":
        doc = load_revocations(args.path)
        print(json.dumps(doc, indent=2, sort_keys=True))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
