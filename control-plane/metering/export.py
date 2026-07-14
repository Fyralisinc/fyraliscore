"""export — export SIGNED usage rollups for billing (CSV / JSON).

Once a per-tenant rollup is computed + signed (:mod:`rollup`), billing needs it in a
**flat, ingestible** form. This module takes one-or-many **signed** rollup bundles and:

  * **verifies each one before export** (verify-before-trust / I6) — a bundle whose
    signature does not validate against the trust root is, by default, **refused** (it is
    never exported as a billable line). ``--skip-verify`` exists for offline re-formatting
    of already-trusted data, but the default is fail-closed.
  * emits a billing artifact in one of two formats:

    - **JSON** — a list of the rollup documents plus a signature receipt per row
      (``key_id``, ``signature``, ``sha256``) so the billing system can re-verify
      independently. Round-trips exactly back to the rollup documents.
    - **CSV** — one row per ``(tenant, period, source)`` for the obs-per-source breakdown,
      plus the period totals (think_runs, cost_usd, total observations) carried on every
      row. CSV is for spreadsheet / ERP import; the JSON export is the system-of-record.

The **round-trip** guarantee the self-test checks: ``export_json`` of a set of rollups,
re-read with :func:`read_json_export`, yields the same usage numbers (so nothing is lost
or mutated in export), and every exported row carries the signature receipt so a billing
consumer can re-verify tamper-evidence end to end.

PII (I1): the export carries tenant ids, period bounds, source *names* (e.g. ``github``),
and aggregate counts/cost only — same T1-aggregate posture as the rollup itself.
"""

from __future__ import annotations

import csv
import io
import json
import os
import sys
from dataclasses import dataclass
from typing import Iterable, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402

from rollup import (  # noqa: E402
    ROLLUP_FILENAME,
    UsageRollup,
    load_rollup,
    verify_rollup,
)

__all__ = [
    "ExportRow",
    "SignedRollup",
    "collect_signed_rollups",
    "export_json",
    "export_csv",
    "read_json_export",
    "CSV_COLUMNS",
]

# Stable CSV header (billing-system contract — do not reorder without bumping consumers).
CSV_COLUMNS = [
    "tenant_id",
    "period_label",
    "period_start",
    "period_end",
    "source",            # per-source row; "__TOTAL__" for the period total row
    "observations",      # obs for this source (or total observations on the TOTAL row)
    "think_runs",        # period total (repeated on every row of the period)
    "cost_usd",          # period total (repeated)
    "key_id",            # signature receipt: which key signed this rollup
    "sha256",            # signed-bytes digest (tamper receipt)
]


@dataclass
class SignedRollup:
    """A loaded rollup plus its signature receipt (read off the bundle's manifest)."""

    rollup: UsageRollup
    bundle_dir: str
    key_id: str
    sha256: str
    signature_b64: str
    verified: bool

    @property
    def receipt(self) -> dict:
        return {
            "key_id": self.key_id,
            "sha256": self.sha256,
            "signature": self.signature_b64,
            "verified": self.verified,
        }


def _read_receipt(bundle_dir: str) -> tuple[str, str, str]:
    """Read ``(key_id, sha256, signature_b64)`` from a bundle's manifest + .sig."""
    rollup_path = os.path.join(bundle_dir, ROLLUP_FILENAME)
    manifest_path = rollup_path + ".manifest.json"
    sig_path = rollup_path + ".sig"
    key_id = sha256 = sig_b64 = ""
    if os.path.isfile(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as fh:
            m = json.load(fh)
        key_id = m.get("key_id", "")
        sha256 = m.get("sha256", "")
    if os.path.isfile(sig_path):
        with open(sig_path, "r", encoding="utf-8") as fh:
            sig_b64 = fh.read().strip()
    return key_id, sha256, sig_b64


def collect_signed_rollups(
    bundle_dirs: Iterable[str],
    *,
    trust_root_path: Optional[str] = None,
    require_valid: bool = True,
) -> list[SignedRollup]:
    """Load + verify each bundle dir. With ``require_valid`` (default), a bundle whose
    signature does not verify raises ``ValueError`` (fail-closed: never bill unverifiable
    usage). With ``require_valid=False`` the bundle is still loaded but flagged
    ``verified=False`` in its receipt.
    """
    out: list[SignedRollup] = []
    for d in bundle_dirs:
        res = verify_rollup(d, trust_root_path=trust_root_path)
        if require_valid and not res.ok:
            raise ValueError(
                f"refusing to export unverifiable rollup at {d!r}: {res.reason}"
            )
        rollup = load_rollup(d)
        key_id, sha256, sig_b64 = _read_receipt(d)
        out.append(
            SignedRollup(
                rollup=rollup,
                bundle_dir=d,
                key_id=key_id or (res.key_id or ""),
                sha256=sha256,
                signature_b64=sig_b64,
                verified=bool(res.ok),
            )
        )
    return out


# --------------------------------------------------------------------------- #
# JSON export (system-of-record; round-trips)                                  #
# --------------------------------------------------------------------------- #


def export_json(
    signed: Iterable[SignedRollup],
    *,
    out_path: Optional[str] = None,
    indent: int | None = 2,
) -> str:
    """Serialize signed rollups to the billing **JSON** export string.

    Shape::

        {
          "export_version": 1,
          "generated_at": "...Z",
          "rollups": [
            {"rollup": {<UsageRollup.to_dict()>},
             "receipt": {"key_id": "...", "sha256": "...", "signature": "...", "verified": true}},
            ...
          ]
        }

    If ``out_path`` is given the string is also written there. The ``rollup`` sub-object is
    byte-for-byte the rollup document (so ``read_json_export`` round-trips it).
    """
    doc = {
        "export_version": 1,
        "generated_at": sl.now_rfc3339(),
        "rollups": [
            {"rollup": s.rollup.to_dict(), "receipt": s.receipt} for s in signed
        ],
    }
    text = json.dumps(doc, indent=indent, sort_keys=True) + (
        "\n" if indent is not None else ""
    )
    if out_path:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(text)
    return text


def read_json_export(path_or_text: str) -> list[UsageRollup]:
    """Read a JSON export back into ``UsageRollup`` objects (the round-trip inverse of
    :func:`export_json`). Accepts a path or the raw JSON text."""
    if os.path.isfile(path_or_text):
        with open(path_or_text, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    else:
        doc = json.loads(path_or_text)
    rollups = doc.get("rollups", [])
    return [UsageRollup.from_dict(entry["rollup"]) for entry in rollups]


def read_json_export_receipts(path_or_text: str) -> list[dict]:
    """Read just the signature receipts from a JSON export (for an independent re-verify)."""
    if os.path.isfile(path_or_text):
        with open(path_or_text, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    else:
        doc = json.loads(path_or_text)
    return [entry.get("receipt", {}) for entry in doc.get("rollups", [])]


# --------------------------------------------------------------------------- #
# CSV export (spreadsheet / ERP)                                               #
# --------------------------------------------------------------------------- #


TOTAL_SOURCE = "__TOTAL__"


@dataclass
class ExportRow:
    tenant_id: str
    period_label: str
    period_start: str
    period_end: str
    source: str
    observations: float
    think_runs: float
    cost_usd: float
    key_id: str
    sha256: str

    def as_csv(self) -> list:
        return [
            self.tenant_id,
            self.period_label,
            self.period_start,
            self.period_end,
            self.source,
            self.observations,
            self.think_runs,
            self.cost_usd,
            self.key_id,
            self.sha256,
        ]


def _rows_for(signed: SignedRollup) -> list[ExportRow]:
    r = signed.rollup
    p = r.period.to_dict()
    base = dict(
        tenant_id=r.tenant_id,
        period_label=p["label"],
        period_start=p["start"],
        period_end=p["end"],
        think_runs=r.think_runs,
        cost_usd=r.think_cost_usd,
        key_id=signed.key_id,
        sha256=signed.sha256,
    )
    rows = [
        ExportRow(source=src, observations=obs, **base)
        for src, obs in sorted(r.obs_per_source.items())
    ]
    # A period TOTAL row so a consumer can sanity-check the per-source rows sum up and
    # always has a single line carrying think_runs/cost even for a tenant with 0 sources.
    rows.append(ExportRow(source=TOTAL_SOURCE, observations=r.ingestion_volume, **base))
    return rows


def export_csv(
    signed: Iterable[SignedRollup],
    *,
    out_path: Optional[str] = None,
) -> str:
    """Serialize signed rollups to the billing **CSV** export (one row per
    ``(tenant, period, source)`` + a ``__TOTAL__`` row per period). Returns the CSV text;
    writes it to ``out_path`` too if given."""
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(CSV_COLUMNS)
    for s in signed:
        for row in _rows_for(s):
            w.writerow(row.as_csv())
    text = buf.getvalue()
    if out_path:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
        with open(out_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)
    return text


def read_csv_export(path_or_text: str) -> list[dict]:
    """Parse a CSV export back to a list of dict rows (for tests / billing import)."""
    if os.path.isfile(path_or_text):
        with open(path_or_text, "r", encoding="utf-8", newline="") as fh:
            text = fh.read()
    else:
        text = path_or_text
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(
        description="Export signed usage rollups for billing (verify-before-export)."
    )
    ap.add_argument("bundle_dirs", nargs="+", help="signed rollup bundle directories")
    ap.add_argument("--format", choices=["json", "csv"], default="json")
    ap.add_argument("--out", default=None, help="output file (default: stdout)")
    ap.add_argument("--trust-root", default=None, help="trust root path for verification")
    ap.add_argument(
        "--skip-verify",
        action="store_true",
        help="do NOT fail on an unverifiable bundle (offline re-format only)",
    )
    args = ap.parse_args(argv)

    try:
        signed = collect_signed_rollups(
            args.bundle_dirs,
            trust_root_path=args.trust_root,
            require_valid=not args.skip_verify,
        )
    except ValueError as exc:
        print(f"export refused: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        text = export_json(signed, out_path=args.out)
    else:
        text = export_csv(signed, out_path=args.out)
    if not args.out:
        sys.stdout.write(text)
    else:
        print(f"exported {len(signed)} rollup(s) -> {args.out} ({args.format})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
