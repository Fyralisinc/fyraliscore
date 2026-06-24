"""rollup — compute a SIGNED, tamper-evident per-tenant usage rollup (FR-F2 / FR-F3).

This is the metering job. For a given tenant + period it:

  1. Queries the central Mimir (one tenant at a time, ``X-Scope-OrgID: <tenant>``) for the
     **aggregate Tier-1 usage counters** (:mod:`mimir_client`):
       * ``writer_full_mode_writes_total{source}``  -> obs-per-source (ingestion volume)
       * ``think_runs_total``                        -> reasoning runs
       * ``think_cost_recent_usd_total``             -> LLM/think spend (``cost_usd``)
     as the **delta over the period** (``increase(<counter>[<period>])``).
  2. Builds a per-tenant **usage rollup document**::

        {
          "tenant_id": "acme",
          "period":   {"start": "...Z", "end": "...Z", "label": "2026-06"},
          "metrics":  {
            "obs_per_source":      {"github": 1234.0, "slack": 88.0, ...},
            "think_runs":          42.0,
            "think_cost_usd":      3.1415,
            "ingestion_volume":    1322.0          # = sum(obs_per_source)
          },
          "totals":   {"observations": 1322.0, "think_runs": 42.0, "cost_usd": 3.1415},
          "schema_version": 1,
          "generated_at": "...Z",
          "metric_source": "mimir-tier1"
        }

  3. **Signs** it via ``control-plane/signing`` (ed25519, detached signature + manifest,
     artifact kind ``config``) so the rollup is **tamper-evident** (FR-F2 / SPRINT_PLAN C2
     / I6): the canonical-JSON bytes of the document are what get signed, and any later
     edit to a usage number breaks ``verify_bundle``. The signed rollup on disk is the
     trio ``rollup.json`` + ``rollup.json.sig`` + ``rollup.json.manifest.json``.

PII posture (Invariant I1): this reads **only aggregate counters** — write counts per
source, run counts, and a USD spend gauge. It never reads a payload, a label that could
carry PII, or anything above T1. The rollup carries integers/floats and a tenant id only.

The signing layer is REUSED, not reimplemented — we call ``sign_bundle.sign_file`` and
``verify_bundle.verify_file`` against the control-plane trust root (verify-before-trust,
I6). The active signing key is resolved from ``signing/trust_root.json``.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

# --- import the committed siblings: signing/ (flat modules) -------------------
HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402  (canonical_json_bytes, now_rfc3339)
import sign_bundle as sb  # noqa: E402  (sign_file -> .sig + .manifest.json)
import verify_bundle as vb  # noqa: E402  (verify_file -> VerifyResult)

from mimir_client import (  # noqa: E402
    MimirClient,
    Sample,
    period_to_promql_range,
)

__all__ = [
    "ROLLUP_SCHEMA_VERSION",
    "METRIC_OBS_WRITES",
    "METRIC_THINK_RUNS",
    "METRIC_THINK_COST_USD",
    "Period",
    "UsageRollup",
    "compute_rollup",
    "sign_rollup",
    "verify_rollup",
    "load_rollup",
]

ROLLUP_SCHEMA_VERSION = 1

# The three Tier-1 counters metering reads (matches fleet-sli/recording_rules.yml).
METRIC_OBS_WRITES = "writer_full_mode_writes_total"      # per-source obs writes (ingestion)
METRIC_THINK_RUNS = "think_runs_total"                   # reasoning runs
METRIC_THINK_COST_USD = "think_cost_recent_usd_total"    # cumulative LLM spend (USD)

# The signed rollup artifact filename inside a bundle dir (the signed trio is
# rollup.json + rollup.json.sig + rollup.json.manifest.json).
ROLLUP_FILENAME = "rollup.json"


# --------------------------------------------------------------------------- #
# Period                                                                       #
# --------------------------------------------------------------------------- #


def _ensure_utc(dt: _dt.datetime) -> _dt.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc)


def _to_rfc3339(dt: _dt.datetime) -> str:
    return _ensure_utc(dt).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_rfc3339(value: str) -> _dt.datetime:
    text = value.strip()
    if text.endswith(("z", "Z")):
        text = text[:-1] + "+00:00"
    dt = _dt.datetime.fromisoformat(text)
    return _ensure_utc(dt)


@dataclass(frozen=True)
class Period:
    """A closed billing window ``[start, end]`` with a human ``label`` (e.g. ``2026-06``)."""

    start: _dt.datetime
    end: _dt.datetime
    label: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "start", _ensure_utc(self.start))
        object.__setattr__(self, "end", _ensure_utc(self.end))
        if self.end <= self.start:
            raise ValueError(f"period end ({self.end}) must be after start ({self.start})")

    @classmethod
    def month(cls, year: int, month: int) -> "Period":
        """The calendar month ``[year-month-01, next-month-01)`` in UTC."""
        start = _dt.datetime(year, month, 1, tzinfo=_dt.timezone.utc)
        if month == 12:
            end = _dt.datetime(year + 1, 1, 1, tzinfo=_dt.timezone.utc)
        else:
            end = _dt.datetime(year, month + 1, 1, tzinfo=_dt.timezone.utc)
        return cls(start=start, end=end, label=f"{year:04d}-{month:02d}")

    @classmethod
    def from_dates(
        cls, start: _dt.datetime, end: _dt.datetime, label: Optional[str] = None
    ) -> "Period":
        start = _ensure_utc(start)
        end = _ensure_utc(end)
        return cls(start=start, end=end, label=label or f"{_to_rfc3339(start)}/{_to_rfc3339(end)}")

    def to_dict(self) -> dict:
        return {
            "start": _to_rfc3339(self.start),
            "end": _to_rfc3339(self.end),
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, obj: Mapping[str, Any]) -> "Period":
        return cls(
            start=_parse_rfc3339(obj["start"]),
            end=_parse_rfc3339(obj["end"]),
            label=obj.get("label", ""),
        )

    @property
    def range_promql(self) -> str:
        return period_to_promql_range(self.start, self.end)


# --------------------------------------------------------------------------- #
# UsageRollup document                                                         #
# --------------------------------------------------------------------------- #


@dataclass
class UsageRollup:
    """The per-tenant usage rollup document (the thing that gets signed).

    Build it with :func:`compute_rollup` (queries Mimir) or :meth:`from_dict` (load from
    disk). ``to_dict`` is the exact wire shape; ``canonical_bytes`` are the signed bytes.
    """

    tenant_id: str
    period: Period
    obs_per_source: dict[str, float]
    think_runs: float
    think_cost_usd: float
    generated_at: str = ""
    schema_version: int = ROLLUP_SCHEMA_VERSION
    metric_source: str = "mimir-tier1"

    def __post_init__(self) -> None:
        if not self.generated_at:
            self.generated_at = sl.now_rfc3339()

    @property
    def ingestion_volume(self) -> float:
        """Total observations written across all sources (the ingestion-volume total)."""
        return float(sum(self.obs_per_source.values()))

    def to_dict(self) -> dict:
        """The canonical wire dict. Keys are stable; ``obs_per_source`` is sorted for
        a deterministic, diff-friendly document (signing re-canonicalizes regardless)."""
        obs = {k: _round(v) for k, v in sorted(self.obs_per_source.items())}
        return {
            "tenant_id": self.tenant_id,
            "period": self.period.to_dict(),
            "metrics": {
                "obs_per_source": obs,
                "ingestion_volume": _round(self.ingestion_volume),
                "think_runs": _round(self.think_runs),
                "think_cost_usd": _round(self.think_cost_usd, 6),
            },
            "totals": {
                "observations": _round(self.ingestion_volume),
                "think_runs": _round(self.think_runs),
                "cost_usd": _round(self.think_cost_usd, 6),
            },
            "schema_version": self.schema_version,
            "metric_source": self.metric_source,
            "generated_at": self.generated_at,
        }

    @classmethod
    def from_dict(cls, obj: Mapping[str, Any]) -> "UsageRollup":
        if not isinstance(obj, Mapping):
            raise ValueError("rollup must be a JSON object")
        metrics = obj.get("metrics") or {}
        obs = metrics.get("obs_per_source") or {}
        if not isinstance(obs, Mapping):
            raise ValueError("metrics.obs_per_source must be an object")
        return cls(
            tenant_id=_require_str(obj, "tenant_id"),
            period=Period.from_dict(obj["period"]),
            obs_per_source={str(k): float(v) for k, v in obs.items()},
            think_runs=float(metrics.get("think_runs", 0.0)),
            think_cost_usd=float(metrics.get("think_cost_usd", 0.0)),
            generated_at=str(obj.get("generated_at", "")) or sl.now_rfc3339(),
            schema_version=int(obj.get("schema_version", ROLLUP_SCHEMA_VERSION)),
            metric_source=str(obj.get("metric_source", "mimir-tier1")),
        )

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True) + (
            "\n" if indent is not None else ""
        )

    def canonical_bytes(self) -> bytes:
        """The exact bytes ``signing`` signs (compact-canonical JSON of ``to_dict``)."""
        return sl.canonical_json_bytes(self.to_dict())

    def fingerprint(self) -> str:
        return sl.sha256_hex(self.canonical_bytes())


def _round(v: float, ndigits: int = 3) -> float:
    """Round to keep the JSON tidy; counts come back whole, cost keeps cents+."""
    r = round(float(v), ndigits)
    # Normalise -0.0 -> 0.0 and whole floats stay floats (JSON has no int/float split here).
    return 0.0 if r == 0 else r


def _require_str(obj: Mapping[str, Any], key: str) -> str:
    val = obj.get(key)
    if not isinstance(val, str) or not val.strip():
        raise ValueError(f"rollup field {key!r} must be a non-empty string")
    return val


# --------------------------------------------------------------------------- #
# Compute (query Mimir)                                                         #
# --------------------------------------------------------------------------- #


def compute_rollup(
    client: MimirClient,
    *,
    tenant_id: str,
    period: Period,
) -> UsageRollup:
    """Query Mimir for ``tenant_id`` over ``period`` and assemble the usage rollup.

    Every query carries ``X-Scope-OrgID: <tenant_id>`` so only that tenant's series are
    visible. Counters are read as ``increase(<counter>[<period>])`` evaluated at the
    period end. A missing series (tenant never ran ``think``, a source had no writes)
    yields an empty vector and is recorded as ``0`` — never an error (you can validly bill
    a tenant for zero think runs).
    """
    rng = period.range_promql
    at = period.end

    # obs-per-source: keep the per-source breakdown by grouping on the `source` label.
    obs_samples = client.increase_over_period(
        METRIC_OBS_WRITES, tenant_id=tenant_id, range_promql=rng, at=at, by=("source",)
    )
    obs_per_source: dict[str, float] = {}
    for s in obs_samples:
        src = s.label("source") or "_unlabeled"
        # Sum in case multiple series collapse to the same source label.
        obs_per_source[src] = obs_per_source.get(src, 0.0) + max(0.0, s.value)

    think_runs = _scalar(
        client.increase_over_period(
            METRIC_THINK_RUNS, tenant_id=tenant_id, range_promql=rng, at=at
        )
    )
    think_cost_usd = _scalar(
        client.increase_over_period(
            METRIC_THINK_COST_USD, tenant_id=tenant_id, range_promql=rng, at=at
        )
    )

    return UsageRollup(
        tenant_id=tenant_id,
        period=period,
        obs_per_source=obs_per_source,
        think_runs=think_runs,
        think_cost_usd=think_cost_usd,
    )


def _scalar(samples: list[Sample]) -> float:
    """Collapse a (sum-wrapped) vector to a single non-negative scalar; [] -> 0.0.

    ``increase()`` can return a tiny negative due to extrapolation at the series edge;
    usage counts must not be negative, so clamp at 0.
    """
    if not samples:
        return 0.0
    total = sum(s.value for s in samples)
    return max(0.0, total)


# --------------------------------------------------------------------------- #
# Sign / verify the rollup (REUSE control-plane/signing — C2 / I6 / FR-F2)     #
# --------------------------------------------------------------------------- #


def sign_rollup(
    rollup: UsageRollup,
    *,
    out_dir: str,
    key_id: Optional[str] = None,
) -> dict:
    """Write ``rollup.json`` into ``out_dir`` and ed25519-sign it (detached sig + manifest).

    Reuses ``signing/sign_bundle.sign_file`` with artifact kind ``config`` so the canonical
    JSON of the rollup is the signed quantity (FR-F2: tamper-evident usage). Returns paths:
    ``{rollup_path, sig_path, manifest_path, fingerprint}``.

    The signer resolves the active signing key from the control-plane trust root
    (``signing/trust_root.json``) unless ``key_id`` pins a specific key.
    """
    os.makedirs(out_dir, exist_ok=True)
    rollup_path = os.path.join(out_dir, ROLLUP_FILENAME)
    with open(rollup_path, "w", encoding="utf-8") as fh:
        fh.write(rollup.to_json(indent=2))

    # version = the period label so the manifest carries which billing window this is.
    sig_path, manifest_path = sb.sign_file(
        rollup_path,
        key_id=key_id,
        kind="config",  # canonical-JSON signing path (order/whitespace independent)
        version=rollup.period.label or rollup.generated_at,
    )
    return {
        "rollup_path": rollup_path,
        "sig_path": sig_path,
        "manifest_path": manifest_path,
        "fingerprint": rollup.fingerprint(),
    }


def verify_rollup(
    rollup_dir_or_path: str,
    *,
    trust_root_path: Optional[str] = None,
    allow_retired: bool = False,
) -> vb.VerifyResult:
    """Verify a signed rollup bundle BEFORE trusting it for billing (verify-before-apply).

    Accepts either the bundle directory (containing ``rollup.json``) or the path to
    ``rollup.json`` itself. Delegates to ``signing/verify_bundle.verify_file`` — a tampered
    usage number, an unknown key, or a corrupt signature all return ``ok=False``.
    """
    if os.path.isdir(rollup_dir_or_path):
        path = os.path.join(rollup_dir_or_path, ROLLUP_FILENAME)
    else:
        path = rollup_dir_or_path
    return vb.verify_file(
        path, trust_root_path=trust_root_path, allow_retired=allow_retired
    )


def load_rollup(rollup_dir_or_path: str) -> UsageRollup:
    """Load (parse) a rollup document from disk. Does NOT verify — call :func:`verify_rollup`
    first if the bytes are untrusted."""
    if os.path.isdir(rollup_dir_or_path):
        path = os.path.join(rollup_dir_or_path, ROLLUP_FILENAME)
    else:
        path = rollup_dir_or_path
    with open(path, "r", encoding="utf-8") as fh:
        return UsageRollup.from_dict(json.load(fh))


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(
        description="Compute + sign a per-tenant Tier-1 usage rollup from Mimir (FR-F2/F3)."
    )
    ap.add_argument("tenant_id", help="tenant id (becomes X-Scope-OrgID for every query)")
    ap.add_argument("--out-dir", required=True, help="directory for the signed rollup bundle")
    ap.add_argument("--mimir-url", default=None, help="Mimir base URL (default http://mimir:9009)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--month", help="billing month YYYY-MM (UTC calendar month)")
    g.add_argument(
        "--range",
        nargs=2,
        metavar=("START", "END"),
        help="explicit RFC-3339 [START END] window",
    )
    ap.add_argument("--key-id", default=None, help="signing key id (default: trust-root active)")
    ap.add_argument("--verify", action="store_true", help="verify the signature after signing")
    args = ap.parse_args(argv)

    if args.month:
        y, m = args.month.split("-")
        period = Period.month(int(y), int(m))
    else:
        period = Period.from_dates(_parse_rfc3339(args.range[0]), _parse_rfc3339(args.range[1]))

    from mimir_client import DEFAULT_MIMIR_URL

    base = args.mimir_url or DEFAULT_MIMIR_URL
    with MimirClient(base) as client:
        rollup = compute_rollup(client, tenant_id=args.tenant_id, period=period)

    paths = sign_rollup(rollup, out_dir=args.out_dir, key_id=args.key_id)
    print(f"signed usage rollup for tenant {args.tenant_id!r} period {period.label}")
    print(f"  observations : {rollup.ingestion_volume}")
    print(f"  think_runs   : {rollup.think_runs}")
    print(f"  cost_usd     : {rollup.think_cost_usd}")
    print(f"  fingerprint  : {paths['fingerprint']}")
    print(f"  -> {paths['rollup_path']}")
    print(f"  -> {paths['sig_path']}")
    print(f"  -> {paths['manifest_path']}")

    if args.verify:
        res = verify_rollup(args.out_dir)
        print(f"  verify: {'OK' if res.ok else 'FAILED'} — {res.reason}")
        return 0 if res.ok else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
