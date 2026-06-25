#!/usr/bin/env python3
"""store.py — the fleet deployment registry (C4) the console serves over HTTP.

A small, dependency-light registry that holds **exactly one row per
``deployment_id``** (the C4 ``DeploymentRecord``) and is the single place the
console's read/write paths agree on health.

Design (NFR-5 + C4)
-------------------
* **Keyed by ``deployment_id``** — ``upsert`` replaces the row for an existing id
  (a heartbeat is an upsert, never an append) and inserts for a new one.
* **Health is *derived on read*, never trusted from the wire.** A deployment that
  heartbeats green and then goes silent must drift yellow→red without anyone
  touching its row, so :meth:`record` / :meth:`list_records` always re-derive
  ``health`` from heartbeat age **at read time** via the shared
  :func:`lib.deployment.derive_health` (the same function the agent stamps with),
  using the NFR-5 thresholds: stale (> ``yellow_after_s``, default **90 s**) ⇒
  ``yellow``; missing (> ``red_after_s``, default 300 s) ⇒ ``red``; an expired
  license forces ``red``; a reported fleet-SLI burn flag degrades green→yellow.
* **In-memory + optional JSON-file persistence** under ``console/data/`` so a
  console restart does not lose the fleet. Persistence is best-effort and writes
  the exact C4 wire shape (``to_registry_dict``); a missing/corrupt file starts
  empty rather than crashing the console.
* **Thread-safe.** A single ``RLock`` guards all mutation/read so the FastAPI
  app (which may serve concurrent requests) sees a consistent registry.

This module deliberately reuses ``lib.deployment`` for the record model and
health math — it does NOT redefine the C4 record (that is the cross-component
contract).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import secrets
import sys
import tempfile
import threading
from pathlib import Path
from typing import Iterable

# Make the control-plane root importable so ``import lib...`` resolves whether the
# console runs as ``python app.py``, under uvicorn, inside the container (where
# the root is /app), or from a test. The root is the dir holding SPRINT_PLAN.md.
# We FRONT-load it (and evict any foreign top-level ``lib`` already imported) so
# the control-plane's shared library always wins over an unrelated ``lib`` that
# may be on the path (e.g. the host repo whose venv runs the tests ships its own).
_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, *_HERE.parents):
    if (_cand / "SPRINT_PLAN.md").is_file() or (_cand / "lib" / "deployment.py").is_file():
        _root = str(_cand)
        while _root in sys.path:
            sys.path.remove(_root)
        sys.path.insert(0, _root)
        _existing = sys.modules.get("lib")
        if _existing is not None and not (
            getattr(_existing, "__file__", "") or ""
        ).startswith(_root):
            for _name in [n for n in list(sys.modules) if n == "lib" or n.startswith("lib.")]:
                del sys.modules[_name]
        break

from lib.deployment import (  # noqa: E402
    DEFAULT_RED_AFTER_S,
    DEFAULT_YELLOW_AFTER_S,
    DeploymentRecord,
    Health,
)
from lib.desired_state import DesiredState  # noqa: E402
from lib.primitives import to_rfc3339, utcnow  # noqa: E402

__all__ = [
    "DeploymentStore",
    "default_data_dir",
    "mint_deployment_id",
]


def default_data_dir() -> Path:
    """The on-disk persistence directory: ``console/data/`` (overridable).

    Honors ``CP_CONSOLE_DATA_DIR`` so the container can point at a mounted
    volume; otherwise defaults to ``console/data/`` next to this module.
    """
    env = os.environ.get("CP_CONSOLE_DATA_DIR")
    if env:
        return Path(env).expanduser()
    return _HERE / "data"


def _slug(value: str) -> str:
    """Lowercase, hyphen-safe slug for building a deployment_id component."""
    cleaned = "".join(c if c.isalnum() else "-" for c in value.strip().lower())
    cleaned = "-".join(p for p in cleaned.split("-") if p)
    return cleaned or "x"


# Map a long/cloud region to a short, stable token for the deployment_id
# (cosmetic only — the full region is stored verbatim on the record).
_REGION_SHORT = {
    "us-east-1": "use1",
    "us-east-2": "use2",
    "us-west-1": "usw1",
    "us-west-2": "usw2",
    "eu-west-1": "euw1",
    "eu-central-1": "euc1",
    "ap-south-1": "aps1",
    "ap-southeast-1": "apse1",
    "ap-northeast-1": "apne1",
}


def _region_token(region: str) -> str:
    r = region.strip().lower()
    if r in _REGION_SHORT:
        return _REGION_SHORT[r]
    # Compact an arbitrary region: drop hyphens, keep it short.
    return _slug(region).replace("-", "")[:8] or "rgn"


def mint_deployment_id(tenant_id: str, region: str) -> str:
    """Mint a stable, human-readable, collision-resistant ``deployment_id``.

    Shape mirrors the C4 example ``acme-use1-7f3a``: ``<tenant>-<region>-<rand>``
    where ``<rand>`` is 4 hex chars of CSPRNG entropy. Uniqueness across the
    registry is guaranteed by the caller (which retries on the astronomically
    unlikely collision).
    """
    return f"{_slug(tenant_id)}-{_region_token(region)}-{secrets.token_hex(2)}"


def mint_tenant_id() -> str:
    """Mint a tenant_id when a registrant did not bring one (P4 register path)."""
    return f"t-{secrets.token_hex(4)}"


class DeploymentStore:
    """In-memory fleet registry with best-effort JSON-file persistence (C4).

    One row per ``deployment_id``. Health is **always derived on read** so the
    console reflects a deployment that went silent after its last heartbeat.
    """

    def __init__(
        self,
        data_dir: "str | Path | None" = None,
        *,
        persist: bool = True,
        yellow_after_s: int = DEFAULT_YELLOW_AFTER_S,
        red_after_s: int = DEFAULT_RED_AFTER_S,
    ) -> None:
        self._lock = threading.RLock()
        self._rows: dict[str, DeploymentRecord] = {}
        self._persist = persist
        self._yellow_after_s = yellow_after_s
        self._red_after_s = red_after_s

        self._data_dir = Path(data_dir) if data_dir is not None else default_data_dir()
        self._path = self._data_dir / "fleet_registry.json"
        # Desired-state + applied-facet sidecars (console-roadmap §4). Kept
        # ALONGSIDE the registry as their own JSON files so the existing
        # registry persistence/round-trip (and its tests) are untouched — the
        # desired facet is operator-written, the applied facet is agent-reported,
        # and neither belongs in the C4 record the heartbeat upserts.
        self._desired_path = self._data_dir / "desired_state.json"
        self._applied_path = self._data_dir / "applied_state.json"
        self._desired: dict[str, DesiredState] = {}
        self._applied: dict[str, dict] = {}
        if self._persist:
            self._load_from_disk()
            self._load_desired_from_disk()
            self._load_applied_from_disk()

    # --- properties --------------------------------------------------------

    @property
    def path(self) -> Path:
        return self._path

    @property
    def yellow_after_s(self) -> int:
        return self._yellow_after_s

    @property
    def red_after_s(self) -> int:
        return self._red_after_s

    @property
    def desired_path(self) -> Path:
        return self._desired_path

    @property
    def applied_path(self) -> Path:
        return self._applied_path

    # --- persistence -------------------------------------------------------

    def _load_from_disk(self) -> None:
        """Load the registry from disk; a missing/corrupt file starts empty."""
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return
        except OSError:
            return
        try:
            data = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            # Corrupt file: do not crash the console — start from empty and let
            # the next write overwrite it.
            return
        if not isinstance(data, dict):
            return
        rows: dict[str, DeploymentRecord] = {}
        for dep_id, row in data.items():
            try:
                rec = DeploymentRecord(**row)
            except Exception:
                # Skip an unparseable row rather than fail the whole load.
                continue
            rows[rec.deployment_id] = rec
        with self._lock:
            self._rows = rows

    def _flush_to_disk(self) -> None:
        """Atomically persist the registry to ``console/data/`` (best-effort)."""
        if not self._persist:
            return
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            payload = {
                dep_id: rec.to_registry_dict() for dep_id, rec in self._rows.items()
            }
            # Atomic replace so a crash mid-write never leaves a truncated file.
            fd, tmp = tempfile.mkstemp(
                dir=str(self._data_dir), prefix=".fleet_registry.", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, indent=2, sort_keys=True)
                os.replace(tmp, self._path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except OSError:
            # Persistence is best-effort; an unwritable data dir must not break
            # the live (in-memory) console.
            return

    # --- desired / applied sidecar persistence (console-roadmap §4) --------

    @staticmethod
    def _load_json_map(path: Path) -> dict:
        """Best-effort load a ``{deployment_id: {...}}`` JSON map; {} on any error."""
        try:
            raw = path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return {}
        try:
            data = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def _atomic_write_json(self, path: Path, payload: dict) -> None:
        """Atomically write ``payload`` to ``path`` (best-effort, never raises)."""
        if not self._persist:
            return
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(
                dir=str(self._data_dir), prefix="." + path.name + ".", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, indent=2, sort_keys=True)
                os.replace(tmp, path)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except OSError:
            return

    def _load_desired_from_disk(self) -> None:
        rows: dict[str, DesiredState] = {}
        for dep_id, row in self._load_json_map(self._desired_path).items():
            try:
                rows[dep_id] = DesiredState.from_dict(row)
            except Exception:
                continue
        with self._lock:
            self._desired = rows

    def _flush_desired_to_disk(self) -> None:
        payload = {dep_id: d.to_dict() for dep_id, d in self._desired.items()}
        self._atomic_write_json(self._desired_path, payload)

    def _load_applied_from_disk(self) -> None:
        rows = {
            dep_id: row
            for dep_id, row in self._load_json_map(self._applied_path).items()
            if isinstance(row, dict)
        }
        with self._lock:
            self._applied = rows

    def _flush_applied_to_disk(self) -> None:
        self._atomic_write_json(self._applied_path, dict(self._applied))

    # --- desired / applied facet API (console-roadmap §4) ------------------

    def put_desired(self, deployment_id: str, desired: DesiredState) -> DesiredState:
        """Persist the operator-written DESIRED state for ``deployment_id``.

        The stored record's ``deployment_id`` is normalized to the path id so a
        desired blob can never be filed under the wrong key. Returns the stored
        :class:`DesiredState`. Auditing + signing of the write are the caller's
        responsibility (the router does both) — the store is the durable home.
        """
        with self._lock:
            stored = desired.model_copy(update={"deployment_id": deployment_id})
            self._desired[deployment_id] = stored
            self._flush_desired_to_disk()
            return stored

    def get_desired(self, deployment_id: str) -> DesiredState | None:
        """Return the DESIRED state for ``deployment_id`` (or None if none set)."""
        with self._lock:
            return self._desired.get(deployment_id)

    def record_applied(self, deployment_id: str, applied: dict) -> None:
        """Store the agent-reported APPLIED facets for ``deployment_id``.

        ``applied`` is merged onto any existing applied record so an old agent
        that only reports some facets does not clobber the rest. Recognized
        facets: ``applied_config_version``, ``applied_release``,
        ``acked_action_ids``, ``license_state_applied``. Unknown keys are kept
        verbatim (forward-compat).
        """
        if not applied:
            return
        with self._lock:
            cur = dict(self._applied.get(deployment_id, {}))
            cur.update(applied)
            self._applied[deployment_id] = cur
            self._flush_applied_to_disk()

    def get_applied(self, deployment_id: str) -> dict:
        """Return the agent-reported APPLIED facets for ``deployment_id`` ({} if none)."""
        with self._lock:
            return dict(self._applied.get(deployment_id, {}))

    # --- mutation ----------------------------------------------------------

    def upsert(self, record: DeploymentRecord) -> DeploymentRecord:
        """Insert or replace the row for ``record.deployment_id`` (heartbeat).

        Returns the stored record **with health re-derived as-of now** so the
        caller sees the canonical health, not whatever the wire claimed.
        """
        with self._lock:
            self._rows[record.deployment_id] = record
            self._flush_to_disk()
            return self._derive(record)

    def register(
        self,
        *,
        tenant_id: str | None,
        region: str,
        plan: str | None = None,
        version: str = "0.0.0",
        license_expiry: "str | _dt.datetime | None" = None,
        telemetry_tier: str = "T1",
        now: _dt.datetime | None = None,
    ) -> DeploymentRecord:
        """Mint a new deployment row (P4 ``POST /api/v1/register``).

        Mints a ``deployment_id`` (and a ``tenant_id`` if the registrant did not
        bring one), stamps an initial heartbeat at ``now`` so the freshly
        registered deployment starts ``green``, and persists the row.

        ``plan`` is accepted per the P4 register contract; it is not a C4 record
        field (the license bundle carries the plan), so it is used only to pick a
        default ``license_expiry`` when the caller does not supply one.
        """
        now = now or utcnow()
        tid = (tenant_id or "").strip() or mint_tenant_id()

        with self._lock:
            # Mint a non-colliding deployment_id.
            dep_id = mint_deployment_id(tid, region)
            for _ in range(8):
                if dep_id not in self._rows:
                    break
                dep_id = mint_deployment_id(tid, region)

            if license_expiry is None:
                # Default: a 1-year license window from now (the signed license
                # bundle is authoritative; this is a placeholder until the first
                # heartbeat carries the real expiry off the verified license).
                license_expiry = now + _dt.timedelta(days=365)

            rec = DeploymentRecord.heartbeat(
                tenant_id=tid,
                deployment_id=dep_id,
                version=version,
                region=region,
                license_expiry=license_expiry,
                telemetry_tier=telemetry_tier,
                last_heartbeat_ts=now,
                now=now,
                yellow_after_s=self._yellow_after_s,
                red_after_s=self._red_after_s,
            )
            self._rows[dep_id] = rec
            self._flush_to_disk()
            return rec

    def delete(self, deployment_id: str) -> bool:
        """Remove a deployment row (and any desired/applied sidecar). Returns
        True if the registry row existed."""
        with self._lock:
            existed = self._rows.pop(deployment_id, None) is not None
            had_desired = self._desired.pop(deployment_id, None) is not None
            had_applied = self._applied.pop(deployment_id, None) is not None
            if existed:
                self._flush_to_disk()
            if had_desired:
                self._flush_desired_to_disk()
            if had_applied:
                self._flush_applied_to_disk()
            return existed

    def clear(self) -> None:
        """Drop all rows + desired/applied sidecars (used by tests)."""
        with self._lock:
            self._rows.clear()
            self._desired.clear()
            self._applied.clear()
            self._flush_to_disk()
            self._flush_desired_to_disk()
            self._flush_applied_to_disk()

    # --- read (health derived on read) -------------------------------------

    def _derive(
        self, record: DeploymentRecord, *, now: _dt.datetime | None = None
    ) -> DeploymentRecord:
        return record.with_derived_health(
            now=now,
            yellow_after_s=self._yellow_after_s,
            red_after_s=self._red_after_s,
        )

    def record(
        self, deployment_id: str, *, now: _dt.datetime | None = None
    ) -> DeploymentRecord | None:
        """Return one deployment with **health re-derived as of ``now``**."""
        with self._lock:
            rec = self._rows.get(deployment_id)
            if rec is None:
                return None
            return self._derive(rec, now=now)

    def list_records(
        self, *, now: _dt.datetime | None = None
    ) -> list[DeploymentRecord]:
        """Return every deployment, each with **health re-derived as of ``now``**.

        Sorted worst-health-first, then by ``deployment_id`` so the operator
        rollup surfaces problems at the top deterministically.
        """
        now = now or utcnow()
        with self._lock:
            derived = [self._derive(r, now=now) for r in self._rows.values()]
        derived.sort(key=lambda r: (-r.health.rank, r.deployment_id))
        return derived

    def __len__(self) -> int:
        with self._lock:
            return len(self._rows)

    def __contains__(self, deployment_id: object) -> bool:
        with self._lock:
            return deployment_id in self._rows

    # --- view helpers (for the HTML rollup) --------------------------------

    @staticmethod
    def heartbeat_age_seconds(
        record: DeploymentRecord, *, now: _dt.datetime | None = None
    ) -> float:
        """Whole-fleet-friendly heartbeat age in seconds (>= 0)."""
        now = now or utcnow()
        hb = record.last_heartbeat_ts
        if hb.tzinfo is None:
            hb = hb.replace(tzinfo=_dt.timezone.utc)
        age = (now - hb).total_seconds()
        return max(0.0, age)

    def summary(
        self, *, now: _dt.datetime | None = None
    ) -> dict[str, int]:
        """Counts of {green, yellow, red, total} as of ``now`` (rollup header)."""
        now = now or utcnow()
        counts = {"green": 0, "yellow": 0, "red": 0}
        for rec in self.list_records(now=now):
            counts[rec.health.value] += 1
        counts["total"] = sum(counts.values())
        return counts
