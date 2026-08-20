#!/usr/bin/env python3
"""rollout — a canary -> fleet rollout CONTROLLER with halt-on-drift + rollback (FR-D / I6).

Promoting a new signed release to the whole fleet at once is how you take an entire
customer base down on a bad build. This controller does it the safe way:

    1. READ the fleet from the console      (GET /api/v1/deployments — C4 records,
                                              health derived on read).
    2. SELECT a CANARY subset               (a count or a fraction of eligible
                                              deployments, deterministically).
    3. RECORD each target's PRIOR version    (so we can roll back).
    4. PROMOTE the canaries to the target    (point them at the new release).
    5. WATCH the canary health rollup        (poll the console; the console re-derives
                                              health from heartbeat freshness + SLI burn).
       - all canaries reach/stay GREEN on the target version  -> proceed.
       - ANY canary goes non-green (drift)                    -> **HALT**: do NOT promote
                                                                 the fleet; optionally roll
                                                                 the canaries back.
    6. PROMOTE THE FLEET (the remaining deployments) only after a clean canary watch.
    7. ROLLBACK is available at any time: re-point promoted deployments at their prior
       version.

The controller is **transport-injected**: it talks to the console through a
``ConsoleClient`` (default = outbound ``requests`` to ``GET /api/v1/deployments``;
the self-test injects an in-memory fake console), and it promotes through a
``Promoter`` callback (default = move the release-registry ``latest`` pointer + log;
the self-test's promoter makes the fake agents adopt the version + report health).
This keeps the *decision logic* — the part that must be correct — independent of how
bytes move, and lets the self-test exercise both the healthy-canary (promote) and
unhealthy-canary (halt) paths deterministically.

A promotion never bypasses signing: the only thing a deployment can adopt is a
release whose signed bundle the agent verified before apply (I6). The controller
operates on *versions*; the agent's ``config_pull``/verify is the gate on the bytes.

Usage (CLI, against a live console)
-----------------------------------
    python rollout.py promote --console http://localhost:8080 --version 1.4.3 \
        --canary-count 1 --watch-seconds 30 --poll-seconds 3

    python rollout.py status   --console http://localhost:8080
    python rollout.py rollback --console http://localhost:8080 --to 1.4.2
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)

__all__ = [
    "ConsoleClient",
    "HttpConsoleClient",
    "RolloutController",
    "RolloutPlan",
    "RolloutResult",
    "Promoter",
    "select_canary",
]

# A Promoter applies (deployment_id, target_version) to the fleet's source of truth.
# It returns nothing; failures should raise. The controller calls it for the canary
# set, then (on a clean watch) for the fleet set, and again on rollback.
Promoter = Callable[[str, str], None]


# --------------------------------------------------------------------------- #
# Console transport (read the fleet; the only thing the controller needs)      #
# --------------------------------------------------------------------------- #


class ConsoleClient(Protocol):
    """The slice of the console the controller depends on: list deployments."""

    def list_deployments(self) -> list[dict]:
        """Return the console's ``GET /api/v1/deployments`` list (C4 dicts w/ derived health)."""
        ...


class HttpConsoleClient:
    """Default OUTBOUND console client: ``GET <base>/api/v1/deployments``.

    Imports ``requests`` lazily so importing this module never needs a network
    stack (and the self-test, which injects a fake client, doesn't need it).
    """

    def __init__(self, base_url: str, *, timeout_s: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s

    def list_deployments(self) -> list[dict]:
        import requests  # lazy

        resp = requests.get(self.base_url + "/api/v1/deployments", timeout=self.timeout_s)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError("console /api/v1/deployments did not return a list")
        return data


# --------------------------------------------------------------------------- #
# Canary selection                                                             #
# --------------------------------------------------------------------------- #


def select_canary(
    deployments: list[dict],
    *,
    target_version: str,
    canary_count: int | None = None,
    canary_fraction: float | None = None,
    region: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Split eligible deployments into ``(canary, fleet_remainder)``.

    Eligible = not already on ``target_version`` (those are skipped — nothing to do)
    and, if ``region`` is given, in that region. Selection is **deterministic**:
    eligible deployments are sorted by ``deployment_id`` and the first N are the
    canary, so a re-run picks the same canary. ``canary_count`` wins over
    ``canary_fraction``; the canary is always at least 1 (if any eligible) and never
    the entire eligible set (you need a fleet remainder to gate) **unless** there is
    only one eligible deployment.
    """
    eligible = [
        d
        for d in deployments
        if d.get("version") != target_version
        and (region is None or d.get("region") == region)
    ]
    eligible.sort(key=lambda d: d.get("deployment_id", ""))
    n = len(eligible)
    if n == 0:
        return [], []

    if canary_count is not None:
        k = canary_count
    elif canary_fraction is not None:
        import math

        k = max(1, math.floor(n * canary_fraction))
    else:
        k = 1

    k = max(1, min(k, n))
    # Keep at least one in the fleet remainder so the canary actually gates — unless
    # there is only a single eligible deployment (then it IS the canary).
    if n > 1 and k >= n:
        k = n - 1

    return eligible[:k], eligible[k:]


# --------------------------------------------------------------------------- #
# Plan + result records                                                        #
# --------------------------------------------------------------------------- #


@dataclass
class RolloutPlan:
    target_version: str
    canary_ids: list[str]
    fleet_ids: list[str]
    prior_versions: dict[str, str]  # deployment_id -> version BEFORE this rollout

    def as_dict(self) -> dict:
        return {
            "target_version": self.target_version,
            "canary_ids": self.canary_ids,
            "fleet_ids": self.fleet_ids,
            "prior_versions": self.prior_versions,
        }


@dataclass
class RolloutResult:
    target_version: str
    plan: RolloutPlan
    canary_promoted: bool = False
    canary_healthy: bool = False
    fleet_promoted: bool = False
    halted: bool = False
    rolled_back: bool = False
    reason: str = ""
    canary_health: dict[str, str] = field(default_factory=dict)  # id -> last health
    events: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """A clean rollout = fleet promoted and not halted."""
        return self.fleet_promoted and not self.halted

    def as_dict(self) -> dict:
        return {
            "target_version": self.target_version,
            "ok": self.ok,
            "canary_promoted": self.canary_promoted,
            "canary_healthy": self.canary_healthy,
            "fleet_promoted": self.fleet_promoted,
            "halted": self.halted,
            "rolled_back": self.rolled_back,
            "reason": self.reason,
            "canary_health": self.canary_health,
            "plan": self.plan.as_dict(),
            "events": self.events,
        }


# --------------------------------------------------------------------------- #
# The controller                                                               #
# --------------------------------------------------------------------------- #


class RolloutController:
    """Drives a canary -> fleet rollout, halting on canary drift."""

    def __init__(
        self,
        console: ConsoleClient,
        promoter: Promoter,
        *,
        # Health policy: which derived-health values are acceptable for a canary to
        # be considered "good". Default: only "green" is good (strict). Pass
        # {"green", "yellow"} to tolerate stale-but-not-dead canaries.
        healthy_states: Iterable[str] = ("green",),
        sleep: Callable[[float], None] = time.sleep,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.console = console
        self.promoter = promoter
        self.healthy_states = set(healthy_states)
        self._sleep = sleep
        self._log = log or (lambda m: print(f"[rollout] {m}"))

    # -- planning ----------------------------------------------------------- #

    def plan(
        self,
        target_version: str,
        *,
        canary_count: int | None = None,
        canary_fraction: float | None = None,
        region: str | None = None,
    ) -> RolloutPlan:
        deployments = self.console.list_deployments()
        canary, fleet = select_canary(
            deployments,
            target_version=target_version,
            canary_count=canary_count,
            canary_fraction=canary_fraction,
            region=region,
        )
        prior = {
            d["deployment_id"]: d.get("version", "")
            for d in deployments
            if d.get("deployment_id")
        }
        return RolloutPlan(
            target_version=target_version,
            canary_ids=[d["deployment_id"] for d in canary],
            fleet_ids=[d["deployment_id"] for d in fleet],
            prior_versions=prior,
        )

    # -- health watch ------------------------------------------------------- #

    def _canary_health(self, canary_ids: set[str], target_version: str) -> dict[str, dict]:
        """Current console view of just the canary deployments (by id)."""
        return {
            d["deployment_id"]: d
            for d in self.console.list_deployments()
            if d.get("deployment_id") in canary_ids
        }

    def watch_canary(
        self,
        canary_ids: list[str],
        target_version: str,
        *,
        watch_seconds: float = 30.0,
        poll_seconds: float = 3.0,
        require_target_version: bool = True,
    ) -> tuple[bool, dict[str, str], str]:
        """Poll the console until every canary is healthy on the target, or a canary
        drifts / the window expires.

        Returns ``(healthy, last_health_by_id, reason)``.

        * **Immediate HALT** the moment any canary reports a non-healthy derived
          health (drift) — we do not wait out the window on a known-bad canary.
        * Success requires every canary to be in ``healthy_states`` AND (if
          ``require_target_version``) to be reporting the ``target_version`` (i.e. it
          actually adopted the release, not just "still green on the old one").
        """
        ids = set(canary_ids)
        deadline = _monotonic() + watch_seconds
        last_health: dict[str, str] = {}
        first = True
        while True:
            view = self._canary_health(ids, target_version)
            last_health = {cid: view.get(cid, {}).get("health", "missing") for cid in ids}

            # Drift: any canary visibly non-healthy -> halt now.
            drifted = [cid for cid, h in last_health.items() if h not in self.healthy_states]
            if drifted:
                return (
                    False,
                    last_health,
                    f"canary drift: {', '.join(sorted(drifted))} non-healthy "
                    f"(health={ {c: last_health[c] for c in drifted} })",
                )

            # A canary that vanished from the registry is a failure, not "healthy".
            missing = [cid for cid in ids if cid not in view]
            if missing:
                return False, last_health, f"canary(s) missing from registry: {sorted(missing)}"

            on_target = (
                all(view[cid].get("version") == target_version for cid in ids)
                if require_target_version
                else True
            )
            if on_target:
                return True, last_health, "all canaries healthy on target version"

            if _monotonic() >= deadline:
                # Window expired without the canaries adopting the target version.
                stuck = {cid: view[cid].get("version") for cid in ids
                         if view[cid].get("version") != target_version}
                return (
                    False,
                    last_health,
                    f"watch window expired; canaries not on target {target_version}: {stuck}",
                )

            if first:
                self._log(
                    f"watching {len(ids)} canary deployment(s) for up to {watch_seconds:.0f}s "
                    f"(poll {poll_seconds:.0f}s); healthy_states={sorted(self.healthy_states)}"
                )
                first = False
            self._sleep(poll_seconds)

    # -- the rollout -------------------------------------------------------- #

    def rollout(
        self,
        target_version: str,
        *,
        canary_count: int | None = None,
        canary_fraction: float | None = None,
        region: str | None = None,
        watch_seconds: float = 30.0,
        poll_seconds: float = 3.0,
        rollback_on_halt: bool = True,
        require_target_version: bool = True,
    ) -> RolloutResult:
        """Run the full canary -> fleet rollout. Halts (and optionally rolls back the
        canaries) on drift; promotes the fleet only after a clean canary watch."""
        plan = self.plan(
            target_version,
            canary_count=canary_count,
            canary_fraction=canary_fraction,
            region=region,
        )
        result = RolloutResult(target_version=target_version, plan=plan)

        def event(msg: str) -> None:
            result.events.append(msg)
            self._log(msg)

        if not plan.canary_ids and not plan.fleet_ids:
            result.reason = f"no eligible deployments to move to {target_version} (all current?)"
            event(result.reason)
            # Nothing to do is a clean no-op, not a halt.
            result.fleet_promoted = True
            return result

        if not plan.canary_ids:
            # Only happens if everything was already eligible-but-zero; defensive.
            result.reason = "no canary could be selected"
            result.halted = True
            event(result.reason)
            return result

        event(
            f"plan: target={target_version} canary={plan.canary_ids} "
            f"fleet={plan.fleet_ids}"
        )

        # 1. Promote the canary subset.
        try:
            for cid in plan.canary_ids:
                self.promoter(cid, target_version)
            result.canary_promoted = True
            event(f"promoted canary -> {target_version}: {plan.canary_ids}")
        except Exception as exc:
            result.halted = True
            result.reason = f"canary promotion failed: {exc}"
            event(result.reason)
            return result

        # 2. Watch the canary health rollup.
        healthy, last_health, reason = self.watch_canary(
            plan.canary_ids,
            target_version,
            watch_seconds=watch_seconds,
            poll_seconds=poll_seconds,
            require_target_version=require_target_version,
        )
        result.canary_health = last_health
        result.canary_healthy = healthy

        if not healthy:
            # HALT — do NOT promote the fleet.
            result.halted = True
            result.reason = f"HALT (halt-on-drift): {reason}"
            event(result.reason)
            event(f"fleet NOT promoted; {len(plan.fleet_ids)} deployment(s) left on prior version")
            if rollback_on_halt:
                self._rollback_canary(plan, result, event)
            return result

        event(f"canary healthy on {target_version}: {last_health}")

        # 3. Promote the fleet remainder.
        try:
            for fid in plan.fleet_ids:
                self.promoter(fid, target_version)
            result.fleet_promoted = True
            result.reason = f"fleet promoted to {target_version}"
            event(result.reason)
        except Exception as exc:
            # A fleet-promotion failure mid-flight is a halt (canary stays on target).
            result.halted = True
            result.reason = f"fleet promotion failed after healthy canary: {exc}"
            event(result.reason)
        return result

    def _rollback_canary(self, plan: RolloutPlan, result: RolloutResult, event) -> None:
        """Roll the canary set back to each deployment's prior version."""
        try:
            for cid in plan.canary_ids:
                prior = plan.prior_versions.get(cid)
                if prior and prior != plan.target_version:
                    self.promoter(cid, prior)
                    event(f"rolled back canary {cid} -> {prior}")
            result.rolled_back = True
        except Exception as exc:
            event(f"rollback encountered an error: {exc}")

    # -- standalone rollback ------------------------------------------------ #

    def rollback_all(self, to_version: str, *, region: str | None = None) -> RolloutResult:
        """Roll the whole (optionally region-scoped) fleet back to ``to_version``.

        Used by the operator's ``rollback`` command. Returns a result whose
        ``fleet_promoted`` reflects that every targeted deployment was re-pointed.
        """
        deployments = self.console.list_deployments()
        targets = [
            d
            for d in deployments
            if d.get("version") != to_version
            and (region is None or d.get("region") == region)
        ]
        plan = RolloutPlan(
            target_version=to_version,
            canary_ids=[],
            fleet_ids=[d["deployment_id"] for d in targets],
            prior_versions={d["deployment_id"]: d.get("version", "") for d in deployments},
        )
        result = RolloutResult(target_version=to_version, plan=plan)

        def event(msg: str) -> None:
            result.events.append(msg)
            self._log(msg)

        if not targets:
            result.reason = f"fleet already on {to_version}; nothing to roll back"
            result.fleet_promoted = True
            event(result.reason)
            return result
        try:
            for d in targets:
                self.promoter(d["deployment_id"], to_version)
            result.fleet_promoted = True
            result.rolled_back = True
            result.reason = f"rolled back {len(targets)} deployment(s) -> {to_version}"
            event(result.reason)
        except Exception as exc:
            result.halted = True
            result.reason = f"rollback failed: {exc}"
            event(result.reason)
        return result


# --------------------------------------------------------------------------- #
# Default promoter (operates the release registry's latest pointer)            #
# --------------------------------------------------------------------------- #


def registry_latest_promoter(registry_root: str):
    """A :data:`Promoter` that records the intended version by moving the registry's
    ``latest`` pointer the first time a version is seen.

    In the real CP, the per-deployment *config* a deployment pulls carries the
    version it should run; an agent then ``config_pull``s + verifies + applies it.
    Wiring that per-deployment config is ``config-dist``'s job (a sibling). At the
    rollout layer the actionable, side-effecting thing the controller owns is "which
    release is current" — so the default promoter ensures the target version is the
    published ``latest`` (and logs the per-deployment intent). The console then
    observes each agent adopt + heartbeat the new version.
    """
    from publish import ReleaseRegistry  # local import; avoid hard dep if unused

    reg = ReleaseRegistry(registry_root)
    _moved: set[str] = set()

    def _promote(deployment_id: str, version: str) -> None:
        if version not in _moved:
            try:
                reg.set_latest(version)
            except KeyError:
                # Version not in this registry — the controller still records intent;
                # config-dist may publish per-deployment config elsewhere.
                pass
            _moved.add(version)
        # Per-deployment intent is logged by the controller's event stream.

    return _promote


def noop_promoter(deployment_id: str, version: str) -> None:
    """A Promoter that does nothing (dry-run planning / health-only watches)."""
    return None


def _monotonic() -> float:
    return time.monotonic()


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def _print_json(obj) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Canary -> fleet rollout controller (halt-on-drift, rollback)."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    common_console = dict(default="http://localhost:8080", help="console base URL")

    pr = sub.add_parser("promote", help="canary -> fleet rollout of a target version")
    pr.add_argument("--console", **common_console)
    pr.add_argument("--version", required=True, help="target release version to roll out")
    pr.add_argument("--canary-count", type=int, default=None, help="number of canary deployments")
    pr.add_argument("--canary-fraction", type=float, default=None, help="fraction of fleet as canary")
    pr.add_argument("--region", default=None, help="restrict rollout to a region")
    pr.add_argument("--watch-seconds", type=float, default=30.0)
    pr.add_argument("--poll-seconds", type=float, default=3.0)
    pr.add_argument("--registry", default=None, help="release registry root (drives latest pointer)")
    pr.add_argument(
        "--tolerate-yellow",
        action="store_true",
        help="treat yellow canaries as acceptable (default: only green is healthy)",
    )
    pr.add_argument("--no-rollback", action="store_true", help="do not roll canaries back on halt")

    st = sub.add_parser("status", help="show the fleet's versions + health from the console")
    st.add_argument("--console", **common_console)

    rb = sub.add_parser("rollback", help="roll the fleet back to a prior version")
    rb.add_argument("--console", **common_console)
    rb.add_argument("--to", required=True, help="version to roll back to")
    rb.add_argument("--region", default=None)
    rb.add_argument("--registry", default=None)

    args = ap.parse_args(argv)

    console = HttpConsoleClient(args.console)

    if args.cmd == "status":
        try:
            deps = console.list_deployments()
        except Exception as exc:
            print(f"status failed: {exc}", file=sys.stderr)
            return 1
        rows = [
            {
                "deployment_id": d.get("deployment_id"),
                "version": d.get("version"),
                "health": d.get("health"),
                "region": d.get("region"),
            }
            for d in deps
        ]
        _print_json({"fleet_size": len(rows), "deployments": rows})
        return 0

    promoter: Promoter
    if getattr(args, "registry", None):
        promoter = registry_latest_promoter(args.registry)
    else:
        promoter = noop_promoter

    if args.cmd == "promote":
        healthy_states = ("green", "yellow") if args.tolerate_yellow else ("green",)
        ctrl = RolloutController(console, promoter, healthy_states=healthy_states)
        try:
            res = ctrl.rollout(
                args.version,
                canary_count=args.canary_count,
                canary_fraction=args.canary_fraction,
                region=args.region,
                watch_seconds=args.watch_seconds,
                poll_seconds=args.poll_seconds,
                rollback_on_halt=not args.no_rollback,
            )
        except Exception as exc:
            print(f"rollout failed: {exc}", file=sys.stderr)
            return 1
        _print_json(res.as_dict())
        return 0 if res.ok else 1

    if args.cmd == "rollback":
        ctrl = RolloutController(console, promoter)
        res = ctrl.rollback_all(args.to, region=args.region)
        _print_json(res.as_dict())
        return 0 if res.fleet_promoted and not res.halted else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
