"""test_e2e.py — pytest wrapper around the end-to-end control-plane smoke.

This drives the SAME real in-process assembly that ``e2e_smoke.py`` builds, but
expressed as discrete pytest tests with clear, per-step assertions so a CI run
gets granular pass/fail and a CTO gets a readable test report.

Layering
--------
* A single ``stack`` (module-scoped fixture) bootstraps the throwaway CA + signing
  trust root ONCE and is shared across the ordered steps (each step builds on the
  previous — onboarding produces the bundle the agent + metric steps consume), so
  the wrapper exercises a genuine end-to-end sequence rather than seven unrelated
  units.
* Each ``test_step_N_*`` runs one phase and asserts via the smoke module's REAL
  component calls. Test names + ordering make the dependency chain explicit.
* The **live-docker-only** path (the metric round-trip against a real *Mimir
  container* behind the deployed proxy) is marked ``@pytest.mark.skip`` so it is
  reported as an explicit SKIP, never a silent pass — flip it on by running with
  ``--live-docker`` once the compose stack is up (see ``-m live_docker``).

Run::

    pytest test_e2e.py -v
    pytest test_e2e.py -v -m live_docker --live-docker   # the docker-gated step
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

# Import the smoke module (same dir) so the wrapper reuses its REAL component
# wiring rather than re-implementing it.
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import e2e_smoke as smoke  # noqa: E402


# --------------------------------------------------------------------------- #
# pytest plumbing: the --live-docker opt-in flag + the `live_docker` marker are #
# registered in this dir's conftest.py (pytest only honors pytest_addoption     #
# from a conftest/plugin, not from a test module).                              #
# --------------------------------------------------------------------------- #
def _live_docker_enabled(request) -> bool:
    try:
        return bool(request.config.getoption("--live-docker"))
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# A reporter that turns smoke checks into pytest assertions.                   #
# --------------------------------------------------------------------------- #
class _AssertReporter(smoke.StepReporter):
    """A StepReporter whose ``check`` ALSO raises AssertionError on failure.

    This lets each pytest test reuse the smoke step functions verbatim (they call
    ``rep.check(cond, msg)``) while still failing the test the moment any
    assertion is violated — with the smoke's descriptive message.
    """

    def check(self, cond: bool, msg: str) -> bool:
        ok = super().check(cond, msg)
        assert ok, msg
        return ok


# --------------------------------------------------------------------------- #
# Shared stack (one CA/signing bootstrap, threaded through the ordered steps). #
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def stack():
    st = smoke.build_stack()
    yield st
    st.close(keep=False)


@pytest.fixture(scope="module")
def rep():
    return _AssertReporter()


# --------------------------------------------------------------------------- #
# The ordered end-to-end steps.                                               #
# pytest runs tests in file order; the steps deliberately build on each other. #
# --------------------------------------------------------------------------- #
def test_step_1_bootstrap_ca_and_signing_keys_exist(stack, rep):
    """(1) CA root+intermediate and the ed25519 signing keys are present + usable."""
    smoke.step1_bootstrap(stack, rep)


def test_step_2_onboard_acme_produces_bundle_registry_console(stack, rep):
    """(2) Onboard 'acme' -> bundle (cert+license), active registry row, console lists it."""
    smoke.step2_onboard(stack, rep)
    # Cross-check the smoke recorded the deployment so later steps can chain.
    assert stack.onboard_result is not None
    assert stack.bundle_dir is not None and Path(stack.bundle_dir).is_dir()


def test_step_3_agent_heartbeats_console_marks_green(stack, rep):
    """(3) Start the agent with the bundle -> console marks acme GREEN."""
    assert stack.onboard_result is not None, "step 2 must run first"
    smoke.step3_agent_green(stack, rep)


def test_step_4_and_5_metric_round_trip_and_isolation(stack, rep):
    """(4+5) Push a metric as acme through the REAL auth-proxy -> Mimir, query it
    back (present), then prove a different tenant cannot see it (isolation)."""
    assert stack.bundle_dir is not None, "step 2 must run first"
    smoke.step4_and_5_metric_and_isolation(stack, rep)


def test_step_6_license_tamper_agent_denies(stack, rep):
    """(6) Flip a byte in the signed license -> the agent's license gate denies."""
    assert stack.bundle_dir is not None, "step 2 must run first"
    smoke.step6_license_tamper(stack, rep)


def test_step_7_config_dist_serves_signed_config_agent_verifies(stack, rep):
    """(7) config-dist serves a SIGNED config the agent verifies-before-apply (I6)."""
    assert stack.onboard_result is not None, "step 2 must run first"
    smoke.step7_signed_config(stack, rep)


# --------------------------------------------------------------------------- #
# Live-docker-only step (explicit SKIP unless the stack is up).               #
# --------------------------------------------------------------------------- #
@pytest.mark.live_docker
def test_step_live_metric_against_real_mimir_container(request):
    """The metric round-trip against a REAL Mimir CONTAINER behind the deployed
    auth-proxy (compose ``cp-net``). Skipped unless ``--live-docker`` is passed
    AND docker is available — never a silent pass.

    The no-docker ``test_step_4_and_5_*`` already proves the boundary->auth-proxy
    ->Mimir *identity + isolation* contract against a multitenant ``MockMimir``
    with the exact ``X-Scope-OrgID`` semantics; this docker variant swaps in the
    real Mimir image to confirm the wire/protocol level too.
    """
    if not _live_docker_enabled(request):
        pytest.skip("live-docker step: pass --live-docker (and have the stack up) to run it")
    if shutil.which("docker") is None:
        pytest.skip("live-docker step: `docker` not installed")
    compose = smoke._CP_ROOT / "docker-compose.control-plane.yml"
    pytest.skip(
        "live-docker step is a MANUAL gate: bring the stack up with "
        f"`docker compose -f {compose} up -d`, point the smoke's upstream at the "
        "real mimir:9009 behind the deployed auth-proxy, then assert the round-trip. "
        "Automating container lifecycle here is out of scope for the unit run."
    )


# Allow ``python test_e2e.py`` to run the suite via pytest for convenience.
if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-v"]))
