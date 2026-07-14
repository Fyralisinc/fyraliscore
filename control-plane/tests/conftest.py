"""conftest.py — pytest config for the control-plane end-to-end suite.

Registers the ``--live-docker`` opt-in flag and the ``live_docker`` marker used
by ``test_e2e.py`` to gate the steps that need a real Docker stack (a running
Mimir container behind the deployed auth-proxy). Without the flag those steps are
reported as explicit SKIPs, never silent passes.
"""

from __future__ import annotations


def pytest_addoption(parser):
    parser.addoption(
        "--live-docker",
        action="store_true",
        default=False,
        help="run the live-docker-only steps against a running compose stack",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "live_docker: step that needs a running Docker stack (real Mimir container)",
    )
