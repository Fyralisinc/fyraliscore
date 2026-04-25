"""services/synthetic/__init__.py — production guard.

Per SYNTHETIC-BYPASS-PLAN §1.3, this module cannot load in a
production environment. The guard raises at import time so every
entry point (direct imports, CLI, HTTP) fails fast before any
synthetic injection path can run.

Set COMPANY_OS_ENV=dev|staging|test to enable.
"""
from __future__ import annotations

import os


_ALLOWED_ENVS = {"dev", "staging", "test"}


def _check_env_guard() -> None:
    env = os.environ.get("COMPANY_OS_ENV", "production")
    if env not in _ALLOWED_ENVS:
        raise RuntimeError(
            "Synthetic signal service cannot run in production environment. "
            "Set COMPANY_OS_ENV=dev|staging|test to enable."
        )


_check_env_guard()


__all__ = ["_check_env_guard"]
