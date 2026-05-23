"""lib/shared/env.py — canonical environment / production detection.

Historically the codebase had two competing "is this production?"
signals:

  * `FYRALIS_ENV`     — read by the secret store (MASTER_KEK fail-fast),
                        the webhook env-fallback guard, and the OAuth
                        state-HMAC guard.
  * `COMPANY_OS_ENV`  — read by the gateway (sim/debug router mounting)
                        and the synthetic package.

A deployment that set only one of them got an inconsistent posture: the
shipped `docker-compose.yml` set `COMPANY_OS_ENV=prod` but left
`FYRALIS_ENV` unset, so the MASTER_KEK fail-fast and webhook guards
never fired — meaning a missing KEK silently generated an ephemeral
in-memory key (encrypted secrets unrecoverable after restart) instead
of refusing to boot.

`is_prod()` collapses the two into one fail-safe rule: the environment
is production if *either* variable says so. Guards key on this so that
setting either variable is sufficient to get the hardened posture.
"""
from __future__ import annotations

import os

# The two historical env vars, checked together. Order is irrelevant —
# prod wins if any of them is "prod".
_ENV_VARS = ("FYRALIS_ENV", "COMPANY_OS_ENV")

_PROD_VALUES = frozenset({"prod", "production"})


def is_prod() -> bool:
    """True if the deployment should use the hardened production posture.

    Production iff *any* of the recognized environment variables
    (`FYRALIS_ENV`, `COMPANY_OS_ENV`) is set to ``prod``/``production``
    (case-insensitive). Defense-in-depth: setting either one is enough.
    """
    for var in _ENV_VARS:
        if os.environ.get(var, "").strip().lower() in _PROD_VALUES:
            return True
    return False


def env_name(default: str = "dev") -> str:
    """The effective environment label.

    Returns ``"prod"`` if :func:`is_prod`; otherwise the first non-empty
    recognized variable (lowercased), or ``default`` if none are set.
    """
    if is_prod():
        return "prod"
    for var in _ENV_VARS:
        val = os.environ.get(var, "").strip().lower()
        if val:
            return val
    return default


__all__ = ["is_prod", "env_name"]
