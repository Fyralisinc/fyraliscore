"""lib/shared/env.py — canonical environment / production detection.

Historically the codebase had two competing "is this production?"
signals:

  * `FYRALIS_ENV`     — read by the secret store (MASTER_KEK fail-fast),
                        the webhook env-fallback guard, and the OAuth
                        state-HMAC guard.
  * `COMPANY_OS_ENV`  — legacy service/runtime label used by worker scripts
                        and the synthetic package.
  * `APP_ENV` / `ENVIRONMENT` — common platform/deploy labels accepted by the
                        gateway settings.

A deployment that set only one of them got an inconsistent posture: the
shipped `docker-compose.yml` set `COMPANY_OS_ENV=prod` but left
`FYRALIS_ENV` unset, so the MASTER_KEK fail-fast and webhook guards
never fired — meaning a missing KEK silently generated an ephemeral
in-memory key (encrypted secrets unrecoverable after restart) instead
of refusing to boot.

`is_prod()` collapses them into one fail-safe rule: the environment is
production if *any* recognized variable says so. Guards key on this so that
setting any one deployment label is sufficient to get the hardened posture.
"""
from __future__ import annotations

import os

# Environment labels accepted across gateway, workers, and deploy platforms.
# Order is used only by env_name() when none of them marks production.
_ENV_VARS = ("FYRALIS_ENV", "COMPANY_OS_ENV", "APP_ENV", "ENVIRONMENT")

_PROD_VALUES = frozenset({"prod", "production"})


def is_prod() -> bool:
    """True if the deployment should use the hardened production posture.

    Production iff *any* recognized environment variable is set to
    ``prod``/``production`` (case-insensitive). Defense-in-depth: setting any
    one deployment label is enough.
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
